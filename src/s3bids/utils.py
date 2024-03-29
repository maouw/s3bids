import contextlib
import gzip
import json
import logging
import os
import os.path as op
from random import shuffle
import tempfile
from io import BytesIO, StringIO
from pathlib import Path
from webbrowser import get
import boto3
import nibabel as nib
import pandas as pd
import s3fs
import random

from bids import BIDSLayout
from botocore import UNSIGNED
from botocore.client import Config

#from dask.base import compute
#from dask.delayed import delayed
#from dask.diagnostics.progress import ProgressBar
import concurrent.futures
from tqdm.auto import tqdm

from collections.abc import Iterator, Sequence, Collection
from dataclasses import dataclass, field, InitVar
import typing
from typing import Any, Literal
import csv

if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


def to_bids_description(
    path: str,
    fname: str = "dataset_description.json",
    bids_version: str = "1.9.0",
    **kwargs,
):
    """Dumps a dict into a bids description at the given location"""
    kwargs.update({"BIDSVersion": bids_version})
    desc_file = os.path.join(path, fname)
    with open(desc_file, "w") as outfile:
        json.dump(kwargs, outfile)


# +----------------------------------------------------+
# | Begin S3BIDSStudy classes and supporting functions |
# +----------------------------------------------------+
def get_s3_client(*, anon=True)-> S3Client:
    """Return a boto3 s3 client

    Global boto clients are not thread safe so we use this function
    to return independent session clients for different threads.

    Parameters
    ----------
    anon : bool
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order). Default: True

    Returns
    -------
    s3_client : boto3.client('s3')
    """
    session = boto3.session.Session()
    return session.client("s3", config=Config(signature_version=UNSIGNED)) if anon else session.client("s3")

def _ls_s3fs(s3_prefix: str, *, anon=True):
    """Returns a dict of list of files using s3fs

    The files are divided between subject directories/files and
    non-subject directories/files.

    Parameters
    ----------
    s3_prefix : str
        AWS S3 key for the study or site "directory" that contains all
        of the subjects

    anon : bool
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order). Default: True

    Returns
    -------
    subjects : dict
    """
    fs = s3fs.S3FileSystem(anon=anon)
    site_files: list[str] = fs.ls(s3_prefix, detail=False)

    # Just need BIDSLayout for the `parse_file_entities` method
    dd = tempfile.TemporaryDirectory()
    layout = BIDSLayout(dd.name, validate=False)

    entities: list[dict[str,Any]] = [layout.parse_file_entities(f) for f in site_files]

    return {
        "subjects": [f for f, e in zip(site_files, entities, strict=False) if e.get("subject") is not None],
        "other": [f for f, e in zip(site_files, entities, strict=False) if e.get("subject") is None],
    }


def _get_matching_s3_keys(bucket: str, prefix: str = "", suffix: str ="", *, anon=True) -> Iterator[str]:
    """Generate all the matching keys in an S3 bucket.

    Parameters
    ----------
    bucket : str
        Name of the S3 bucket

    prefix : str, optional
        Only fetch keys that start with this prefix

    suffix : str, optional
        Only fetch keys that end with this suffix

    anon : bool
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order). Default: True

    Yields
    ------
    key : list
        S3 keys that match the prefix and suffix
    """
    s3 = get_s3_client(anon=anon)
    kwargs = {"Bucket": bucket, "MaxKeys": 1000}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str) and prefix:
        kwargs["Prefix"] = prefix

    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp["Contents"]
        except KeyError:
            return

        for obj in contents:
            if (key := obj.get("Key", "")) and key.startswith(prefix) and key.endswith(suffix):
                yield key
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break


def _download_from_s3(fname: str, bucket: str, key: str, *, overwrite: bool = False, anon bool =True):
    """Download object from S3 to local file

    Parameters
    ----------
    fname : str
        File path to which to download the object

    bucket : str
        S3 bucket name

    key : str
        S3 key for the object to download

    overwrite : bool
        If True, overwrite file if it already exists.
        If False, skip download and return. Default: False

    anon : bool
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order). Default: True
    """
    # Create the directory and file if necessary
    fs = s3fs.S3FileSystem(anon=anon)
    if overwrite or not os.path.exists(fname):
        os.makedirs(os.path.dirname(fname), exist_ok=True)
        fs.get("/".join([bucket, key]), fname)


from typing import TypedDict



@dataclass
class S3BIDSSubject:
    """A single study subject hosted on AWS S3."""
    subject_id: str
    """Subject-ID for this subject."""
    study: "S3BIDSStudy"
    """The S3BIDSStudy for which this subject was a participant."""
    s3_keys: dict[str, list[str]] = field(default_factory=dict, init=False, repr=False)
    files: dict = field(default_factory=dict, init=False, repr=False)
    """Local files for this subject's dMRI data

        Before the call to subject.download(), this is None.
        Afterward, the files are stored in a dict with keys
        for each Amazon S3 key and values corresponding to
        the local file.
        """
    # dict[str, dict[Any, Any]]
    """Dict of S3 keys for this subject's data."""
    def __post_init__(self):
        logging.getLogger("botocore").setLevel(logging.WARNING) # TODO: Setby environment variable?
        self.files = {"raw": {}, "derivatives": {}}
        self._init_s3_keys()

    def __repr__(self):
        return f"{type(self).__name__}(subject_id={self.subject_id}, " f"study_id={self.study.study_id}"

    def _init_s3_keys(self):
        """Initialize all required S3 keys for this subject."""
        prefixes = {
            "raw": f"{self.study.s3_prefix}/{self.subject_id}".lstrip("/"),
            "derivatives": {
                dt: "/".join(
                    [
                        *dt.split("/")[1:],  # removes bucket name
                        self.subject_id,
                    ],
                ).lstrip("/")
                for dt in self.study.derivative_types
            },
        }

        s3_keys = {
            "raw": list(
                dict.fromkeys(
                    _get_matching_s3_keys(
                        bucket=self.study.bucket,
                        prefix=prefixes["raw"],
                        anon=self.study.anon,
                    ),
                ),
            ),
            "derivatives": {
                dt: list(
                    dict.fromkeys(
                        _get_matching_s3_keys(
                            bucket=self.study.bucket,
                            prefix=prefixes["derivatives"][dt],
                            anon=self.study.anon,
                        ),
                    ),
                )
                for dt in self.study.derivative_types
            },
        }

        self.s3_keys = s3_keys

    def download(
        self,
        directory: str,
        *,
        include_derivs: bool =False,
        overwrite: bool =False,
        suffix: str | None =None,
        pbar: bool = True,
        pbar_idx: int =0,
    ):
        """Download files from S3

        Parameters
        ----------
        directory : str
            Directory to which to download subject files

        include_derivs : bool or str
            If True, download all derivatives files. If False, do not.
            If a string or sequence of strings is passed, this will
            only download derivatives that match the string(s) (e.g.
            ['dmriprep', 'afq']). Default: False

        overwrite : bool
            If True, overwrite files for each subject. Default: False

        suffix : str
            Suffix, including extension, of file(s) to download.
            Default: None

        pbar : bool
            If True, include download progress bar. Default: True

        pbar_idx : int
            Progress bar index for multithreaded progress bars. Default: 0
        """
        if not isinstance(directory, str): # TOOD: Better to ask forgiveness than permission
            raise TypeError("directory must be a string.")

        if not (isinstance(include_derivs, bool | str)):  # TOOD: Better to ask forgiveness than permission
            raise TypeError(
                "include_derivs must be a boolean, a " "string, or a sequence of strings.",
            )

        if not isinstance(overwrite, bool): # TOOD: Better to ask forgiveness than permission
            raise TypeError("overwrite must be a boolean.")

        if (suffix is not None) and not (isinstance(suffix, str)): # TOOD: Better to ask forgiveness than permission
            raise TypeError("suffix must be a string.")

        if not isinstance(pbar, bool): # TOOD: Better to ask forgiveness than permission
            raise TypeError("pbar must be a boolean.")

        if not isinstance(pbar_idx, int): # TOOD: Better to ask forgiveness than permission
            raise TypeError("pbar_idx must be an integer.")

        def split_key(key):
            if self.study.s3_prefix:
                return key.split(self.study.s3_prefix)[-1]
            else:
                return key

        # Filter out keys that do not end with suffix
        if suffix is not None:
            s3_keys_raw = [s3key for s3key in self.s3_keys["raw"] if s3key.endswith(suffix)]
            s3_keys_deriv = {
                dt: [s3key for s3key in s3keys if s3key.endswith(suffix)]
                for dt, s3keys in self.s3_keys["derivatives"].items()
            }
        else:
            s3_keys_raw = self.s3_keys["raw"]
            s3_keys_deriv = self.s3_keys["derivatives"]

        files = {
            "raw": [
                os.path.abspath(
                    os.path.join(
                        directory,
                        split_key(key).lstrip("/"),
                    ),
                )
                for key in s3_keys_raw
            ],
            "derivatives": {
                dt: [
                    os.path.abspath(
                        os.path.join(
                            directory,
                            split_key(s3key).lstrip("/"),
                        ),
                    )
                    for s3key in s3keys
                ]
                for dt, s3keys in s3_keys_deriv.items()
            },
        }

        raw_zip = list(zip(s3_keys_raw, files["raw"], strict=False))

        # Populate files parameter
        self.files["raw"].update({k: f for k, f in raw_zip})

        # Generate list of (key, file) tuples
        download_pairs = [(k, f) for k, f in raw_zip]

        deriv_zips = {
            dt: list(
                zip(
                    s3keys,
                    files["derivatives"][dt],
                    strict=False,
                ),
            )
            for dt, s3keys in s3_keys_deriv.items()
        }

        deriv_pairs = []
        for dt in files["derivatives"].keys():
            if include_derivs is True:
                # In this case, include all derivatives files
                deriv_pairs += [(k, f) for k, f in deriv_zips[dt]]
                self.files["derivatives"][dt] = {k: f for k, f in deriv_zips[dt]}
            elif include_derivs is False:
                pass
            elif (
                isinstance(include_derivs, str)
                # In this case, filter only derivatives S3 keys that
                # include the `include_derivs` string as a substring
                and include_derivs in dt
            ):
                deriv_pairs += [(k, f) for k, f in deriv_zips[dt]]
                self.files["derivatives"][dt] = {k: f for k, f in deriv_zips[dt]}
            elif all(isinstance(s, str) for s in include_derivs) and any(
                [deriv in dt for deriv in include_derivs],
            ):
                # In this case, filter only derivatives S3 keys that
                # include any of the `include_derivs` strings as a
                # substring
                deriv_pairs += [(k, f) for k, f in deriv_zips[dt]]
                self.files["derivatives"][dt] = {k: f for k, f in deriv_zips[dt]}

        if include_derivs is not False:
            download_pairs += deriv_pairs

        # Now iterate through the list and download each item
        if pbar:
            progress = tqdm(
            desc=f"Download {self.subject_id}",
            position=pbar_idx,
            total=len(download_pairs) + 1,
        )

        for key, fname in download_pairs:
            _download_from_s3(
                fname=fname,
                bucket=self.study.bucket,
                key=key,
                overwrite=overwrite,
                anon=self.study.anon,
            )
            with contextlib.suppress(NameError, AttributeError):
                progress.update() # type: ignore[attr-defined,name-defined]
        with contextlib.suppress(NameError, AttributeError):
            progress.update() # type: ignore[attr-defined,name-defined]
            progress.close() # type: ignore[attr-defined,name-defined]



class HBNSubject(S3BIDSSubject):
    """A subject in the HBN study

    See Also
    --------
    AFQ.data.S3BIDSSubject
    """
    site: str | None = field(default=None, init=False, repr=True)
    """Site-ID for the site from which this subject's data was collected"""

    def _init_s3_keys(self):
        """Initialize all required S3 keys for this subject."""
        prefixes = {
            "raw": f"{self.study.s3_prefix}/{self.subject_id}".lstrip("/"),
            "derivatives": f"{self.study.s3_prefix}/derivatives/{self.subject_id}".lstrip("/"),
        }

        s3_keys = {
            datatype: list(
                dict.fromkeys(
                    _get_matching_s3_keys(
                        bucket=self.study.bucket,
                        prefix=prefix,
                        anon=self.study.anon,
                    ),
                ),
            )
            for datatype, prefix in prefixes.items()
        }

        def get_deriv_type(s3_key: str) -> str:
            after_sub = s3_key.split("/" + self.subject_id + "/")[-1]
            return after_sub.split("/")[0]

        deriv_keys = {
            dt: [s3key for s3key in s3_keys["derivatives"] if dt == get_deriv_type(s3key)]
            for dt in self.study.derivative_types
        }

        s3_keys["derivatives"] = deriv_keys
        self.s3_keys = s3_keys


        # self._study_id = study_id
        # self._bucket = bucket
        # self._s3_prefix = s3_prefix
        # self._use_participants_tsv = use_participants_tsv
        # self._random_seed = random_seed
        # self._anon = anon
        # self._subject_class = _subject_class
        # self._local_directories = []

        # # Get a list of all subjects in the study
        # self._all_subjects = self._list_all_subjects()
        # self._derivative_types = self._get_derivative_types()
        # self._non_subject_s3_keys = self._get_non_subject_s3_keys()

class S3BIDSStudy:
    """A BIDS-compliant study hosted on AWS S3"""
        # def __init__(self, study_id, bucket, s3_prefix='', subjects=None,
        #          anon=True, use_participants_tsv=False, random_seed=None,
        #          _subject_class=S3BIDSSubject):
    def __init__(self,
        study_id: str,
        bucket: str,
        s3_prefix: str = "",
        subjects: int | str | Collection[str] | None = None,
        anon: bool = True,    
        use_participants_tsv: bool = False,
        random_seed: int | None = None,
        _subject_class: type[S3BIDSSubject] = S3BIDSSubject
    ):
        """Initialize an S3BIDSStudy instance

        Parameters
        ----------
        study_id : str
            An identifier string for the study

        bucket : str
            The S3 bucket that contains the study data

        s3_prefix : str, optional
            The S3 prefix common to all of the study objects on S3.
            Default: the empty string, which indicates that the study
            is at the top level of the bucket.

        subjects : str, sequence(str), int, or None, optional
            If int, retrieve S3 keys for the first `subjects` subjects.
            If "all", retrieve all subjects. If str or sequence of
            strings, retrieve S3 keys for the specified subjects. If sequence
            of ints, then for each int n retrieve S3 keys for the nth subject.
            If None, retrieve S3 keys for the first subject. Default: None

        anon : bool, optional
            Whether to use anonymous connection (public buckets only).
            If False, uses the key/secret given, or boto’s credential
            resolver (client_kwargs, environment, variables, config
            files, EC2 IAM server, in that order). Default: True

        use_participants_tsv : bool, optional
            If True, use the particpants tsv files to retrieve subject
            identifiers. This is faster but may not catch all subjects.
            Sometimes the tsv files are outdated. Default: False

        random_seed : int or None, optional
            Random seed for selection of subjects if `subjects` is an
            integer. Use the same random seed for reproducibility.
            Default: None

        _subject_class : object, optional
            The subject class to be used for this study. This parameter
            has a leading underscore because you probably don't want
            to change it. If you do change it, you must provide a
            class that quacks like AFQ.data.S3BIDSSubject. Default:
            S3BIDSSubject

        Examples
        --------
        Access data stored in a bucket using credentials:
        >>> study = S3BIDSStudy('studyname',
        ...                     'bucketname',
        ...                     '/path/to/dataset/',
        ...                     anon=False)

        Access data stored in a publicly accessible bucket:
        >>> study = S3BIDSStudy('hbn',
        ...    'fcp-indi',
        ...    'data/Projects/HBN/BIDS_curated/derivatives/qsiprep/')

        """
        logging.getLogger("botocore").setLevel(logging.WARNING)
        

"

    def __init__(
        self,
        subjects: int | str | Collection[str] | None = None,
    ):
        """Initialize an S3BIDSStudy instance

        Parameters
        ----------
        subjects : str, sequence(str), int, or None, optional
            If int, retrieve S3 keys for the first `subjects` subjects.
            If "all", retrieve all subjects. If str or sequence of
            strings, retrieve S3 keys for the specified subjects. If sequence
            of ints, then for each int n retrieve S3 keys for the nth subject.
            If None, retrieve S3 keys for the first subject. Default: None

        anon : bool, optional
            Whether to use anonymous connection (public buckets only).
            If False, uses the key/secret given, or boto’s credential
            resolver (client_kwargs, environment, variables, config
            files, EC2 IAM server, in that order). Default: True

        use_participants_tsv : bool, optional
            If True, use the particpants tsv files to retrieve subject
            identifiers. This is faster but may not catch all subjects.
            Sometimes the tsv files are outdated. Default: False

        random_seed : int or None, optional
            Random seed for selection of subjects if `subjects` is an
            integer. Use the same random seed for reproducibility.
            Default: None

        _subject_class : object, optional
            The subject class to be used for this study. This parameter
            has a leading underscore because you probably don't want
            to change it. If you do change it, you must provide a
            class that quacks like AFQ.data.S3BIDSSubject. Default:
            S3BIDSSubject

        Examples
        --------
        Access data stored in a bucket using credentials:
        >>> study = S3BIDSStudy("studyname", "bucketname", "/path/to/dataset/", anon=False)

        Access data stored in a publicly accessible bucket:
        >>> study = S3BIDSStudy(
        ...     "hbn", "fcp-indi", "data/Projects/HBN/BIDS_curated/derivatives/qsiprep/"
        ... )

        """
        logging.getLogger("botocore").setLevel(logging.WARNING) # TODO: Setby environment variable?
        if self.s3_prefix == "/":
            raise ValueError(
                "If the study is at the top level "
                "of the s3 bucket, please pass the "
                "empty string as the s3 prefix"
                "(the default value)",
            )
        if not (isinstance(subjects, int | str)):
            raise TypeError(
                "`subjects` must be an int, string, " "sequence of strings, or a sequence of ints.",
            )

        if isinstance(subjects, int) and subjects < 1:
            raise ValueError("If `subjects` is an int, it must be greater than 0.")

        if not isinstance(use_participants_tsv, bool):
            raise TypeError("`use_participants_tsv` must be boolean.")

        if not (random_seed is None or isinstance(random_seed, int)):
            raise TypeError("`random_seed` must be an integer.")


        self._study_id = study_id
        self._bucket = bucket
        self._s3_prefix = s3_prefix
        self._use_participants_tsv = use_participants_tsv
        self._random_seed = random_seed
        self._anon = anon
        self._subject_class = _subject_class
        self._local_directories = []

        # Get a list of all subjects in the study
        self._all_subjects = self._list_all_subjects() or []
        self._derivative_types = self._get_derivative_types() or []
        self._non_subject_s3_keys = self._get_non_subject_s3_keys() or []

        # subjects : str, sequence(str), int, or None, optional
        #     If int, retrieve S3 keys for the first `subjects` subjects.
        #     If "all", retrieve all subjects. If str or sequence of
        #     strings, retrieve S3 keys for the specified subjects. If sequence
        #     of ints, then for each int n retrieve S3 keys for the nth subject.
        #     If None, retrieve S3 keys for the first subject. Default: None
        
        
        if subjects == "all":
            # if "all," retrieve all subjects
            self._subjects = sorted(self.all_subjects)
        elif isinstance(subjects, str):
            # if a string, just get that one subject
            self._subjects = [subjects]
        else:
            random.seed(self.random_seed)
            randomized_subjects = self.all_subjects.copy()
            random.shuffle(randomized_subjects)
            match subjects:
                case None:
                    subjects = [random.choice(self.all_subjects)]
                case int():
                    subjects = randomized_subjects[:subjects]
                    subjects = random.sample(self.all_subjects, k=subjects)
                case Collection():
                    subjects = [randomized_subjects[i] for i in subjects]
                    
            else:
                subjects = [randomized_subjects[i] for i in subjects]

            if isinstance(subjects, str):
                subjects = [subjects]
        
        match subjects:
            case str("all"):
                self.subjects = sorted(self.all_subjects)
            case _:
                random.seed(self.random_seed)
                randomized_subjects = self.all_subjects.copy()

                
                
        # Convert `subjects` into a sequence of subjectID strings
        if (
            subjects is None
            or isinstance(subjects, int)
            or (isinstance(subjects, list) and isinstance(subjects[0], int))
        ):
            # if subjects is an int, get that many random subjects
            random.seed(self.random_seed)
            randomized_subjects = self.all_subjects.copy()
            random.shuffle(randomized_subjects)
            if subjects is None:
                subjects = randomized_subjects[0]
            elif isinstance(subjects, int):
                subjects = randomized_subjects[:subjects]
            else:
                subjects = [randomized_subjects[i] for i in subjects]

            if isinstance(subjects, str):
                subjects = [subjects]
        elif subjects == "all":
            # if "all," retrieve all subjects
            subjects = sorted(self.all_subjects)
        elif isinstance(subjects, str):
            # if a string, just get that one subject
            subjects = [subjects]
        # The last case for subjects is what we want. No transformation needed.
        
        subjects = set(subjects)
        if not subjects <= set(self.all_subjects):
            raise ValueError(
                f"The following subjects could not be found in the study: "
                f"{subjects - set(self.all_subjects)}",
            )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            subs = [ executor.submit(self._get_subject, s) for s in subjects ]
            logging.getLogger(__name__).info(msg=f"Retrieving N={len(subjects)} subject S3 keys")
            self.subjects = [f.result() for f in tqdm(concurrent.futures.as_completed(subs), total=len(subs))]
            

        print("Retrieving subject S3 keys")
        with ProgressBar():
            subjects = list(compute(*subs, scheduler="threads"))

        self._subjects = subjects

    def __repr__(self):
        return f"{type(self).__name__}(study_id={self.study_id}, " f"bucket={self.bucket}, s3_prefix={self.s3_prefix})"

    def _get_subject(self, subject_id):
        """Return a Subject instance from a subject-ID"""
        return self._subject_class(subject_id=subject_id, study=self)

    def _get_derivative_types(self):
        """Return a list of available derivatives pipelines

        Returns
        -------
        list
            list of available derivatives pipelines
        """
        s3_prefix = "/".join([self.bucket, self.s3_prefix]).rstrip("/")
        nonsub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["other"]
        derivatives_prefix = "/".join([s3_prefix, "derivatives"])
        if derivatives_prefix in nonsub_keys:
            return _ls_s3fs(
                s3_prefix=derivatives_prefix,
                anon=self.anon,
            )["other"]
        else:
            return []

    def _get_non_subject_s3_keys(self):
        """Return a list of 'non-subject' files

        In this context, a 'non-subject' file is any file
        or directory that is not a subject ID folder

        Returns
        -------
        dict
            dict with keys 'raw' and 'derivatives' and whose values
            are lists of S3 keys for non-subject files
        """
        non_sub_s3_keys = {}

        s3_prefix = "/".join([self.bucket, self.s3_prefix]).rstrip("/")

        nonsub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["other"]
        nonsub_keys = [k for k in nonsub_keys if not k.endswith("derivatives")]

        nonsub_deriv_keys = []
        for dt in self.derivative_types:
            nonsub_deriv_keys.append(
                _ls_s3fs(
                    s3_prefix=dt,
                    anon=self.anon,
                )["other"],
            )

        non_sub_s3_keys = {
            "raw": nonsub_keys,
            "derivatives": nonsub_deriv_keys,
        }

        return non_sub_s3_keys

    def _list_all_subjects(self):
        """Return list of subjects

        Returns
        -------
        list
            list of participant_ids
        """
        if self._use_participants_tsv:
            tsv_key = "/".join([self.s3_prefix, "participants.tsv"]).lstrip("/")
            s3 = get_s3_client(anon=self.anon)

            def get_subs_from_tsv_key(s3_key):
                response = s3.get_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                )
                #f = StringIO(response.get('Body', 'participant_id\n'))
                # Make sorted list of unique participant_ids with no empty strings
                # botocore.response.StreamingBody.iter_lines() returns an iterator over lines...
                b_line_iterator = response["Body"].iter_lines()
                # ..however, they are bytes, and we need to decode the bytes to strings:
                s_line_iterator = (line.decode("utf-8") for line in b_line_iterator)
                # Read the lines and get the participant_id column and return them in the original order but deduplicated:
                dct = dict.fromkeys(x.get('participant_id', '') for x in csv.DictReader(s_line_iterator, dialect=csv.excel_tab))
                # Remove possible empty strings, although this should not be necessary:
                dct.pop('', None)
                dct.pop(' ', None)
                return list(dct)

            subjects = get_subs_from_tsv_key(tsv_key)
        else:
            s3_prefix = f"{self.bucket}/{self.s3_prefix}".rstrip("/")
            sub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["subjects"]

            # Just need BIDSLayout for the `parse_file_entities`
            dd = tempfile.TemporaryDirectory()
            layout = BIDSLayout(dd.name, validate=False)
            subjects: list[str] = list(
                dict.fromkeys(
                "sub-" + layout.parse_file_entities(key)["subject"] for key in sub_keys # if we are prepending entities["subject"] with "sub-", then entities["subject"] MUST be a string, so .get() is not necessary
                )
            )
            return subjects

    def _download_non_sub_keys(
        self,
        directory: str,
        select: Collection[str] | str = ("dataset_description.json",),
        filenames: Collection[str] | None = None,
    ):
        fs = s3fs.S3FileSystem(anon=self.anon)
        filenames = filenames if filenames is not None else self.non_sub_s3_keys["raw"]
        match select:
            case str("all"):
                matches = filenames
            case str():
                matches = [fname for fname in filenames if select in fname]
            case Collection():
                matches = [fname for fname in filenames if any(substring in fname for substring in select)]
            case _:
                matches = []
        for fname in matches:
            os.makedirs(os.path.dirname(fname), exist_ok=True)
            fs.get(fname, os.path.join(directory, os.path.basename(fname)))
    
    def _download_derivative_descriptions(self, include_derivs: bool | str | list | None = None, directory: str):
        for derivative in self.derivative_types:
            if (
                include_derivs is True
                or (isinstance(include_derivs, str) and include_derivs == os.path.basename(derivative))
                or (
                    isinstance(include_derivs, list)
                    and all(isinstance(s, str) for s in include_derivs)
                    and any([deriv in derivative for deriv in include_derivs])
                )
            ):
                filenames = _ls_s3fs(s3_prefix=derivative, anon=self.anon)["other"]
                deriv_directory = os.path.join(directory, *derivative.split("/")[-2:])
                self._download_non_sub_keys(
                    deriv_directory,
                    select=("dataset_description.json",),
                    filenames=filenames,
                )

    def download(
        self,
        directory: str,
        include_modality_agnostic: bool | str | Collection[str] =("dataset_description.json",),
        include_derivs: bool | str = False,
        include_derivs_dataset_description: bool = True,
        suffix: str | None =None,
        overwrite: bool =False,
        pbar: bool = True,
    ):
        """Download files for each subject in the study

        Parameters
        ----------
        directory : str
            Directory to which to download subject files

        include_modality_agnostic : bool, "all" or any subset of [
                "dataset_description.json", "CHANGES", "README", "LICENSE"]
            If True or "all", download all keys in self.non_sub_s3_keys
            also. If a subset of ["dataset_description.json", "CHANGES",
            "README", "LICENSE"], download only those files. This is
            useful if the non_sub_s3_keys contain files common to all
            subjects that should be inherited.
            Default: ("dataset_description.json",)

        include_derivs : bool or str
            If True, download all derivatives files. If False, do not.
            If a string or sequence of strings is passed, this will
            only download derivatives that match the string(s) (e.g.
            ["dmriprep", "afq"]). Default: False

        include_derivs_dataset_description : bool
            Used only if include_derivs is not False. If True,
            dataset_description.json downloaded for each derivative.

        suffix : str
            Suffix, including extension, of file(s) to download.
            Default: None

        overwrite : bool
            If True, overwrite files for each subject. Default: False

        pbar : bool
            If True, include progress bar. Default: True

        See Also
        --------
        AFQ.data.S3BIDSSubject.download
        """
        self.local_directories = list(dict.fromkeys(self.local_directories + [directory]))
        
        match include_modality_agnostic:
            case str("all") | True:
                self._download_non_sub_keys(directory, select="all")
            case False:
                pass
            case _:
                valid_set = {"dataset_description.json", "CHANGES", "README", "LICENSE"}
                if not set(include_modality_agnostic) < valid_set:
                    raise ValueError(f"include_modality_agnostic must be either  `bool`, 'all', or a subset of {valid_set}")
                self._download_non_sub_keys(
                    directory,
                    select=list(include_modality_agnostic)
                )

        # download dataset_description.json for derivatives
        if (include_derivs is not False) and include_derivs_dataset_description:
            self._download_derivative_descriptions(include_derivs, directory)

        results = [
            delayed(sub.download)( # TODO: what is sub.download?
                directory=directory,
                include_derivs=include_derivs,
                suffix=suffix,
                overwrite=overwrite,
                pbar=pbar,
                pbar_idx=idx,
            )
            for idx, sub in enumerate(self.subjects)
        ]

        compute(*results, scheduler="threads")

    
    @property
    def study_id(self) -> str:
        """An identifier string for the study"""
        return self._study_id
    
    @property
    def bucket(self) -> str:
        """The S3 bucket that contains the study data"""
        return self._bucket
    
    @property
    def s3_prefix(self) -> str:
        """The S3 prefix common to all of the study objects on S3"""
        return self._s3_prefix
    
    @property
    def anon(self) -> bool:
        """Whether to use anonymous connection (public buckets only)"""
        return self._anon
    
    @property
    def use_participants_tsv(self) -> bool:
        """If True, use the particpants tsv files to retrieve subject identifiers"""
        return self._use_participants_tsv
    
    @property
    def random_seed(self) -> int | None:
        """Random seed for selection of subjects if `subjects` is an integer"""
        return self._random_seed
    
    @property
    def _subject_class(self) -> type[S3BIDSSubject]:
        """The subject class to be used for this study"""
        return self.__subject_class
    
    @property
    def local_directories(self) -> Collection[str]:
        """A list of local directories where this study has been downloaded"""
        return self._local_directories
    
    @property
    def all_subjects(self) -> Collection[str]:
        """A list of all subjects in the study"""
        return self._all_subjects
    
    @property
    def derivative_types(self) -> Collection[str]:
        """A list of derivative pipelines available in this study"""
        return self._derivative_types
    
    @property
    def non_subject_s3_keys(self) -> dict[str, Collection[str]]:
        """A dict of S3 keys that are not in subject directories"""
        return self._non_subject_s3_keys
    
    @property
    def subjects(self) -> Collection[S3BIDSSubject]:
        """A list of S3BIDSSubject instances for this study"""
        return self._subjects
    

class HBNSite(S3BIDSStudy):
    """An HBN study site

    See Also
    --------
    AFQ.data.S3BIDSStudy
    """

    def __init__(
        self,
        site,
        study_id="HBN",
        bucket="fcp-indi",
        s3_prefix="data/Projects/HBN/MRI",
        subjects=None,
        use_participants_tsv=False,
        random_seed=None,
    ):
        """Initialize the HBN site

        Parameters
        ----------
        site : ["Site-SI", "Site-RU", "Site-CBIC", "Site-CUNY"]
            The HBN site

        study_id : str
            An identifier string for the site

        bucket : str
            The S3 bucket that contains the study data

        s3_prefix : str
            The S3 prefix common to all of the study objects on S3

        subjects : str, sequence(str), int, or None
            If int, retrieve S3 keys for the first `subjects` subjects.
            If "all", retrieve all subjects. If str or sequence of
            strings, retrieve S3 keys for the specified subjects. If
            None, retrieve S3 keys for the first subject. Default: None

        use_participants_tsv : bool
            If True, use the particpants tsv files to retrieve subject
            identifiers. This is faster but may not catch all subjects.
            Sometimes the tsv files are outdated. Default: False

        random_seed : int or None
            Random seed for selection of subjects if `subjects` is an
            integer. Use the same random seed for reproducibility.
            Default: None
        """
        valid_sites = ["Site-SI", "Site-RU", "Site-CBIC", "Site-CUNY"]
        if site not in valid_sites:
            raise ValueError(
                f"site must be one of {valid_sites}.",
            )

        self._site = site

        super().__init__(
            study_id=study_id,
            bucket=bucket,
            s3_prefix="/".join([s3_prefix, site]),
            subjects=subjects,
            use_participants_tsv=use_participants_tsv,
            random_seed=random_seed,
            _subject_class=HBNSubject,
        )

    @property
    def site(self):
        """The HBN site"""
        return self._site

    def _get_subject(self, subject_id):
        """Return a Subject instance from a subject-ID"""
        return self._subject_class(subject_id=subject_id, study=self, site=self.site)

    def _get_derivative_types(self):
        """Return a list of available derivatives pipelines

        The HBN dataset is not BIDS compliant so to go a list
        of available derivatives, we must peak inside every
        directory in `derivatives/sub-XXXX/`

        Returns
        -------
        list
            list of available derivatives pipelines
        """
        s3_prefix = "/".join([self.bucket, self.s3_prefix]).rstrip("/")
        nonsub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["other"]
        derivatives_prefix = "/".join([s3_prefix, "derivatives"])

        if any([derivatives_prefix in key for key in nonsub_keys]):
            deriv_subs = _ls_s3fs(
                s3_prefix=derivatives_prefix,
                anon=self.anon,
            )["subjects"]

            deriv_types = []
            for sub_key in deriv_subs:
                deriv_types += [
                    s.split(sub_key)[-1].lstrip("/")
                    for s in _ls_s3fs(
                        s3_prefix=sub_key,
                        anon=self.anon,
                    )["subjects"]
                ]

            return list(set(deriv_types))
        else:
            return []

    def _get_non_subject_s3_keys(self):
        """Return a list of 'non-subject' files

        In this context, a 'non-subject' file is any file
        or directory that is not a subject ID folder. This method
        is different from AFQ.data.S3BIDSStudy because the HBN
        dataset is not BIDS compliant

        Returns
        -------
        dict
            dict with keys 'raw' and 'derivatives' and whose values
            are lists of S3 keys for non-subject files

        See Also
        --------
        AFQ.data.S3BIDSStudy._get_non_subject_s3_keys
        """
        non_sub_s3_keys = {}

        s3_prefix = "/".join([self.bucket, self.s3_prefix]).rstrip("/")

        nonsub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["other"]
        nonsub_keys = [k for k in nonsub_keys if not k.endswith("derivatives")]

        nonsub_deriv_keys = _ls_s3fs(
            s3_prefix="/".join(
                [
                    self.bucket,
                    self.s3_prefix,
                    "derivatives",
                ],
            ),
            anon=self.anon,
        )["other"]

        non_sub_s3_keys = {
            "raw": nonsub_keys,
            "derivatives": nonsub_deriv_keys,
        }

        return non_sub_s3_keys

    def download(
        self,
        directory,
        include_modality_agnostic=False,
        include_derivs=False,
        overwrite=False,
        pbar=True,
    ):
        """Download files for each subject in the study

        Parameters
        ----------
        directory : str
            Directory to which to download subject files

        include_modality_agnostic : bool, "all" or any subset of [
                "dataset_description.json", "CHANGES", "README", "LICENSE"]
            If True or "all", download all keys in self.non_sub_s3_keys
            also. If a subset of ["dataset_description.json", "CHANGES",
            "README", "LICENSE"], download only those files. This is
            useful if the non_sub_s3_keys contain files common to all
            subjects that should be inherited. Default: False

        include_derivs : bool or str
            If True, download all derivatives files. If False, do not.
            If a string or sequence of strings is passed, this will
            only download derivatives that match the string(s) (e.g.
            ["dmriprep", "afq"]). Default: False

        overwrite : bool
            If True, overwrite files for each subject. Default: False

        pbar : bool
            If True, include progress bar. Default: True

        See Also
        --------
        AFQ.data.S3BIDSSubject.download
        """
        super().download(
            directory=directory,
            include_modality_agnostic=include_modality_agnostic,
            include_derivs=include_derivs,
            overwrite=overwrite,
            pbar=pbar,
        )

        to_bids_description(
            directory,
            BIDSVersion="1.0.0",
            Name="HBN Study, " + self.site,
            DatasetType="raw",
            Subjects=[s.subject_id for s in self.subjects],
        )


# +--------------------------------------------------+
# | End S3BIDSStudy classes and supporting functions |
# +--------------------------------------------------+


def s3fs_nifti_write(img, fname, fs=None):
    """
    Write a nifti file straight to S3

    Paramters
    ---------
    img : nib.Nifti1Image class instance
        The image containing data to be written into S3
    fname : string
        Full path (including bucket name and extension) to the S3 location
        where the file is to be saved.
    fs : an s3fs.S3FileSystem class instance, optional
        A file-system to refer to. Default to create a new file-system
    """
    if fs is None:
        fs = s3fs.S3FileSystem()

    bio = BytesIO()
    file_map = img.make_file_map({"image": bio, "header": bio})
    img.to_file_map(file_map)
    data = gzip.compress(bio.getvalue())
    with fs.open(fname, "wb") as ff:
        ff.write(data)


def s3fs_nifti_read(fname, fs=None, anon=False):
    """
    Lazily reads a nifti image from S3.

    Paramters
    ---------
    fname : string
        Full path (including bucket name and extension) to the S3 location
        of the file to be read.
    fs : an s3fs.S3FileSystem class instance, optional
        A file-system to refer to. Default to create a new file-system.
    anon : bool
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order). Default: True

    Returns
    -------
    nib.Nifti1Image class instance

    Notes
    -----
    Because the image is lazily loaded, data stored in the file
    is not transferred until `get_fdata` is called.

    """
    if fs is None:
        fs = s3fs.S3FileSystem(anon=anon)
    with fs.open(fname) as ff:
        zz = gzip.open(ff)
        rr = zz.read()
        bb = BytesIO(rr)
        fh = nib.FileHolder(fileobj=bb)
        img = nib.Nifti1Image.from_file_map({"header": fh, "image": fh})
    return img


def s3fs_json_read(fname, fs=None, anon=False):
    """
    Reads json directly from S3

    Paramters
    ---------
    fname : str
        Full path (including bucket name and extension) to the file on S3.
    fs : an s3fs.S3FileSystem class instance, optional
        A file-system to refer to. Default to create a new file-system.
    anon : bool
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order). Default: True
    """
    if fs is None:
        fs = s3fs.S3FileSystem(anon=anon)
    with fs.open(fname) as ff:
        data = json.load(ff)
    return data


def s3fs_json_write(data, fname, fs=None):
    """
    Writes json from a dict directly into S3

    Parameters
    ----------
    data : dict
        The json to be written out
    fname : str
        Full path (including bucket name and extension) to the file to
        be written out on S3
    fs : an s3fs.S3FileSystem class instance, optional
        A file-system to refer to. Default to create a new file-system.
    """
    if fs is None:
        fs = s3fs.S3FileSystem()
    with fs.open(fname, "w") as ff:
        json.dump(data, ff)
