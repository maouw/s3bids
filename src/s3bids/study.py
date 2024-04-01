import concurrent.futures
import csv
import logging
import os
import random
import tempfile
import weakref
from collections.abc import Callable, Collection, Set

import s3fs
from bids import BIDSLayout
from tqdm.asyncio import tqdm

from src.s3bids.S3BIDSSubject import S3BIDSSubject
from src.s3bids.utils import _ls_s3fs, get_s3_client


class S3BIDSStudy:
    """A BIDS-compliant study hosted on AWS S3."""

    def __init__(
        self,
        study_id: str,
        bucket: str,
        s3_prefix: str = "",
        subjects: int | str | Collection[str] | None = None,
        anon: bool = True,
        use_participants_tsv: bool = False,
        random_seed: int | None = None,
        _subject_class: Callable[..., S3BIDSSubject] = S3BIDSSubject,
    ):
        """Initialize an S3BIDSStudy instance

        Parameters
        ----------
        study_id
            An identifier string for the study.

        bucket
            The S3 bucket that contains the study data.

        s3_prefix
            The S3 prefix common to all the study objects on S3.

            Default: the empty string, which indicates that the study
            is at the top level of the bucket.

        subjects
            If int, retrieve S3 keys for the first `subjects` subjects.

            If "all", retrieve all subjects.

            If str or sequence of strings, retrieve S3 keys for the specified subjects.

            If int or sequence of ints, then for each int `n` retrieve S3 keys for the nth subject.

            If `None`, retrieve S3 keys for the first subject.

        anon
            Whether to use anonymous connection (public buckets only).
            If `False`, uses the key/secret given, or boto’s credential
            resolver (client_kwargs, environment, variables, config
            files, EC2 IAM server, in that order).

        use_participants_tsv
            If `True`, use the `particpants.tsv` files to retrieve subject
            identifiers. This is faster but may not catch all subjects.
            Sometimes the `t.sv` files are outdated.

        random_seed
            Random seed for selection of subjects if `subjects` is an
            integer. Use the same random seed for reproducibility.

        _subject_class
            The subject class to be used for this study. This parameter
            has a leading underscore because you probably don't want
            to change it. If you do change it, you must provide a
            class that quacks like `S3BIDSSubject`

        _subject_class
            The subject class to be used for this study. This parameter
            has a leading underscore because you probably don't want
            to change it. If you do change it, you must provide a
            class that quacks like `S3BIDSSubject`

        Examples
        --------
        Access data stored in a bucket using credentials:
        >>> study = S3BIDSStudy("studyname", "bucketname", "/path/to/dataset/", anon=False)

        Access data stored in a publicly accessible bucket:
        >>> study = S3BIDSStudy(
        ...     "hbn", "fcp-indi", "data/Projects/HBN/BIDS_curated/derivatives/qsiprep/"
        ... )

        """
        logging.getLogger("botocore").setLevel(logging.WARNING)  # TODO: Setby environment variable?

        if not isinstance(study_id, str):
            raise TypeError("`study_id` must be a string.")

        if not isinstance(bucket, str):
            raise TypeError("`bucket` must be a string.")

        if not isinstance(s3_prefix, str):
            raise TypeError("`s3_prefix` must be a string.")

        if s3_prefix == "/":
            raise ValueError(
                "If the study is at the top level "
                "of the s3 bucket, please pass the "
                "empty string as the s3 prefix"
                "(the default value)"
            )

        match subjects:
            case str("all"):
                pass
            case str():
                subjects = [subjects]
            case None:
                subjects = 1
            case int():
                if subjects < 1:
                    raise ValueError(f"`subjects` must be greater than 0 if of type `int` (found {subjects})")
            case [int() as first, *rest]:
                for x in subjects:
                    if not isinstance(x, int):
                        raise TypeError(f"Elements in `subjects` must be of type `int` (found a {type(x).__name__})")
                    if x < 1:
                        raise ValueError(f"Element {x}  in `subjects` must be > 0")
            case [str() as first, *rest]:
                for x in subjects:
                    if not isinstance(x, str):
                        raise TypeError(f"Elements in `subjects` must be of type `str` (found a {type(x).__name__})")
            case _:
                raise ValueError(
                    f"`subjects` must be `None`, `int`, `string`, or a sequence of `int` or `str`. (found {type(subjects).__name__})"
                )

        if not isinstance(anon, bool):
            raise TypeError("`anon` must be of type `bool`.")

        if not isinstance(use_participants_tsv, bool):
            raise TypeError("`use_participants_tsv` must be of type `bool`.")

        if random_seed is not None and not isinstance(random_seed, int):
            raise TypeError("`random_seed` must be of type `int`.")

        self._study_id = study_id
        self._bucket = bucket
        self._s3_prefix = s3_prefix
        self._use_participants_tsv = use_participants_tsv
        self._random_seed = random_seed
        self._anon = anon
        self.__subject_class = _subject_class
        self._local_directories = []

        # Get a list of all subjects in the study
        self._all_subjects = self._get_all_subjects()
        self._derivative_types = self._get_derivative_types()
        self._non_subject_s3_keys = self._get_non_subject_s3_keys()

        match subjects:
            case "all":
                selected_subjects = self._all_subjects.copy()
            case int():
                if subjects < len(subjects):
                    raise ValueError(
                        f"Cannot select {subjects} subjects because only {len(subjects)} subjects are available."
                    )
                selected_subjects = random.sample(list(self._all_subjects))
            case [int() as first, *rest]:
                selected_subjects = list(self._all_subjects)
                random.shuffle(selected_subjects)
                try:
                    selected_subjects = [selected_subjects[i] for i in subjects]
                except IndexError as e:
                    raise ValueError(f"Index out of range for subjects of length {len(selected_subjects)}") from e
            case [str() as first, *rest]:
                if not_found := set(subjects) - self._all_subjects:
                    msg = f"{len(not_found)} of the supplied subjects could not be found in the study"
                    if len(not_found) <= 10:
                        msg = msg + "\n" + "  " + str(not_found)
                    raise ValueError(msg)
                selected_subjects = subjects
            case _:
                selected_subjects = []

        # TODO: Should we 'del subjects' for memory issues?

        with concurrent.futures.ThreadPoolExecutor() as executor:
            subs = [executor.submit(self._get_subject, s) for s in selected_subjects]
            logging.getLogger(__name__).info(msg=f"Retrieving N={len(subjects)} subject S3 keys")
            # TODO: make tqdm() optional on par:
            result = [f.result() for f in tqdm(concurrent.futures.as_completed(subs), total=len(subs))]
            self._subjects = dict.fromkeys(result)

    def __repr__(self):
        return f"{type(self).__name__}(study_id={self.study_id}, " f"bucket={self.bucket}, s3_prefix={self.s3_prefix})"

    def _get_subject(self, subject_id: str, *args, **kwargs) -> S3BIDSSubject:
        """Return a Subject instance from a subject-ID"""
        return S3BIDSSubject(subject_id=subject_id, study=weakref.proxy(self))

    def _get_derivative_types(self) -> list:
        """Return a list of available derivatives pipelines

        Returns
        -------
        list
            list of available derivatives pipelines
        """
        s3_prefix = self.bucket + self.s3_prefix.rstrip("/")
        nonsub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["other"]
        derivatives_prefix = "/".join([s3_prefix, "derivatives"])
        if derivatives_prefix in nonsub_keys:
            return _ls_s3fs(
                s3_prefix=derivatives_prefix,
                anon=self.anon,
            )["other"]
        else:
            return []

    def _get_non_subject_s3_keys(self) -> dict[str, list]:
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

        s3_prefix = self.bucket + self.s3_prefix.rstrip("/")
        nonsub_keys = [
            x for x in _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["other"] if not x.endswith("derivatives")
        ]
        non_sub_s3_keys = {
            "raw": nonsub_keys,
            "derivatives": [_ls_s3fs(s3_prefix=dt, anon=self.anon)["other"] for dt in self.derivative_types],
        }

        return non_sub_s3_keys

    def _get_all_subjects(self) -> Set:
        """Return list of subjects.

        Returns
        -------
        Set
            Unique ordered participant IDs.
        """
        if self._use_participants_tsv:
            tsv_key = self.s3_prefix.lstrip("/") + "participants.tsv"
            s3 = get_s3_client(anon=self.anon)

            def get_subs_from_tsv_key(s3_key):
                response = s3.get_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                )
                # f = StringIO(response.get('Body', 'participant_id\n'))
                # Make sorted list of unique participant_ids with no empty strings
                # botocore.response.StreamingBody.iter_lines() returns an iterator over lines...
                b_line_iterator = response["Body"].iter_lines()
                # ..however, they are bytes, and we need to decode the bytes to strings:
                s_line_iterator = (line.decode("utf-8") for line in b_line_iterator)
                # Read the lines and get the participant_id column:
                reader = csv.DictReader(s_line_iterator, dialect=csv.excel_tab)
                dct = dict.fromkeys(row.get("participant_id", "") for row in reader)
                # Remove possible empty strings, although this should not be necessary:
                dct.pop("", None)
                dct.pop(" ", None)
                return dct.keys()

            subjects = get_subs_from_tsv_key(tsv_key)
        else:
            s3_prefix = f"{self.bucket}/{self.s3_prefix}".rstrip("/")
            sub_keys = _ls_s3fs(s3_prefix=s3_prefix, anon=self.anon)["subjects"]

            # Just need BIDSLayout for the `parse_file_entities`
            dd = tempfile.TemporaryDirectory()
            layout = BIDSLayout(dd.name, validate=False)
            subjects = dict.fromkeys(
                "sub-" + layout.parse_file_entities(key)["subject"]
                for key in sub_keys  # if we are prepending entities["subject"] with "sub-", then entities["subject"] MUST be a string, so .get() is not necessary
            )
        return subjects.keys()

    def _download_non_sub_keys(
        self,
        directory: str,
        select: Collection[str] | str = ("dataset_description.json",),
        filenames: Collection[str] | None = None,
    ):
        fs = s3fs.S3FileSystem(anon=self.anon)
        if filenames is None:
            filenames = self.non_subject_s3_keys["raw"]

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

    def _download_derivative_descriptions(self, include_derivs: bool | str | list, directory: str):
        for derivative in self.derivative_types:
            if (
                include_derivs is True
                or (isinstance(include_derivs, str) and include_derivs == os.path.basename(derivative))
                or (
                    isinstance(include_derivs, list)
                    and all(isinstance(s, str) for s in include_derivs)
                    and any(deriv in derivative for deriv in include_derivs)
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
        include_modality_agnostic: bool | str | Collection[str] = ("dataset_description.json",),
        include_derivs: bool | str = False,
        include_derivs_dataset_description: bool = True,
        suffix: str | None = None,
        overwrite: bool = False,
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
        self._local_directories = list(dict.fromkeys(self.local_directories, directory))

        match include_modality_agnostic:
            case str("all") | True:
                self._download_non_sub_keys(directory, select="all")
            case False:
                pass
            case _:
                valid_set = {"dataset_description.json", "CHANGES", "README", "LICENSE"}
                if not set(include_modality_agnostic) < valid_set:
                    raise ValueError(
                        f"include_modality_agnostic must be either  `bool`, 'all', or a subset of {valid_set}"
                    )
                self._download_non_sub_keys(directory, select=list(include_modality_agnostic))

        # download dataset_description.json for derivatives
        if (include_derivs is not False) and include_derivs_dataset_description:
            self._download_derivative_descriptions(include_derivs, directory)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            subs = [
                executor.submit(
                    sub.download,
                    directory=directory,
                    include_derivs=include_derivs,
                    suffix=suffix,
                    overwrite=overwrite,
                    pbar=pbar,
                    pbar_idx=idx,
                )
                for idx, sub in enumerate(self.subjects)
            ]
            logging.getLogger(__name__).info(msg=f"Retrieving files for N={len(self.subjects)} subjects")
            # TODO: make tqdm() optional on par:
            result = [f.result() for f in tqdm(concurrent.futures.as_completed(subs), total=len(subs))]
            self._subjects = dict.fromkeys(result)

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
    def all_subjects(self) -> list[str]:
        """A list of all subjects in the study"""
        return list(self._all_subjects)

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
        return list(self._subjects)
