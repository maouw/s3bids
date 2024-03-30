import gzip
import json
import os
import tempfile
import typing

# from dask.base import compute
# from dask.delayed import delayed
# from dask.diagnostics.progress import ProgressBar
from collections.abc import Iterator
from io import BytesIO
from typing import Any

import boto3
import nibabel as nib
import s3fs
from bids import BIDSLayout
from botocore import UNSIGNED
from botocore.client import Config

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
def get_s3_client(*, anon=True) -> S3Client:
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

    entities: list[dict[str, Any]] = [layout.parse_file_entities(f) for f in site_files]

    return {
        "subjects": [f for f, e in zip(site_files, entities, strict=False) if e.get("subject") is not None],
        "other": [f for f, e in zip(site_files, entities, strict=False) if e.get("subject") is None],
    }


def _get_matching_s3_keys(bucket: str, prefix: str = "", suffix: str = "", *, anon=True) -> Iterator[str]:
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


def _download_from_s3(fname: str, bucket: str, key: str, *, overwrite: bool = False, anon: bool = True):
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
