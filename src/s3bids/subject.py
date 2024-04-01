import contextlib
import logging
import os
from dataclasses import dataclass, field
from typing import Any
import weakref
from tqdm.asyncio import tqdm

from s3bids import S3BIDSStudy

from s3bids.utils import _download_from_s3, _get_matching_s3_keys


class S3BIDSSubject:
    """A single study subject hosted on AWS S3."""

    subject_id: str
    """Subject-ID for this subject."""
    study: "S3BIDSStudy"
    """The S3BIDSStudy for which this subject was a participant."""

    def __post_init__(self):
        logging.getLogger("botocore").setLevel(logging.WARNING)  # TODO: Setby environment variable?
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
        include_derivs: bool = False,
        overwrite: bool = False,
        suffix: str | None = None,
        pbar: bool = True,
        pbar_idx: int = 0,
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
        if not isinstance(directory, str):  # TOOD: Better to ask forgiveness than permission
            raise TypeError("directory must be a string.")

        if not (isinstance(include_derivs, bool | str)):  # TOOD: Better to ask forgiveness than permission
            raise TypeError(
                "include_derivs must be a boolean, a " "string, or a sequence of strings.",
            )

        if not isinstance(overwrite, bool):  # TOOD: Better to ask forgiveness than permission
            raise TypeError("overwrite must be a boolean.")

        if (suffix is not None) and not (isinstance(suffix, str)):  # TOOD: Better to ask forgiveness than permission
            raise TypeError("suffix must be a string.")

        if not isinstance(pbar, bool):  # TOOD: Better to ask forgiveness than permission
            raise TypeError("pbar must be a boolean.")

        if not isinstance(pbar_idx, int):  # TOOD: Better to ask forgiveness than permission
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
                progress.update()  # type: ignore[attr-defined,name-defined]
        with contextlib.suppress(NameError, AttributeError):
            progress.update()  # type: ignore[attr-defined,name-defined]
            progress.close()  # type: ignore[attr-defined,name-defined]
