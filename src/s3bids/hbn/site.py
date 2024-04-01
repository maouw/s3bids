import weakref

from s3bids import S3BIDSStudy
from s3bids.hbn.subject import HBNSubject
from s3bids.utils import _ls_s3fs, to_bids_description


class HBNSite(S3BIDSStudy):
    """An HBN study site

    See Also
    --------
    AFQ.data.S3BIDSStudy
    """

    def __init__(
        self,
        site: str,
        study_id: str = "HBN",
        bucket: str = "fcp-indi",
        s3_prefix: str = "data/Projects/HBN/MRI",
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
        if site not in (valid_sites := ("Site-SI", "Site-RU", "Site-CBIC", "Site-CUNY")):
            raise ValueError(
                f"site must be one of {valid_sites=}.",
            )

        self._site = site

        super().__init__(
            study_id=study_id,
            bucket=bucket,
            s3_prefix=s3_prefix + "/" + site,
            subjects=subjects,
            use_participants_tsv=use_participants_tsv,
            random_seed=random_seed,
            _subject_class=HBNSubject,
        )

    @property
    def site(self):
        """The HBN site"""
        return self._site

    def _get_subject(self, subject_id, *args, **kwargs):
        """Return a Subject instance from a subject-ID"""
        return HBNSubject(subject_id=subject_id, study=weakref.proxy(self), site=self.site)

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
