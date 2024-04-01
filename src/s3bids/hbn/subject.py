from s3bids import S3BIDSSubject
from s3bids.hbn.site import HBNSite
from s3bids.utils import _get_matching_s3_keys

class HBNSubject(S3BIDSSubject):
    """A subject in the HBN study

    See Also
    --------
    AFQ.data.S3BIDSSubject
    """

    def __init__(self, subject_id: str, study: HBNSite, site: str | None = None):
        """Initialize a Subject instance

        Parameters
        ----------
        subject_id : str
            Subject-ID for this subject

        study : AFQ.data.S3BIDSStudy
            The S3BIDSStudy for which this subject was a participant

        site : str, optional
            Site-ID for the site from which this subject's data was collected
        """
        if not isinstance(site, str | None):
            raise TypeError("site must be a string or None.")

        self._site = getattr(study, "site", None) if site is None else site

        super().__init__(subject_id=subject_id, study=study)

    @property
    def site(self):
        """The site at which this subject was a participant"""
        return self._site

    def __repr__(self):
        return (
            f"{type(self).__name__}(subject_id={self.subject_id}, " f"study_id={self.study.study_id}, site={self.site})"
        )

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
