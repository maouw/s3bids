try:
    from ._version import __version__
except ImportError:
    pass

from s3bids.subject import *
from s3bids.study import *
