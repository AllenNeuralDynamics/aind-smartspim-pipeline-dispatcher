"""
Backwards-compatibility shim.

This module previously contained all utility functions.  They have been split
into domain-specific modules:

    utils/io.py            — file I/O, path helpers, shell execution
    utils/aws.py           — S3, Secrets Manager
    utils/notifications.py — email alerts, MS Teams (AlertBot)
    utils/schemas.py       — aind-data-schema generation
    utils/visualization.py — neuroglancer, wavelength, orientation helpers

All public names are re-exported here so that existing call-sites that do
    from utils import utils; utils.read_json_as_dict(...)
continue to work without modification.
"""

from utils.aws import *  # noqa: F401, F403
from utils.io import *  # noqa: F401, F403
from utils.notifications import *  # noqa: F401, F403
from utils.schemas import *  # noqa: F401, F403
from utils.versioning import *  # noqa: F401, F403
from utils.visualization import *  # noqa: F401, F403
