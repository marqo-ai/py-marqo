from marqo.client import Client
from marqo.enums import SearchMethods
from marqo.version import supported_marqo_version
import logging
from marqo.version import __version__


def set_log_level(level):
    package_logger = logging.getLogger('marqo')
    package_logger.setLevel(level)

