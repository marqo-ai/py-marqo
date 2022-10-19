from marqo.client import Client
from marqo.enums import SearchMethods
from marqo.version import supported_marqo_version
import logging


def set_log_level(level):
    package_logger = logging.getLogger('marqo')
    package_logger.setLevel(level)

