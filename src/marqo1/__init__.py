from marqo1.client import Client
from marqo1.enums import SearchMethods
from marqo1.version import supported_marqo_version
import logging

__version__ = "2.1.0"

def set_log_level(level):
    package_logger = logging.getLogger('marqo')
    package_logger.setLevel(level)

