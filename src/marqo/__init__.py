from marqo.client import Client
from marqo.marqo_logging import logger
from marqo.enums import SearchMethods
from marqo.version import supported_marqo_version


def set_log_levels(level):
    logger.setLevel(level)

