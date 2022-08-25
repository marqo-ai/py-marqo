from marqo.client import Client
from marqo.marqo_logging import logger
from marqo.enums import SearchMethods


def set_log_levels(level):
    logger.setLevel(level)


