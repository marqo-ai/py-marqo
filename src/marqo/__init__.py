import logging



def set_log_level(level):
    package_logger = logging.getLogger('marqo')
    package_logger.setLevel(level)

