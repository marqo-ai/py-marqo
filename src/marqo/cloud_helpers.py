import time

from marqo.marqo_logging import mq_logger
from marqo._httprequests import HttpRequests
from marqo.enums import IndexStatus


def cloud_wait_for_index_status(req: HttpRequests, index_name: str, status: IndexStatus):
    """ Wait for index to achieve some status on Marqo Cloud by checking
    it's status every 10 seconds until it becomes expected value

    Args:
        req (HttpRequests): HttpRequests object
        index_name (str): name of the index
        status (IndexStatus): expected status of the index
    """
    current_status = req.get(f"indexes/{index_name}/status")
    while current_status['index_status'] != status:
        time.sleep(10)
        current_status = req.get(f"indexes/{index_name}/status")
        mq_logger.info(f"Current index status: {current_status['index_status']}")
    mq_logger.info(f"Index achieved status {status} successfully")
    return True
