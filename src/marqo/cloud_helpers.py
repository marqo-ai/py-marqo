import time

from marqo.marqo_logging import mq_logger


def cloud_wait_for_index_status(req, index_name, status):
    creation = req.get(f"indexes/{index_name}/status")
    while creation['index_status'] != status:
        time.sleep(10)
        creation = req.get(f"indexes/{index_name}/status")
        mq_logger.info(f"Index creation status: {creation['index_status']}")
    mq_logger.info("Index created successfully")
    return True
