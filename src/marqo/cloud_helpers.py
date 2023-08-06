import time

from marqo.marqo_logging import mq_logger


def cloud_wait_for_index_status(req , index_name: str, status):
    """ Wait for index to be created on Marqo Cloud by checking
    it's status every 10 seconds until it becomes expected value"""
    current_status = req.get(f"indexes/{index_name}/status")
    while current_status['index_status'] != status:
        time.sleep(10)
        current_status = req.get(f"indexes/{index_name}/status")
        mq_logger.info(f"Index creation status: {current_status['index_status']}")
    mq_logger.info("Index created successfully")
    return True
