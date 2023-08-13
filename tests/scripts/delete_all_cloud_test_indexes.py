import os

import marqo


def delete_all_test_indices():
    local_marqo_settings = {
        "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
    }
    suffix = os.environ.get("MQ_TEST_RUN_IDENTIFIER", None)
    api_key = os.environ.get("MARQO_API_KEY", None)
    if api_key:
        local_marqo_settings["api_key"] = api_key
    client = marqo.Client(**local_marqo_settings)
    indexes = client.get_indexes()
    for index in indexes['results']:
        if index.index_name.startswith('test-index'):
            if suffix is not None and suffix in index.index_name.split('-'):
                if index.get_status()["index_status"] == marqo.enums.IndexStatus.READY:
                    index.delete(wait_for_readiness=False)
    print("All test indices has been deleted")


if __name__ == '__main__':
    delete_all_test_indices()
