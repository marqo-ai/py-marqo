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
    indices_to_delete = []
    for index in indexes['results']:
        if index.index_name.startswith('test-index'):
            if suffix is not None and suffix in index.index_name.split('-'):
                indices_to_delete.append(index.index_name)

    for index_name in indices_to_delete:
        index = client.index(index_name)
        if index.get_status()["index_status"] == marqo.enums.IndexStatus.READY:
            index.delete(wait_for_readiness=False)


if __name__ == '__main__':
    delete_all_test_indices()
