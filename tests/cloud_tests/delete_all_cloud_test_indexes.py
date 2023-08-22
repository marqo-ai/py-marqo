import os
import requests
import marqo


def delete_all_test_indices(wait_for_readiness=False):
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
            elif suffix is None:
                indices_to_delete.append(index.index_name)

    for index_name in indices_to_delete:
        index = client.index(index_name)
        if index.get_status()["index_status"] == marqo.enums.IndexStatus.READY:
            index.delete(wait_for_readiness=False)
    if wait_for_readiness:
        max_retries = 100
        attempt = 0
        while indices_to_delete:
            resp = requests.get(f"{client.config.instance_mapping.get_control_base_url()}/indexes",
                                headers={"x-api-key": client.config.api_key})
            resp_json = resp.json()
            all_index_names = [index["index_name"] for index in resp_json['results']]
            for index_for_deletion_name in indices_to_delete:
                if index_for_deletion_name not in all_index_names:
                    indices_to_delete.remove(index_for_deletion_name)
            if attempt > max_retries:
                raise RuntimeError("Timed out waiting for indices to be deleted, still remaining: "
                                   f"{indices_to_delete}. Please delete manually")
        print("All test indices deleted successfully")


if __name__ == '__main__':
    delete_all_test_indices()
