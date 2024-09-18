import os

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import time
import marqo
from marqo.enums import IndexStatus
from marqo.errors import MarqoWebError


@retry(stop=stop_after_attempt(5),  # Stop after 5 attempts
       wait=wait_exponential(multiplier=1, min=4, max=10),  # Wait exponentially between retries
       retry=retry_if_exception_type(requests.exceptions.RequestException))  # Retry on network-related exceptions
def fetch_marqo_indexes(client: marqo.Client):
    """A function to fetch all Marqo indexes with retries to handle transient network errors and Marqo API errors"""
    response = requests.get(f"{client.config.instance_mapping.get_control_base_url()}/v2/indexes",
                            headers={"x-api-key": client.config.api_key})
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       retry=retry_if_exception_type((requests.exceptions.RequestException, MarqoWebError)))
def fetch_marqo_index(client: marqo.Client, index_name: str):
    """A function to fetch a Marqo index by name with retries to handle transient network errors and Marqo API errors"""
    return client.index(index_name)

def get_unique_run_identifier():
    """
    Get the unique run identifier for this test.
    Prioritize environment variable MQ_TEST_RUN_IDENTIFIER, then use run ID from GitHub workflow.
    """

    index_suffix = os.environ.get("MQ_TEST_RUN_IDENTIFIER", None)
    if index_suffix:
        print(f"Using the environment variable MQ_TEST_RUN_IDENTIFIER: {index_suffix} as the unique identifier",
              flush=True)
        return index_suffix

    github_run_id = os.environ.get("MARQO_GITHUB_RUN_ID", None)
    if github_run_id:
        print(f"Found GitHub run ID: {github_run_id}. "
              f"Using the last 4 characters: {github_run_id[-4:]} as the unique identifier.", flush=True)
        return github_run_id[-4:]

    print("No unique identifier found. Please set the environment variable MQ_TEST_RUN_IDENTIFIER."
          "Deleting all indexes with the correct prefixes.", flush=True)
    return None

def delete_all_test_indices(wait_for_readiness=False):
    """ Delete all test indices from Marqo Cloud Account that match the following criteria:
    - index name starts with 'test_index'
    - index name contains the value of the environment variable MQ_TEST_RUN_IDENTIFIER
    ( if not specified then all indices that start with 'test_index' will be deleted )
    """
    local_marqo_settings = {
        "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
    }
    suffix = get_unique_run_identifier()
    prefix = "pymarqo"
    api_key = os.environ.get("MARQO_API_KEY", None)
    if api_key:
        local_marqo_settings["api_key"] = api_key
    print(f"Deleting all test indices from Marqo Cloud Account that match the following criteria:")
    print(f"- index name starts with '{prefix}' AND")
    print(f"- index name ends with the suffix: {suffix}\n")

    client = marqo.Client(**local_marqo_settings)
    indexes = client.get_indexes()
    indices_to_delete = []
    for index in indexes['results']:
        if index["indexName"].startswith(prefix):
            if suffix is not None and index["indexName"].endswith(suffix):
                indices_to_delete.append(index["indexName"])
            elif suffix is None:
                indices_to_delete.append(index["indexName"])

    if not indices_to_delete:
        print("No indices to delete. Exiting.")
        return

    print("Indices to delete: ", indices_to_delete)
    print("Marqo Cloud deletion responses:")

    # First pass will either
    # 1. If the index is READY, send it into DELETING
    # 2. If the index is DELETED, do nothing
    # 3. If the index is FAILED, send it into DELETING
    # 4. If the index is CREATING, MODIFYING, DELETING, do nothing

    for index_name in indices_to_delete:
        index = fetch_marqo_index(client, index_name)
        if index.get_status()["indexStatus"] == IndexStatus.READY:
            print(index_name, index.delete(wait_for_readiness=False))
        elif index.get_status()["indexStatus"] == IndexStatus.DELETED:
            print(f"Index {index_name} is already deleted")
        elif index.get_status()["indexStatus"] == IndexStatus.FAILED:
            print(f"Index {index_name} has failed status, deleting anyway")
            index.delete(wait_for_readiness=False)
        else:
            # Either CREATING, MODIFYING, DELETING.
            print(f"Index {index_name} is not ready for deletion, status: {index.get_status()['indexStatus']}")

    # All indexes now are either DELETING, CREATING, MODIFYING (might need future deletion)
    if wait_for_readiness:
        max_retries = 100
        attempt = 0

        while indices_to_delete:
            print(f"Attempt #{attempt} at trying to delete indices: {indices_to_delete}", flush=True)
            resp = fetch_marqo_indexes(client)
            resp_json = resp.json()
            all_index_names = [index["indexName"] for index in resp_json['results']]
            for index_for_deletion_name in indices_to_delete:
                # Index has successfully been DELETED
                if index_for_deletion_name not in all_index_names:
                    print(f"Index {index_for_deletion_name} has been successfully deleted.")
                    indices_to_delete.remove(index_for_deletion_name)
                else:
                    # Check if index has finally become READY or FAILED
                    # Kick off deletion again if so
                    index = fetch_marqo_index(client, index_for_deletion_name)
                    if index.get_status()["indexStatus"] == IndexStatus.READY or \
                            index.get_status()["indexStatus"] == IndexStatus.FAILED:
                        print(f"Index {index_for_deletion_name} has {index.get_status()['indexStatus']} status, "
                              f"sending a delete request.")
                        index.delete(wait_for_readiness=False)
                    else:
                        print(f"Index {index_for_deletion_name} still has status: {index.get_status()['indexStatus']}. "
                              f"Waiting for it to be READY, FAILED, or disappear from list.")

            if attempt > max_retries:
                raise RuntimeError("Timed out waiting for indices to be deleted, still remaining: "
                                   f"{indices_to_delete}. Please delete manually")
            attempt += 1
            time.sleep(30)

        print("All test indices deleted successfully", flush=True)


if __name__ == '__main__':
    delete_all_test_indices(wait_for_readiness=True)
