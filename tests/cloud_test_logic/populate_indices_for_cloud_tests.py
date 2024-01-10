import os
import time
from marqo.errors import MarqoWebError
import marqo
from cloud_test_index import index_name_to_settings_mappings

INDEX_NAME_SEPARATOR = "_"


def populate_indices():
    populate_indices_start_time = time.time()
    test_uniqueness_id = os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")

    marqo_settings = {
        "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
    }
    api_key = os.environ.get("MARQO_API_KEY", None)
    if api_key:
        marqo_settings["api_key"] = api_key

    mq = marqo.Client(**marqo_settings)

    for index_name, index_settings_dicts in index_name_to_settings_mappings.items():
        print(f"Creating {index_name} with config: {index_settings_dicts}")
        try:
            print(mq.create_index(
                index_name=index_name + INDEX_NAME_SEPARATOR + test_uniqueness_id,
                wait_for_readiness=False,
                settings_dict=index_settings_dicts
                )
            )
        except MarqoWebError as e:
            print(f"Attempting to create index {index_name} resulting in error {e}")


    # Around 30 min:
    # TODO Temporarily bump this to 3600s (1 hour) due to the slow performance of the staging environment
    max_retries = 360
    attempt = 0
    while True:
        if all(creating_index + INDEX_NAME_SEPARATOR + test_uniqueness_id in mq.config.instance_mapping._urls_mapping["READY"].keys()
               for creating_index in index_name_to_settings_mappings.keys()):
            break
        mq.config.instance_mapping._refresh_urls()
        time.sleep(10)
        print(f"Waiting for indexes to be created. Current Mappings: "
              f"{mq.config.instance_mapping._urls_mapping}")
        attempt += 1
        if attempt > max_retries:
            raise Exception("Timed out waiting for indexes to be created")
    print(f"Populating indices took {time.time() - populate_indices_start_time} seconds")
