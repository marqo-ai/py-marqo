import json
import os
import time
import zlib

import marqo


# replication of create_settings_hash from marqo.marqo_test
def create_settings_hash(settings_dict, **kwargs):
    """
    Creates a hash from the settings dictionary and kwargs. Used to ensure that each index is created unique.
    Size is restricted on 10 characters to prevent having to big index name which could cause issues.

    We de-nest the index_defaults so that some kwarg indexes and setttings_dict
    indexes are more likely to generate the same index name,
    saving us from creating an unnecessary index, Example:
        settings_dict (original) = {"index_defaults": {"a": 1}, "number_of_shards": 2}
        -> settings_dict (which we will use for hashing) = {"a": 1, "number_of_shards": 2}
        # which matches the following index kwargs:
        kwargs = {"a": 1, "number_of_shards": 2}
    """
    if settings_dict:
        settings_dict = settings_dict.copy()
        index_defaults = settings_dict.get("index_defaults", {}).copy()
        if index_defaults:
            del settings_dict["index_defaults"]
            settings_dict.update(index_defaults)
    dict_to_hash = settings_dict if settings_dict else kwargs
    combined_str = json.dumps(dict_to_hash, sort_keys=True)
    crc32_hash = zlib.crc32(combined_str.encode())
    short_hash = hex(crc32_hash & 0xffffffff)[2:][
                 :10]  # Take the first 10 characters of the hexadecimal representation
    print(f"Created index with settings hash: {short_hash} for settings: {dict_to_hash}")
    return short_hash


def populate_indices():
    """ Populates indices for cloud tests. The flow is following:
    in the index_name_to_config_mappings dict we define prefix as a key and list of settings as a value.
    for each setting in a list there will be created an index with a name prefix + settings_hash.

    All of the passed settings there will be taken as kwargs for the index creation,
    so if you want to pass settings dict object,
    the dict value must be wrapped in a dict with key "settings_dict".
    """
    populate_indices_start_time = time.time()
    test_uniqueness_id = os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")
    generic_test_index_name = 'test-index'
    index_name_to_config_mappings = {
        generic_test_index_name + '-2' + '-' + test_uniqueness_id:
            [{}],
        generic_test_index_name + '-' + test_uniqueness_id:
            [{},
             {"settings_dict": {
                 "index_defaults": {
                     "ann_parameters": {
                         "parameters": {
                             "m": 24
                         }
                     }
                 }
             }
              },
             {"treat_urls_and_pointers_as_images": True,
              "model": "ViT-B/32",
              },
             {"model": "ViT-B/32"
              },
             {"settings_dict":
                 {
                     'index_defaults': {
                         'treat_urls_and_pointers_as_images': False,
                         'model': 'test-model',
                         'model_properties': {
                             'name': 'sentence-transformers/multi-qa-MiniLM-L6-cos-v1',
                             'dimensions': 384,
                             'tokens': 128,
                             'type': 'sbert'
                         },
                         'normalize_embeddings': True,
                     }
                 }
              },
             {"treat_urls_and_pointers_as_images": True,
              "model": "ViT-B/16",
              "image_preprocessing_method": None
              },
             {"treat_urls_and_pointers_as_images": True,
              "model": "ViT-B/16",
              "image_preprocessing_method": "simple"
              },
             {"sentences_per_chunk": int(1e3),
              "sentence_overlap": 0
              },
             {"sentences_per_chunk": 2,
              "sentence_overlap": 0
              },
             {"sentences_per_chunk": 2,
              "sentence_overlap": 1
              },
             {"settings_dict":
                 {
                     'index_defaults': {
                         'model': "ViT-B/16",
                         'treat_urls_and_pointers_as_images': True
                     },
                 }
              },
             ]
    }

    marqo_settings = {
        "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
    }
    api_key = os.environ.get("MARQO_API_KEY", None)
    if api_key:
        marqo_settings["api_key"] = api_key

    mq = marqo.Client(**marqo_settings)

    indexes_to_create = []

    # map real index name (with settings hash) to settings
    for index_name, settings_list in index_name_to_config_mappings.items():
        for index_settings in settings_list:
            settings_dict = index_settings.get("settings_dict")
            if index_settings:
                if not settings_dict:
                    index_name_to_create = index_name + '-' + create_settings_hash(settings_dict=None, **index_settings)
                else:
                    index_name_to_create = index_name + '-' + create_settings_hash(settings_dict=settings_dict)
            else:
                index_name_to_create = index_name
            index_settings_with_cloud_settings = index_settings.copy()
            if settings_dict:
                index_settings_with_cloud_settings["settings_dict"].update(
                    {
                        "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
                    }
                )
            else:
                index_settings_with_cloud_settings.update(
                    {
                        "inference_node_type": "marqo.CPU.large", "storage_node_type": "marqo.basic"
                    }
                )
            indexes_to_create.append((index_name_to_create, index_settings_with_cloud_settings))
    # create indexes
    for index_name, index_settings in indexes_to_create:
        print(f"Creating {index_name} with settings: {index_settings}")
        print(mq.create_index(index_name=index_name, wait_for_readiness=False, **index_settings))

    # wait for indexes to be created
    max_retries = 100
    attempt = 0
    while True:
        if all(creating_index in mq.config.instance_mapping._urls_mapping["READY"].keys()
               for creating_index, _ in indexes_to_create):
            break
        mq.config.instance_mapping._refresh_urls()
        time.sleep(10)
        print("Waiting for indexes to be created...")
        attempt += 1
        if attempt > max_retries:
            raise Exception("Timed out waiting for indexes to be created")
    print(f"Populating indices took {time.time() - populate_indices_start_time} seconds")
