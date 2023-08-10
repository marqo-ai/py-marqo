import json
import os
import time
import zlib

import marqo


def create_settings_hash(settings_dict, **kwargs):
    """
    Creates a hash from the settings dictionary and kwargs. Used to ensure that each index is created unique.
    Size is restricted on 10 characters to prevent having to big index name which could cause issues.
    """
    dict_to_hash = settings_dict["index_defaults"] if settings_dict else kwargs
    combined_str = json.dumps(dict_to_hash, sort_keys=True)
    crc32_hash = zlib.crc32(combined_str.encode())
    short_hash = hex(crc32_hash & 0xffffffff)[2:][
                 :10]  # Take the first 10 characters of the hexadecimal representation
    print(f"Created index with settings hash: {short_hash} for settings: {dict_to_hash}")
    return short_hash


def populate_indices():
    test_uniqueness_id = os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")
    generic_test_index_name = 'test-index'
    index_name_to_config_mappings = {
        generic_test_index_name + '-2' + '-' + test_uniqueness_id:
            [{}],
        "cool-index" + '-' + test_uniqueness_id:
            [{}],
        "first-index" + '-' + test_uniqueness_id:
            [{}],
        "weight-index" + '-' + test_uniqueness_id:
            [{}],
        "mmodal-index" + '-' + test_uniqueness_id:
            [{"treat_urls_and_pointers_as_images": True, "model": "ViT-B/32"}],
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
             {"treat_urls_and_pointers_as_images": True,  # allows us to find an image file and index it
              "model": "ViT-B/16",
              "image_preprocessing_method": None
              },
             {"treat_urls_and_pointers_as_images": True,  # allows us to find an image file and index it
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

    for index_name, configs_list in index_name_to_config_mappings.items():
        for config in configs_list:
            settings_dict = config.get("settings_dict")
            if config:
                if not settings_dict:
                    index_name_to_create = index_name + '-' + create_settings_hash(settings_dict=None, **config)
                else:
                    index_name_to_create = index_name + '-' + create_settings_hash(settings_dict=settings_dict)
            else:
                index_name_to_create = index_name
            config_with_cloud_settings = config.copy()
            if settings_dict:
                config_with_cloud_settings["settings_dict"].update(
                    {
                        "inference_type": "marqo.CPU", "storage_class": "marqo.basic",
                    }
                )
            else:
                config_with_cloud_settings.update(
                    {
                        "inference_node_type": "marqo.CPU", "storage_node_type": "marqo.basic"
                    }
                )
            indexes_to_create.append((index_name_to_create, config_with_cloud_settings))
    for index_name, config in indexes_to_create:
        print(f"Creating {index_name} with config: {config}")
        print(mq.create_index(index_name=index_name, wait_for_readiness=False, **config))

    while True:
        if all(creating_index in mq.config.instance_mapping._urls_mapping["READY"] .keys()
               for creating_index, _ in indexes_to_create):
            break
        mq.config.instance_mapping._refresh_urls()
        time.sleep(10)
        print("Waiting for indexes to be created...")
