from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests,

    Please try to keep names short to avoid hitting name-length limits"""
    unstructured_basic_index = "unstructured_test_index"
    structured_basic_index = "structured_test_index"

    unstructured_image_index = "unstructured_test_index_image"
    structured_image_index = "structured_test_index_image"

    unstructured_text_index_with_custom_model = "unstructured_test_index_custom"
    structured_text_index_with_custom_model = "structured_test_index_custom"

    unstructured_image_index_with_preprocessing_method = "unstructured_test_index_preprocess"
    structured_image_index_with_preprocessing_method = "structured_test_index_preprocess"

    # text_index_with_custom_model = "test-index-custom"
    # image_index_with_preprocessing_method = "test-index-preprocess"
    # no_model_index = "test-index-no-model"


index_name_to_settings_mappings = {
        "type": "unstructured",
        "vectorNumericType": "float",
        "model": "sentence-transformers/all-MiniLM-L6-v2",
        "textPreprocessing": {
            "splitLength": 2,
            "splitOverlap": 0,
            "splitMethod": "sentence"
        },
        "imagePreprocessing": {
            "patchMethod": "simple"
        },
        "annParameters": {
            "spaceType": "prenormalized-angular",
            "parameters": {
                "efConstruction": 128,
                "m": 16
            }
        },
        "numberOfShards": 1,
        "numberOfReplicas": 0,
        "numberOfInferences": 1,
        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.basic"
    }



# index_name_to_settings_mappings = {
#     CloudTestIndex.basic_index: {
#         "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
#     },
#     CloudTestIndex.image_index: {
#         "index_defaults": {
#              'model': "ViT-B/32",
#              'treat_urls_and_pointers_as_images': True,
#              "ann_parameters": {
#                  "parameters": {
#                      "m": 24
#                  }
#              },
#         },
#         "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
#     },
#     CloudTestIndex.text_index_with_custom_model: {
#         'index_defaults': {
#             'treat_urls_and_pointers_as_images': False,
#             'model': 'test-model',
#             'model_properties': {
#                 'name': 'sentence-transformers/multi-qa-MiniLM-L6-cos-v1',
#                 'dimensions': 384,
#                 'tokens': 128,
#                 'type': 'sbert'
#             },
#             'normalize_embeddings': True,
#             "text_preprocessing": {
#                 "split_length": 2,
#                 "split_overlap": 1,
#             },
#         },
#         "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
#     },
#     CloudTestIndex.image_index_with_preprocessing_method: {
#         "index_defaults": {
#             "treat_urls_and_pointers_as_images": True,
#             "model": "ViT-B/16",
#             "image_preprocessing": {
#                 "patch_method": "simple"
#             },
#         },
#         "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
#     },
#     # `no_model` not supported on Cloud yet.
#     # CloudTestIndex.no_model_index: {
#     #     "index_defaults": {
#     #         "model": "no_model",
#     #         "model_properties": {
#     #             "dimensions": 123
#     #         }
#     #     },
#     #     "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
#     # }
# }
