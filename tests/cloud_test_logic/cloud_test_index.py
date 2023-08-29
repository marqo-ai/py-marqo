from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests,

    Please try to keep names short to avoid hitting name-length limits"""
    basic_index = "test-index"
    image_index = "test-index-image"
    text_index_with_custom_model = "test-index-custom"
    image_index_with_preprocessing_method = "test-index-preprocess"


index_name_to_settings_mappings = {
    CloudTestIndex.basic_index: {
        "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
    },
    CloudTestIndex.image_index: {
        "index_defaults": {
             'model': "ViT-B/32",
             'treat_urls_and_pointers_as_images': True,
             "ann_parameters": {
                 "parameters": {
                     "m": 24
                 }
             },
        },
        "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
    },
    CloudTestIndex.text_index_with_custom_model: {
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
            "text_preprocessing": {
                "split_length": 2,
                "split_overlap": 1,
            },
        },
        "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
    },
    CloudTestIndex.image_index_with_preprocessing_method: {
        "index_defaults": {
            "treat_urls_and_pointers_as_images": True,
            "model": "ViT-B/16",
            "image_preprocessing": {
                "patch_method": "simple"
            },
        },
        "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic",
    }
}
