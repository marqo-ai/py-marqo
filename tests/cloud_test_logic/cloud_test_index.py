from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests,

    Please try to keep names short to avoid hitting name-length limits"""
    basic_index = "test-index"
    image_index = "test-index-image"
    text_index_with_custom_model = "test-index-custom"
    image_index_with_preprocessing_method = "test-index-preprocess"
    # no_model_index = "test-index-no-model"


index_name_to_settings_mappings = {
    CloudTestIndex.unstructured_basic_index: {
        "type": "unstructured",
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
    CloudTestIndex.unstructured_image_index: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion400m_e32",
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
    CloudTestIndex.unstructured_text_index_with_custom_model: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": False,
        "model": "test-model",
        "modelProperties": {
            "name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
            "dimensions": 384,
            "tokens": 128,
            "type": "sbert"
        },
        "normalizeEmbeddings": True,
        "textPreprocessing": {
            "splitLength": 2,
            "splitOverlap": 1,
        },
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
    CloudTestIndex.unstructured_image_index_with_preprocessing_method: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-16/laion400m_e31",
        "imagePreprocessing": {
            "patchMethod": "simple"
        },
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
    # Structured indexes
    CloudTestIndex.structured_basic_index: {
        "type": "structured",
        "inferenceType": "marqo.CPU.large",
        "allFields": [{"name": "text_field_1", "type": "text"}],
        "tensorFields": ["text_field_1"],
        "storageClass": "marqo.basic"
    },
    CloudTestIndex.structured_image_index: {
        "type": "structured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion400m_e32",
        "allFields": [{"name": "text_field_1", "type": "text"}],
        "tensorFields": ["text_field_1"],
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
    CloudTestIndex.structured_text_index_with_custom_model: {
        "type": "structured",
        "treatUrlsAndPointersAsImages": False,
        "model": "test-model",
        "allFields": [{"name": "text_field_1", "type": "text"}],
        "tensorFields": ["text_field_1"],
        "modelProperties": {
            "name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
            "dimensions": 384,
            "tokens": 128,
            "type": "sbert"
        },
        "normalizeEmbeddings": True,
        "textPreprocessing": {
            "splitLength": 2,
            "splitOverlap": 1,
        },
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
    CloudTestIndex.structured_image_index_with_preprocessing_method: {
        "type": "structured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-16/laion400m_e31",
        "allFields": [{"name": "text_field_1", "type": "text"}],
        "tensorFields": ["text_field_1"],
        "imagePreprocessing": {
            "patchMethod": "simple"
        },
        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.basic"
    },
}