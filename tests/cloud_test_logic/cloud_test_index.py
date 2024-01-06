from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests,

    Please try to keep names short to avoid hitting name-length limits

    We create 3 unsructured indexes and 3 structured indexes to test:
    1) basic_index: a multimodal index, no image preprocessing, no custom model, no text
    preprocessing. This should be used to test documents with text or image fields.

    2) image_index_with_image_preprocessing: a multimodal index with image processing enabled, this should be used to
    test document with image fields that requires image processing.

    3) custom_model_index_with_text_preprocessing: a text-only index with custom model and text preprocessing.
     This should be used to test documents with only text fields that requires text preprocessing.

    For examples,
        1) You want to test text fields without text preprocessing-> use basic_index
        2) You want to test image fields without image preprocessing-> use basic_index
        3) You want to test image fields with image preprocessing -> use image_index_with_image_preprocessing
        4) You want to test text fields with text preprocessing -> use custom_model_index_with_text_preprocessing
    """
    unstructured_basic_index = "test_index_unstr_basic"
    structured_basic_index = "test_index_str_basic"

    unstructured_image_index_with_image_preprocessing = "test_index_unstr_image"
    structured_image_index_with_image_preprocessing = "test_index_str_image"

    unstructured_text_index_with_custom_model_and_text_preprocessing = "test_index_unstr_text_custom_model"
    structured_text_index_with_custom_model_and_text_preprocessing = "test_index_str_text_custom_model"
    # no_model_index = "test-index-no-model"


index_name_to_settings_mappings = {
    CloudTestIndex.unstructured_basic_index: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",

        "inferenceType": "marqo.GPU",
        "storageClass": "marqo.performance",
        "numberOfInferences": 2,
    },
    CloudTestIndex.unstructured_image_index_with_image_preprocessing: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion400m_e32",

        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.balanced",
        "numberOfReplicas": 1,
    },
    CloudTestIndex.unstructured_text_index_with_custom_model_and_text_preprocessing: {
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
            "splitMethod": "sentence",
        },
        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.basic"
    },

    #Structured indexes
    CloudTestIndex.structured_basic_index: {
        "type": "structured",
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",
        "allFields": [{"name": "text_field_1", "type": "text"},
                      {"name": "image_field_1", "type": "image_pointer"}],
        "tensorFields": ["text_field_1", "image_field_1"],

        "inferenceType": "marqo.GPU",
        "storageClass": "marqo.performance",
        "numberOfInferences": 2,
    },
    CloudTestIndex.structured_image_index_with_image_preprocessing: {
        "type": "structured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion400m_e32",
        "allFields": [{"name": "text_field_1", "type": "text"},
                      {"name": "image_field_1", "type": "image_pointer"}],
        "tensorFields": ["text_field_1", "image_field_1"],

        "inferenceType": "marqo.CPU.large",
        "storageClass": "marqo.balanced",
        "numberOfReplicas": 1,
    },
    CloudTestIndex.structured_text_index_with_custom_model_and_text_preprocessing: {
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
            "splitMethod": "sentence",
        },
        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.basic"
    },
}