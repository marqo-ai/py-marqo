from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests.

    Please try to keep names short to avoid hitting name-length limits

    We create 3 unstructured indexes and 3 structured indexes to test:
    1) unstructured_text: a basic text-only index with default settings.
    2) unstructured_image: an image-compatible index with GPU inference pod and performance storage class.
    3) unstructured_text_custom_prepro: a text-only index with custom model and text preprocessing, with 1 replica.
    4) structured_image_prepro: a structured index with image-compatible models with image preprocessing
    5) structured_image_custom: a structured index with custom image-compatible models using 2 inference pods
    6) structured_text: a text-only index with balanced storage class and 2 shards.
    For more information on the settings of each index, please refer to index_name_to_settings_mappings.

    We design these indexes to maximize the coverage of different settings and features. For each test method,
    we will have to manually specify which index to use.

    For example,
    1) You want to test text fields without text preprocessing
        -> use 1) unstructured_text or 6) structured_text
    2) You want to test image fields without image preprocessing
        -> use 2) unstructured_image or 5) structured_image_custom
    3) You want to test text fields with text preprocessing
        -> 3) use unstructured_text_custom_prepro
    4) You want to test image fields with image preprocessing
        -> 4) use structured_image_prepro
    """

    unstructured_text = "pymarqo_unstr_txt"
    unstructured_image = "pymarqo_unstr_img"
    unstructured_text_custom_prepro = "pymarqo_unstr_txt_custom_prepro"

    structured_image_prepro = "pymarqo_str_img_prepro"
    structured_image_custom = "pymarqo_str_img_custom"
    structured_text = "pymarqo_str_txt"


index_name_to_settings_mappings = {
    # TODO Due to the resources limit of the staging cluster, we only use 2 indexes for testing purpose now
    # CloudTestIndex.unstructured_text: {
    #     "type": "unstructured",
    #     "treatUrlsAndPointersAsImages": False,
    #     "model": "hf/e5-base-v2",
    #
    #     "inferenceType": "marqo.CPU.small",
    #     "storageClass": "marqo.basic",
    # },
    CloudTestIndex.unstructured_image: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",

        "inferenceType": "marqo.GPU",
        "storageClass": "marqo.performance",
    },
    # CloudTestIndex.unstructured_text_custom_prepro: {
    #     "type": "unstructured",
    #     "treatUrlsAndPointersAsImages": False,
    #     "model": "test-model",
    #     "modelProperties": {
    #         "name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
    #         "dimensions": 384,
    #         "tokens": 128,
    #         "type": "sbert"
    #     },
    #     "normalizeEmbeddings": True,
    #     "textPreprocessing": {
    #         "splitLength": 2,
    #         "splitOverlap": 1,
    #         "splitMethod": "sentence",
    #     },
    #
    #     "storageClass": "marqo.balanced",
    #     "numberOfReplicas": 1,
    # },
    # Structured indexes
    # CloudTestIndex.structured_image_prepro: {
    #     "type": "structured",
    #     "model": "open_clip/ViT-B-16/laion2b_s34b_b88k",
    #     "allFields": [
    #         {"name": "text_field_1", "type": "text", "features": ["lexical_search", "filter"]},
    #         {"name": "text_field_2", "type": "text", "features": ["filter"]},
    #         {"name": "image_field_1", "type": "image_pointer"},
    #         {"name": "array_field_1", "type": "array<text>", "features": ["filter"]},
    #         {"name": "float_field_1", "type": "float", "features": ["filter", "score_modifier"]},
    #         {"name": "int_field_1", "type": "int", "features": ["filter", "score_modifier"]},
    #         {"name": "bool_field_1", "type": "bool", "features": ["filter"]},
    #     ],
    #     "tensorFields": ["text_field_1", "image_field_1", "text_field_2"],
    #     "imagePreprocessing": {"patchMethod": "simple"},
    #
    #     "inferenceType": "marqo.GPU",
    #     "storageClass": "marqo.balanced",
    # },
    # CloudTestIndex.structured_image_custom: {
    #     "type": "structured",
    #     "treatUrlsAndPointersAsImages": True,
    #     "model": "test-image-model",
    #     "modelProperties": {
    #         "name": "ViT-B-32-quickgelu",
    #         "dimensions": 512,
    #         "url": "https://github.com/mlfoundations/open_clip/releases/download/v0.2-weights/vit_b_32-quickgelu-laion400m_avg-8a00ab3c.pt",
    #         "type": "open_clip",
    #     },
    #     "allFields": [
    #         {"name": "text_field_1", "type": "text", "features": ["lexical_search", "filter"]},
    #         {"name": "text_field_2", "type": "text", "features": ["filter"]},
    #         {"name": "image_field_1", "type": "image_pointer"},
    #         {"name": "array_field_1", "type": "array<text>", "features": ["filter"]},
    #         {"name": "float_field_1", "type": "float", "features": ["filter", "score_modifier"]},
    #         {"name": "int_field_1", "type": "int", "features": ["filter", "score_modifier"]},
    #         {"name": "bool_field_1", "type": "bool", "features": ["filter"]},
    #     ],
    #     "tensorFields": ["text_field_1", "image_field_1", "text_field_2"],
    #
    #     "inferenceType": "marqo.CPU.large",
    #     "numberOfInferences": 2,
    # },
    CloudTestIndex.structured_text: {
        "type": "structured",
        "treatUrlsAndPointersAsImages": False,
        "model": "hf/all_datasets_v4_MiniLM-L6",
        "allFields": [
            {"name": "text_field_1", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "text_field_2", "type": "text", "features": ["filter"]},
            {"name": "text_field_3", "type": "text", "features": ["lexical_search"]},
            {"name": "array_field_1", "type": "array<text>", "features": ["filter"]},
            {"name": "float_field_1", "type": "float", "features": ["filter", "score_modifier"]},
            {"name": "int_field_1", "type": "int", "features": ["filter", "score_modifier"]},
            {"name": "bool_field_1", "type": "bool", "features": ["filter"]},
        ],
        "tensorFields": ["text_field_1", "text_field_2", "text_field_3"],

        "storageClass": "marqo.balanced",
        "numberOfShards": 2,
    },
}
