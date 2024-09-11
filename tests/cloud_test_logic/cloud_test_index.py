from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests.

    Please try to keep names short to avoid hitting name-length limits

    We create 3 unstructured indexes and 3 structured indexes to test:

    1) unstructured_text: Text-only index using hf/e5-base-v2, 2 shards, 1 replica, CPU, balanced storage, for hybrid duplicates testing.
    2) unstructured_image: Image-compatible index using open_clip/ViT-B-32/laion2b_s34b_b79k, 1 shard, no replicas, CPU, basic storage.
    3) unstructured_no_model: 512-dimension custom vectors, 1 shard, no replicas, CPU, basic storage.
    4) structured_text: Structured text index with hf/e5-base-v2, lexical search, 2 shards, 1 replica, CPU, balanced storage.
    5) structured_image: Structured image-text index with open_clip/ViT-B-32, 2 shards, 1 replica, CPU, balanced storage, with image preprocessing.
    6) structured_languagebind_model: a structured index using the LanguageBind model for multi-modal support.
    For more information on the settings of each index, please refer to index_name_to_settings_mappings.

    FOR CLOUD REPLICAS AND SHARDS:
    - Use unstructured_text, structured_text, or structured_images for 1 replica & 2 shards
    - Use all other indexes for 0 replicas & 1 shard

    We design these indexes to maximize the coverage of different settings and features. For each test method,
    we will have to manually specify which index to use.

    """

    unstructured_text = "pymarqo_unstr_txt"
    unstructured_image = "pymarqo_unstr_img"
    unstructured_text_custom_prepro = "pymarqo_unstr_txt_cstm_pre"
    unstructured_no_model = "pymarqo_unstr_no_model"

    structured_image_prepro = "pymarqo_str_img_prepro"
    structured_image_custom = "pymarqo_str_img_custom"
    structured_text = "pymarqo_str_txt"
    structured_image = "pymarqo_str_img"
    structured_languagebind_model = "pymarqo_str_langbind_model"


index_name_to_settings_mappings = {
    CloudTestIndex.unstructured_text: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": False,
        "model": "hf/e5-base-v2",

        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.balanced",
        "numberOfShards": 2,
        "numberOfReplicas": 1,  # For hybrid duplicates test
    },
    CloudTestIndex.unstructured_image: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",

        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.basic",
        "numberOfShards": 1,
        "numberOfReplicas": 0,
    },
    CloudTestIndex.unstructured_no_model: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": False,

        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.basic",
        "numberOfShards": 1,
        "numberOfReplicas": 0,

        "model": "no_model",
        "modelProperties": {
            "type": "no_model",
            "dimensions": 512
        },
    },
    CloudTestIndex.structured_text: {
        "type": "structured",
        "model": "hf/e5-base-v2",
        "allFields": [
            {"name": "text_field_1", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "text_field_2", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "text_field_3", "type": "text", "features": ["lexical_search"]},
            {"name": "int_field_1", "type": "int", "features": ["score_modifier"]},
            {"name": "int_filter_field_1", "type": "int", "features": ["filter", "score_modifier"]}],
        "tensorFields": ["text_field_1", "text_field_2", "text_field_3"],
        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.balanced",
        "numberOfShards": 2,
        "numberOfReplicas": 1, # For hybrid duplicates test
    },
    CloudTestIndex.structured_image: {
        "type": "structured",
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",

        "inferenceType": "marqo.CPU.small",
        "storageClass": "marqo.balanced",
        "numberOfShards": 2,
        "numberOfReplicas": 1,  # For hybrid duplicates test

        "allFields": [
            {"name": "text_field_1", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "text_field_2", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "text_field_3", "type": "text", "features": ["filter"]},
            {"name": "image_field_1", "type": "image_pointer"},
            {"name": "array_field_1", "type": "array<text>", "features": ["filter"]},
            {"name": "float_field_1", "type": "float", "features": ["filter", "score_modifier"]},
            {"name": "int_field_1", "type": "int", "features": ["score_modifier"]},
            {"name": "int_filter_field_1", "type": "int", "features": ["filter", "score_modifier"]},
            {"name": "bool_field_1", "type": "bool", "features": ["filter"]},
        ],
        "tensorFields": ["text_field_1", "text_field_2", "text_field_3", "image_field_1"],
        "imagePreprocessing": {
            "patchMethod": "simple",
        }
    },
    CloudTestIndex.structured_languagebind_model: {
        "type": "structured",
        "model": "LanguageBind/Video_V1.5_FT_Audio_FT_Image",
        "inferenceType": "marqo.GPU",
        "storageClass": "marqo.performance",
        "allFields": [
            {"name": "text_field_1", "type": "text"},
            {"name": "text_field_2", "type": "text"},
            {"name": "text_field_3", "type": "text"},
            {"name": "video_field_1", "type": "video_pointer"},
            {"name": "video_field_2", "type": "video_pointer"},
            {"name": "video_field_3", "type": "video_pointer"},
            {"name": "audio_field_1", "type": "audio_pointer"},
            {"name": "audio_field_2", "type": "audio_pointer"},
            {"name": "image_field_1", "type": "image_pointer"},
            {"name": "image_field_2", "type": "image_pointer"},
            {"name": "multimodal_field", "type": "multimodal_combination"},
        ],
        "tensorFields": ["multimodal_field", "text_field_3", "video_field_3", "audio_field_2", "image_field_2"],
        "normalizeEmbeddings": True,
    },

}
