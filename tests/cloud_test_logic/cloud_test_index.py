from enum import Enum


class CloudTestIndex(str, Enum):
    """ Index names that will be mapped to settings of index
    and used in cloud tests.

    Please try to keep names short to avoid hitting name-length limits

    We create unstructured indexes to test:

    1) unstructured_text: Text-only index using hf/e5-base-v2, 2 shards, 1 replica, CPU, basic storage, for hybrid duplicates testing.
    2) unstructured_image: Image-compatible index using open_clip/ViT-B-32/laion2b_s34b_b79k, 1 shard, no replicas, CPU, basic storage.
    For more information on the settings of each index, please refer to index_name_to_settings_mappings.

    FOR CLOUD REPLICAS AND SHARDS:
    - Use unstructured_text for 1 replica & 2 shards
    - Use all other indexes for 0 replicas & 1 shard

    We design these indexes to maximize the coverage of different settings and features. For each test method,
    we will have to manually specify which index to use.

    """

    unstructured_text = "pymarqo_unstr_txt"
    unstructured_image = "pymarqo_unstr_img"
    unstructured_text_custom_prepro = "pymarqo_unstr_txt_cstm_pre"


index_name_to_settings_mappings = {
    CloudTestIndex.unstructured_text: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": False,
        "model": "hf/e5-base-v2",
        "storageClass": "marqo.basic",
        "numberOfShards": 2,
        "numberOfReplicas": 1,  # For hybrid duplicates test
    },
    CloudTestIndex.unstructured_image: {
        "type": "unstructured",
        "treatUrlsAndPointersAsImages": True,
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",
        "storageClass": "marqo.basic",
        "numberOfShards": 1,
        "numberOfReplicas": 0,
    },
}
