import os
import uuid
from typing import Dict, List

import numpy as np
import requests
from pytest import mark

from marqo import Client
from marqo.errors import MarqoWebError
from marqo.models.marqo_cloud import CloudIndexSettings
from tests.cloud_test_logic.cloud_test_index import index_name_to_settings_mappings
from tests.marqo_test import MarqoTestCase


@mark.fixed
@mark.local_only_tests
class TestCreateIndex(MarqoTestCase):
    index_name = "test_create_index" + str(uuid.uuid4()).replace('-', '')
    override_index_name = "override_prefix" + str(uuid.uuid4()).replace('-', '')
    default_index_name = "default_prefix" + str(uuid.uuid4()).replace('-', '')

    def tearDown(self):
        super().tearDown()
        try:
            self.client.delete_index(index_name=self.index_name)
            self.client.delete_index(index_name=self.override_index_name)
            self.client.delete_index(index_name=self.default_index_name)
        except MarqoWebError:
            pass

    def test_simple_index_creation(self):
        self.client.create_index(index_name=self.index_name)
        self.client.index(self.index_name).add_documents([{"test": "test"}], tensor_fields=["test"])

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))
        index_settings = self.client.index(self.index_name).get_settings()

        expected_settings = {
            'type': 'unstructured',
            'treatUrlsAndPointersAsImages': False,
            'treatUrlsAndPointersAsMedia': False,
            'model': 'hf/e5-base-v2',
            'normalizeEmbeddings': True,
            'textPreprocessing': {'splitLength': 2, 'splitOverlap': 0, 'splitMethod': 'sentence'},
            'imagePreprocessing': {},
            'audioPreprocessing': {'splitLength': 10, 'splitOverlap': 3},
            'videoPreprocessing': {'splitLength': 20, 'splitOverlap': 3},
            'vectorNumericType': 'float',
            'filterStringMaxLength': 50,
            'annParameters': {
                'spaceType': 'prenormalized-angular', 'parameters': {
                    'efConstruction': 512, 'm': 16}
            }
        }
        for key, value in expected_settings.items():
            self.assertEqual(value, index_settings[key])

    def test_create_simple_index_creation_with_prefix(self):
        # Create the indexes
        self.client.create_index(
            index_name=self.override_index_name,
            model="test_prefix",
            model_properties={
                "name": "sentence-transformers/all-MiniLM-L6-v2",
                "dimensions": 384,
                "tokens": 256,
                "type": "hf",
                "text_query_prefix": "test query: ",
                "text_chunk_prefix": "test passage: ",
                "notes": ""
            },
            text_query_prefix="test: ",
            text_chunk_prefix="test: ",
        )
        self.client.create_index(
            index_name=self.default_index_name,
            model="test_prefix",
            model_properties={
                "name": "sentence-transformers/all-MiniLM-L6-v2",
                "dimensions": 384,
                "tokens": 256,
                "type": "hf",
                "text_query_prefix": "test query: ",
                "text_chunk_prefix": "test passage: ",
                "notes": ""
            }
        )

        d1 = {
            "_id": "doc1",
            "text_field_1": "hello document"
        }
        # Add documents to both
        self.client.index(self.override_index_name).add_documents([d1], tensor_fields=["text_field_1"])
        self.client.index(self.default_index_name).add_documents([d1], tensor_fields=["text_field_1"])

        # Get override doc with tensor facets (for reference vector)
        retrieved_override_doc = self.client.index(self.override_index_name).get_document(
            document_id="doc1", expose_facets=True)

        # Get default doc with tensor facets (for reference vector)
        retrieved_default_doc = self.client.index(self.default_index_name).get_document(
            document_id="doc1", expose_facets=True)

        # Embed override
        embed_res_override = self.client.index(self.override_index_name).embed("test: hello document",
                                                                               content_type=None)

        # Embed default
        embed_res_default = self.client.index(self.default_index_name).embed("test passage: hello document",
                                                                             content_type=None)

        # Assert that the embeddings from override add docs and the embeddings from the embed call are the same
        self.assertTrue(
            np.allclose(embed_res_override["embeddings"][0], retrieved_override_doc["_tensor_facets"][0]["_embedding"]))

        # Assert that the embeddings from override add docs and the embeddings from the embed call are the same
        self.assertTrue(
            np.allclose(embed_res_default["embeddings"][0], retrieved_default_doc["_tensor_facets"][0]["_embedding"]))

    def test_create_unstructured_image_index(self):
        self.client.create_index(index_name=self.index_name, type="unstructured",
                                 treat_urls_and_pointers_as_images=True, model="open_clip/ViT-B-32/laion400m_e32")
        image_url = "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg"
        documents = [{"test": "test",
                      "image": image_url}]
        res = self.client.index(self.index_name).add_documents(documents, tensor_fields=["test", "image"])
        self.assertFalse(res["errors"], f"add_documents failed: {res}")

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")
        tensor_search_res_image = self.client.index(self.index_name).search(q=image_url, search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res_image['hits']))

        index_settings = self.client.index(self.index_name).get_settings()
        self.assertEqual(True, index_settings['treatUrlsAndPointersAsImages'])
        self.assertEqual("open_clip/ViT-B-32/laion400m_e32", index_settings['model'])

    def test_create_unstructured_text_index_custom_model(self):
        self.client.create_index(index_name=self.index_name, type="unstructured",
                                 treat_urls_and_pointers_as_images=False,
                                 model="test-model",
                                 model_properties={"name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
                                                   "dimensions": 384,
                                                   "tokens": 512,
                                                   "type": "hf"}
                                 )
        documents = [{"test": "test"}]
        self.client.index(self.index_name).add_documents(documents, tensor_fields=["test"])

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))

        index_settings = self.client.index(self.index_name).get_settings()
        self.assertEqual(False, index_settings['treatUrlsAndPointersAsImages'])
        self.assertEqual("test-model", index_settings['model'])
        self.assertEqual({"name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
                          "dimensions": 384,
                          "tokens": 512,
                          "type": "hf"}, index_settings['modelProperties'])

    def test_created_unstructured_image_index_with_preprocessing(self):
        self.client.create_index(index_name=self.index_name, type="unstructured",
                                 treat_urls_and_pointers_as_images=True,
                                 model="open_clip/ViT-B-16/laion400m_e31",
                                 image_preprocessing={"patchMethod": "simple"})
        image_url = "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg"
        documents = [{"test": "test",
                      "image": image_url}]
        res = self.client.index(self.index_name).add_documents(documents, tensor_fields=["test", "image"])
        self.assertFalse(res["errors"], f"add_documents failed: {res}")

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")
        tensor_search_res_image = self.client.index(self.index_name).search(q=image_url, search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res_image['hits']))

        index_settings = self.client.index(self.index_name).get_settings()
        self.assertEqual(True, index_settings['treatUrlsAndPointersAsImages'])
        self.assertEqual("open_clip/ViT-B-16/laion400m_e31", index_settings['model'])
        self.assertEqual("simple", index_settings['imagePreprocessing']['patchMethod'])

    def test_dash_and_underscore_in_index_name(self):
        """Test that we can create indexes with dash and underscore in the index name and
        these two indexes are different indexes."""
        self.client.create_index(index_name="test-dash-and-under-score", type="unstructured")
        self.client.create_index(index_name="test_dash_and_under_score", type="unstructured")

        self.client.index("test-dash-and-under-score").add_documents([{"test": "test"}], tensor_fields=["test"])
        self.client.index("test_dash_and_under_score").add_documents([{"test": "test"}], tensor_fields=["test"])

        res = self.client.index("test-dash-and-under-score").search(q="test", search_method="TENSOR")
        self.assertEqual(1, len(res['hits']))

        res = self.client.index("test_dash_and_under_score").search(q="test", search_method="TENSOR")
        self.assertEqual(1, len(res['hits']))
        self.client.delete_index("test-dash-and-under-score")
        self.client.delete_index("test_dash_and_under_score")

    def test_create_index_SettingsDictCanNotBeSpecificWithOtherParametersLocal(self):
        """Test that settings_dict cannot be specified with other index creation
        parameters in local create_index call."""
        parameters_pool = {
            "type": "unstructured",
            "all_fields": [{"name": "test", "type": "text", "features": ["lexical_search"]}],
            "tensor_fields": ["test"],
            "treat_urls_and_pointers_as_images": True,
            "treat_urls_and_pointers_as_media": True,
            "filter_string_max_length": 50,
            "model": "hf/e5-base-v2",
            "model_properties": {
                "name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
                "dimensions": 384,
                "tokens": 512,
                "type": "sbert"
            },
            "normalize_embeddings": True,
            "text_preprocessing": "splitMethod",
            "image_preprocessing": "patchMethod",
            "vector_numeric_type": "float",
            "ann_parameters": {"spaceType": "prenormalized-angular", "parameters": {"efConstruction": 512, "m": 16}},
            "text_query_prefix": "test",
            "text_chunk_prefix": "test",
        }

        for key in parameters_pool.keys():
            with self.subTest(f"key={key}"):
                with self.assertRaises(ValueError) as e:
                    self.client.create_index(
                        index_name=self.index_name,
                        **{key: parameters_pool[key]},
                        settings_dict={"type": "unstructured"}
                    )
                self.assertIn(
                    f"'settings_dict' cannot be specified with other index creation parameters.",
                    str(e.exception)
                )

    def test_create_index_SettingsDictCanNotBeSpecificWithOtherParametersCloud(self):
        """Test that settings_dict cannot be specified with other index creation
        parameters in cloud create_index call."""
        parameters_pool = {
            "storage_class": "marqo.basic",
            "number_of_replicas": 1,
            "number_of_shards": 1,
        }

        test_cloud_client = Client("https://api.marqo.ai", api_key="test")
        for key in parameters_pool.keys():
            with self.subTest(f"key={key}"):
                with self.assertRaises(ValueError) as e:
                    test_cloud_client.create_index(
                        index_name=self.index_name,
                        **{key: parameters_pool[key]},
                        settings_dict={"type": "unstructured"}
                    )
                self.assertIn(
                    f"'settings_dict' cannot be specified with other index creation parameters.",
                    str(e.exception)
                )

    def test_CloudIndexSettingsGenerateRequestBody(self):
        """Test that CloudIndexSettings generate_request_body method returns the correct request body."""
        expected_request_body = {
            "type": "unstructured",
        }
        cloud_index_settings = CloudIndexSettings(
            type="unstructured"
        )
        self.assertEqual(expected_request_body, cloud_index_settings.generate_request_body())

@mark.fixed
@mark.cloud_only_tests
class TestCloudCreateIndex(MarqoTestCase):

    TEST_ATTRIBUTES_WITH_DEFAULTS = {
        "numberOfShards": 1,
        "numberOfReplicas": 0,
        "storageClass": "BASIC"
    }

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.get_index_response: List[Dict] = requests.get(
            f"{cls.authorized_url}/api/v2/indexes",
            headers={"x-api-key": os.environ.get("MARQO_API_KEY", None)}
        ).json()["results"]

    def test_cloud_index_attributes(self):
        test_indexes = list(index_name_to_settings_mappings.keys())
        for test_index in test_indexes:
            index_name = test_index + "_" + os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")
            with self.subTest(f"Index name: {index_name}"):
                index_meta_data = [d for d in self.get_index_response if d.get("indexName") == index_name][0]
                for test_attribute in list(self.TEST_ATTRIBUTES_WITH_DEFAULTS.keys()):
                    expected_value = index_name_to_settings_mappings[test_index].get(
                        test_attribute,
                        self.TEST_ATTRIBUTES_WITH_DEFAULTS[test_attribute]
                    )
                    if isinstance(expected_value, int):
                        self.assertEqual(int(index_meta_data[test_attribute]), expected_value)
                    elif isinstance(expected_value, str):
                        self.assertIn(index_meta_data[test_attribute].upper(), expected_value.upper())
                    else:
                        raise ValueError(f"Unexpected type for {test_attribute}: {type(expected_value)}")
