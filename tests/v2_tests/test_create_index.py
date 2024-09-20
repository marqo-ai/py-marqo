import os
import uuid
from typing import Dict, List

import numpy as np
import requests
from pytest import mark

from marqo import Client
from marqo.errors import MarqoWebError
from marqo.models.marqo_cloud import CloudIndexSettings
from marqo.models.marqo_index import FieldType
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
            'audioPreprocessing': {'splitLength': 20, 'splitOverlap': 3},
            'videoPreprocessing': {'splitLength': 20, 'splitOverlap': 3},
            'vectorNumericType': 'float',
            'filterStringMaxLength': 50,
            'annParameters': {
                'spaceType': 'prenormalized-angular', 'parameters': {
                    'efConstruction': 512, 'm': 16}
            }
        }
        self.assertEqual(expected_settings, index_settings)

    def test_create_simple_index_creation_with_prefix(self):
        # Create the indexes
        self.client.create_index(
            index_name=self.override_index_name,
            model="test_prefix",
            text_query_prefix="test: ",
            text_chunk_prefix="test: ",
        )
        self.client.create_index(
            index_name=self.default_index_name,
            model="test_prefix",
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
        self.client.index(self.index_name).add_documents(documents, tensor_fields=["test", "image"])

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
                                                   "type": "sbert"}
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
                          "type": "sbert"}, index_settings['modelProperties'])

    def test_created_unstructured_image_index_with_preprocessing(self):
        self.client.create_index(index_name=self.index_name, type="unstructured",
                                 treat_urls_and_pointers_as_images=True,
                                 model="open_clip/ViT-B-16/laion400m_e31",
                                 image_preprocessing={"patchMethod": "simple"})
        image_url = "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg"
        documents = [{"test": "test",
                      "image": image_url}]
        self.client.index(self.index_name).add_documents(documents, tensor_fields=["test", "image"])

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

    def test_create_simple_structured_index(self):
        self.client.create_index(index_name=self.index_name, type="structured",
                                 model="hf/all_datasets_v4_MiniLM-L6",
                                 all_fields=[{"name": "test", "type": "text",
                                              "features": ["lexical_search"]}],
                                 tensor_fields=["test"])
        documents = [{"test": "test"}]
        self.client.index(self.index_name).add_documents(documents)

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))

        index_settings = self.client.index(self.index_name).get_settings()
        expected_index_settings = {
            'type': 'structured',
            'allFields': [{'name': 'test', 'type': 'text', 'features': ['lexical_search']}],
            'tensorFields': ['test'],
            'model': 'hf/all_datasets_v4_MiniLM-L6',
            'normalizeEmbeddings': True,
            'textPreprocessing': {'splitLength': 2, 'splitOverlap': 0, 'splitMethod': 'sentence'},
            'imagePreprocessing': {},
            'audioPreprocessing': {'splitLength': 20, 'splitOverlap': 3},
            'videoPreprocessing': {'splitLength': 20, 'splitOverlap': 3},
            'vectorNumericType': 'float',
            'annParameters': {'spaceType': 'prenormalized-angular', 'parameters': {'efConstruction': 512, 'm': 16}}}
        self.assertEqual(expected_index_settings, index_settings)

    def test_create_structured_image_index(self):
        self.client.create_index(index_name=self.index_name,
                                 type="structured",
                                 model="open_clip/ViT-B-32/laion400m_e32",
                                 all_fields=[{"name": "test", "type": "text", "features": ["lexical_search"]},
                                             {"name": "image", "type": "image_pointer"}],
                                 tensor_fields=["test", "image"])
        image_url = "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg"
        documents = [{"test": "test",
                      "image": image_url}]

        self.client.index(self.index_name).add_documents(documents)

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")
        tensor_search_res_image = self.client.index(self.index_name).search(q=image_url, search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res_image['hits']))

        index_settings = self.client.index(self.index_name).get_settings()

        self.assertEqual(["test", "image"], index_settings["tensorFields"])
        self.assertEqual("open_clip/ViT-B-32/laion400m_e32", index_settings["model"])

    def test_create_structured_index_with_custom_model(self):
        self.client.create_index(index_name=self.index_name,
                                 type="structured",
                                 model="test-model",
                                 model_properties={"name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
                                                   "dimensions": 384,
                                                   "tokens": 512,
                                                   "type": "sbert"},
                                 all_fields=[{"name": "test", "type": "text", "features": ["lexical_search"]}],
                                 tensor_fields=["test"])
        documents = [{"test": "test"}]
        self.client.index(self.index_name).add_documents(documents)

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))

        index_settings = self.client.index(self.index_name).get_settings()
        self.assertEqual("test-model", index_settings['model'])
        self.assertEqual({"name": "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
                          "dimensions": 384,
                          "tokens": 512,
                          "type": "sbert"}, index_settings['modelProperties'])

    def test_create_structured_index_with_map_fields(self):
        self.client.create_index(
            index_name=self.index_name,
            type="structured",
            model="hf/all_datasets_v4_MiniLM-L6",
            all_fields=[{"name": "test", "type": "text", "features": ["lexical_search"]},
                        {"name": "map_score_mod_float_1", "type": FieldType.MapFloat, "features": ["score_modifier"]},
                        {"name": "map_score_mod_double_1", "type": FieldType.MapDouble, "features": ["score_modifier"]},
                        {"name": "map_score_mod_int_1", "type": FieldType.MapInt, "features": ["score_modifier"]},
                        {"name": "map_score_mod_long_1", "type": FieldType.MapLong, "features": ["score_modifier"]}, ],
            tensor_fields=["test"]
        )
        documents = [{"test": "test"}]
        self.client.index(self.index_name).add_documents(documents)

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))

    def test_create_structured_image_index_with_preprocessing(self):
        self.client.create_index(index_name=self.index_name,
                                 type="structured",
                                 model="open_clip/ViT-B-16/laion400m_e31",
                                 image_preprocessing={"patchMethod": "simple"},
                                 all_fields=[{"name": "test", "type": "text", "features": ["lexical_search"]},
                                             {"name": "image", "type": "image_pointer"}],
                                 tensor_fields=["test", "image"])
        image_url = "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg"
        documents = [{"test": "test",
                      "image": image_url}]

        self.client.index(self.index_name).add_documents(documents)

        lexical_search_res = self.client.index(self.index_name).search(q="test", search_method="LEXICAL")
        tensor_search_res = self.client.index(self.index_name).search(q="test", search_method="TENSOR")
        tensor_search_res_image = self.client.index(self.index_name).search(q=image_url, search_method="TENSOR")

        self.assertEqual(1, len(lexical_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res['hits']))
        self.assertEqual(1, len(tensor_search_res_image['hits']))

        index_settings = self.client.index(self.index_name).get_settings()

        self.assertEqual(["test", "image"], index_settings["tensorFields"])
        self.assertEqual("open_clip/ViT-B-16/laion400m_e31", index_settings["model"])
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

    # TODO: Add back
    """
    def test_create_invalid_unstructured_languagebind_index(self):
        with self.assertRaises(MarqoWebError) as e:
            self.client.create_index(
                index_name=self.index_name,
                type="unstructured",
                model="LanguageBind/Video_V1.5_FT_Audio_FT_Image",
                video_preprocessing={
                    "splitLength": 10,
                    "splitOverlap": 3
                },
                treat_urls_and_pointers_as_media=True,
                treat_urls_and_pointers_as_images=False
            )

    def test_create_unstructured_index_with_languagebind(self):
        self.client.create_index(
            index_name=self.index_name,
            type="unstructured",
            model="LanguageBind/Video_V1.5_FT_Audio_FT_Image",
            treat_urls_and_pointers_as_media=True,
            treat_urls_and_pointers_as_images=True
        )

        index_settings = self.client.index(self.index_name).get_settings()

        expected_settings = {
            "type": "unstructured",
            "model": "LanguageBind/Video_V1.5_FT_Audio_FT_Image",
            "normalizeEmbeddings": True,
            "treatUrlsAndPointersAsMedia": True,
            "treatUrlsAndPointersAsImages": True,
            "vectorNumericType": "float"
        }

        for key, value in expected_settings.items():
            self.assertEqual(value, index_settings[key])

        # Test adding and searching documents
        ix = self.client.index(self.index_name)

        res = ix.add_documents(
            documents=[
                {"audio_field": "https://audio-previews.elements.envatousercontent.com/files/187680354/preview.mp3",
                 "_id": "corporate"},
                {"audio_field": "https://audio-previews.elements.envatousercontent.com/files/492763015/preview.mp3",
                 "_id": "lofi"},
            ],
            tensor_fields=["audio_field"]
        )

        doc = ix.search(
            q="corporate video background music",
            limit=5
        )

        self.assertEqual(2, len(doc['hits']))
        self.assertEqual("corporate", doc['hits'][0]['_id'])
        self.assertEqual("lofi", doc['hits'][1]['_id'])

    def test_create_structured_index_with_languagebind(self):
        self.client.create_index(
            index_name=self.index_name,
            type="structured",
            model="LanguageBind/Video_V1.5_FT_Audio_FT_Image",
            all_fields=[
                {"name": "text_field", "type": "text"},
                {"name": "video_field", "type": "video_pointer"},
                {"name": "audio_field", "type": "audio_pointer"},
                {"name": "image_field", "type": "image_pointer"}
            ],
            tensor_fields=["text_field", "video_field", "audio_field", "image_field"]
        )

        index_settings = self.client.index(self.index_name).get_settings()

        expected_settings = {
            "type": "structured",
            "model": "LanguageBind/Video_V1.5_FT_Audio_FT_Image",
            "normalizeEmbeddings": True,
            "vectorNumericType": "float",
            "tensorFields": ["text_field", "video_field", "audio_field", "image_field"],
            "allFields": [
                {"features": [], "name": "text_field", "type": "text"},
                {"features": [], "name": "video_field", "type": "video_pointer"},
                {"features": [], "name": "audio_field", "type": "audio_pointer"},
                {"features": [], "name": "image_field", "type": "image_pointer"},
            ]
        }

        for key, value in expected_settings.items():
            self.assertEqual(value, index_settings[key])

        # Test adding and searching documents
        ix = self.client.index(self.index_name)

        res = ix.add_documents(
            documents=[
                {"audio_field": "https://audio-previews.elements.envatousercontent.com/files/187680354/preview.mp3",
                 "_id": "corporate"},
                {"audio_field": "https://audio-previews.elements.envatousercontent.com/files/492763015/preview.mp3",
                 "_id": "lofi"},
            ],
        )

        doc = ix.search(
            q="corporate video background music",
            limit=5
        )

        self.assertEqual(2, len(doc['hits']))
        self.assertEqual("corporate", doc['hits'][0]['_id'])
        self.assertEqual("lofi", doc['hits'][1]['_id'])
    """

    def test_create_index_SettingsDictCanNotBeSpecificWithOtherParametersLocal(self):
        """Test that settings_dict cannot be specified with other index creation
        parameters in local create_index call."""
        parameters_pool = {
            "type": "structured",
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
            "inference_type": "marqo.CPU.large",
            "storage_class": "marqo.basic",
            "number_of_replicas": 1,
            "number_of_shards": 1,
            "number_of_inferences": 1
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
        "inferenceType": "CPU.SMALL",
        "numberOfShards": 1,
        "numberOfReplicas": 0,
        "numberOfInferences": 1,
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