import copy
import pprint
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
import unittest
from tests.marqo_test import MarqoTestCase
from unittest import mock
import requests


class TestIndex(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def tearDown(self) -> None:
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def test_create_index_settings_dict(self):
        """if settings_dict exists, it should override existing params"""
        for non_settings_dicts_param, settings_dict, expected_treat_urls_and_pointers_as_images in [
                    ({"treat_urls_and_pointers_as_images": False},
                     {"index_defaults": {"treat_urls_and_pointers_as_images": True}},
                     True),
                    ({"treat_urls_and_pointers_as_images": False},
                     None,
                     False),
                    ({"treat_urls_and_pointers_as_images": False},
                     {},
                     False),
                ]:
            mock__post = mock.MagicMock()
            @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
            def run():
                self.client.create_index(
                    index_name=self.index_name_1,
                    settings_dict=settings_dict,
                    **non_settings_dicts_param)
                return True
            assert run()
            args, kwargs = mock__post.call_args
            assert dict(kwargs['body'])["index_defaults"]["treat_urls_and_pointers_as_images"] \
                   is expected_treat_urls_and_pointers_as_images

    def test_get_documents(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "Title": "Treatise on the viability of rocket cars",
            "Blurb": "A rocket car is a car powered by a rocket engine. "
                     "This treatise proposes that rocket cars are the inevitable "
                     "future of land-based transport.",
            "_id": "article_152"
        }
        d2 = {
            "Title": "Your space suit and you",
            "Blurb": "One must maintain one's space suite. "
                     "It is, after all, the tool that will help you explore "
                     "distant galaxies.",
            "_id": "article_985"
        }
        self.client.index(self.index_name_1).add_documents([
            d1, d2
        ], tensor_fields=["Blurb", "Title"])
        res = self.client.index(self.index_name_1).get_documents(
            ["article_152", "article_490", "article_985"]
        )
        assert len(res['results']) == 3
        for doc_res in res['results']:
            if doc_res["_id"] == 'article_490':
                assert not doc_res['_found']
            else:
                assert "Blurb" in doc_res
                assert "Title" in doc_res
                assert doc_res['_found']

    def test_get_documents_expose_facets(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "Title": "Treatise on the viability of rocket cars",
            "Blurb": "A rocket car is a car powered by a rocket engine. "
                     "This treatise proposes that rocket cars are the inevitable "
                     "future of land-based transport.",
            "_id": "article_152"
        }
        d2 = {
            "Title": "Your space suit and you",
            "Blurb": "One must maintain one's space suite. "
                     "It is, after all, the tool that will help you explore "
                     "distant galaxies.",
            "_id": "article_985"
        }
        self.client.index(self.index_name_1).add_documents([
            d1, d2
        ], tensor_fields=["Blurb", "Title"])
        res = self.client.index(self.index_name_1).get_documents(
            ["article_152", "article_490", "article_985"],
            expose_facets=True
        )
        assert len(res['results']) == 3
        for doc_res in res['results']:
            if doc_res["_id"] == 'article_490':
                assert not doc_res['_found']
            else:
                assert "_tensor_facets" in doc_res
                assert '_embedding' in doc_res['_tensor_facets'][0]
                assert isinstance(doc_res['_tensor_facets'][0]['_embedding'], list)
                assert 'Blurb' in doc_res['_tensor_facets'][0] or 'Title' in doc_res['_tensor_facets'][0]
                assert "Blurb" in doc_res
                assert "Title" in doc_res
                assert doc_res['_found']

    def test_get_document_expose_facets(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "Title": "Treatise on the viability of rocket cars",
            "Blurb": "A rocket car is a car powered by a rocket engine. "
                     "This treatise proposes that rocket cars are the inevitable "
                     "future of land-based transport.",
            "_id": "article_152"
        }
        self.client.index(self.index_name_1).add_documents([
            d1
        ], tensor_fields=["Blurb", "Title"])
        doc_res = self.client.index(self.index_name_1).get_document(
            document_id="article_152",
            expose_facets=True
        )
        assert "_tensor_facets" in doc_res
        assert '_embedding' in doc_res['_tensor_facets'][0]
        assert isinstance(doc_res['_tensor_facets'][0]['_embedding'], list)
        assert 'Blurb' in doc_res['_tensor_facets'][0] or 'Title' in doc_res['_tensor_facets'][0]
        assert "Blurb" in doc_res
        assert "Title" in doc_res

    def test_create_cloud_index(self):
        mock__post = mock.MagicMock()
        test_client = copy.deepcopy(self.client)
        test_client.config.api_key = 'some-super-secret-API-key'
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            test_client.create_index(index_name=self.index_name_1)
            args, kwargs = mock__post.call_args
            # this is specific to cloud
            assert kwargs['body']['number_of_shards'] == 1
            assert kwargs['body']['number_of_replicas'] == 0
            assert kwargs['body']['index_defaults']['treat_urls_and_pointers_as_images'] is False
            return True
        assert run()

    def test_create_cloud_index_non_default_param(self):
        mock__post = mock.MagicMock()
        test_client = copy.deepcopy(self.client)
        test_client.config.api_key = 'some-super-secret-API-key'
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            # this is overridden by a create_index() default parameter
            test_client.create_index(
                index_name=self.index_name_1, model='sentence-transformers/stsb-xlm-r-multilingual')
            args, kwargs = mock__post.call_args
            assert kwargs['body']['index_defaults']['model'] == 'sentence-transformers/stsb-xlm-r-multilingual'
            assert kwargs['body']['number_of_shards'] == 1
            assert kwargs['body']['number_of_replicas'] == 0
            assert kwargs['body']['index_defaults']['treat_urls_and_pointers_as_images'] is False
            return True
        assert run()

    def test_create_cloud_index_settings_dict_precedence(self):
        """settings_dict overrides all cloud defaults"""
        mock__post = mock.MagicMock()
        test_client = copy.deepcopy(self.client)
        test_client.config.api_key = 'some-super-secret-API-key'

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            # this is overridden by a create_index() default parameter
            test_client.create_index(
                index_name=self.index_name_1, settings_dict={
                    'index_defaults': {"treat_urls_and_pointers_as_images": True}
                })
            args, kwargs = mock__post.call_args
            assert kwargs['body'] == {
                    'index_defaults': {"treat_urls_and_pointers_as_images": True}
                }
            return True
        assert run()

    def test_create_custom_number_of_replicas(self):
        intended_replicas = 0
        settings = {
            "number_of_replicas": intended_replicas
        }
        self.client.create_index(index_name=self.index_name_1, settings_dict = settings)
        index_setting = self.client.index(self.index_name_1).get_settings()
        print(index_setting)
        assert intended_replicas == index_setting['number_of_replicas']

    @mock.patch("marqo._httprequests.HttpRequests.post", return_value={"acknowledged": True})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index(self, mock_get, mock_post):
        self.client.config.url = "https://api.marqo.ai"
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.cluster_is_marqo = True

        result = self.client.create_index(
            index_name=self.index_name_1, inference_node_type="marqo.CPU", inference_node_count=1,
            storage_node_type="marqo.basic"
        )

        mock_post.assert_called_with('indexes/my-test-index-1', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': None, 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': "marqo.CPU", 'storage_class': "marqo.basic", 'inference_node_count': 1})
        mock_get.assert_called_with(path="indexes/my-test-index-1/status")
        assert result == {"acknowledged": True}

    @mock.patch("marqo._httprequests.HttpRequests.post", return_value={"error": "inference_type is required"})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index_wrong_inference_settings(self, mock_get, mock_post):
        self.client.config.url = "https://api.marqo.ai"
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.cluster_is_marqo = True

        result = self.client.create_index(
            index_name=self.index_name_1, inference_node_type=None, inference_node_count=1,
            storage_node_type="marqo.basic"
        )

        mock_post.assert_called_with('indexes/my-test-index-1', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': None, 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': None, 'storage_class': "marqo.basic", 'inference_node_count': 1})
        mock_get.assert_called_with(path="indexes/my-test-index-1/status")
        assert result == {"error": "inference_type is required"}

    @mock.patch("marqo._httprequests.HttpRequests.post", return_value={"error": "storage_class is required"})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index_wrong_storage_settings(self, mock_get, mock_post):
        self.client.config.url = "https://api.marqo.ai"
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.cluster_is_marqo = True

        result = self.client.create_index(
            index_name=self.index_name_1, inference_node_type="marqo.CPU", inference_node_count=1,
            storage_node_type=None
        )

        mock_post.assert_called_with('indexes/my-test-index-1', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': None, 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': "marqo.CPU", 'storage_class': None, 'inference_node_count': 1})
        mock_get.assert_called_with(path="indexes/my-test-index-1/status")
        assert result == {"error": "storage_class is required"}

    @mock.patch("marqo._httprequests.HttpRequests.post",
                return_value={"error": "inference_node_count must be greater than 0"})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index_wrong_inference_node_count(self, mock_get, mock_post):
        self.client.config.url = "https://api.marqo.ai"
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.cluster_is_marqo = True

        result = self.client.create_index(
            index_name=self.index_name_1, inference_node_type="marqo.CPU", inference_node_count=-1,
            storage_node_type="marqo.basic"
        )

        mock_post.assert_called_with('indexes/my-test-index-1', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': None, 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': "marqo.CPU", 'storage_class': "marqo.basic", 'inference_node_count': -1})
        mock_get.assert_called_with(path="indexes/my-test-index-1/status")
        assert result == {"error": "inference_node_count must be greater than 0"}

