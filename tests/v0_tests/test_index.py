import copy
import pprint
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
import unittest
from tests.marqo_test import MarqoTestCase
from unittest import mock


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
        ])
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
        ])
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
        ])
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
            assert kwargs['body']['number_of_shards'] == 2
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
            assert kwargs['body']['number_of_shards'] == 2
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


