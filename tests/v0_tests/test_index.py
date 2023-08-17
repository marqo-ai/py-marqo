import copy

from pytest import mark

from marqo.errors import BackendCommunicationError, BackendTimeoutError, \
    UnsupportedOperationError

from marqo.index import marqo_url_and_version_cache
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from unittest import mock
import requests
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings


class TestIndex(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        marqo_url_and_version_cache.clear()

    def tearDown(self) -> None:
        marqo_url_and_version_cache.clear()

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
            mock_get = mock.MagicMock()
            mock_get.return_value = {"index_status": "READY"}

            @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
            @mock.patch("marqo._httprequests.HttpRequests.get", mock_get)
            def run():
                test_index_name = self.client.create_index(
                    index_name=self.generic_test_index_name,
                    settings_dict=settings_dict,
                    **non_settings_dicts_param)
                return True
            assert run()
            args, kwargs = mock__post.call_args
            assert dict(kwargs['body'])["index_defaults"]["treat_urls_and_pointers_as_images"] \
                   is expected_treat_urls_and_pointers_as_images

    def test_get_documents(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
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
        self.client.index(test_index_name).add_documents([
            d1, d2
        ], tensor_fields=["Blurb", "Title"])
        res = self.client.index(test_index_name).get_documents(
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
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
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
        self.client.index(test_index_name).add_documents([
            d1, d2
        ], tensor_fields=["Blurb", "Title"])
        res = self.client.index(test_index_name).get_documents(
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
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        d1 = {
            "Title": "Treatise on the viability of rocket cars",
            "Blurb": "A rocket car is a car powered by a rocket engine. "
                     "This treatise proposes that rocket cars are the inevitable "
                     "future of land-based transport.",
            "_id": "article_152"
        }
        self.client.index(test_index_name).add_documents([
            d1
        ], tensor_fields=["Blurb", "Title"])
        doc_res = self.client.index(test_index_name).get_document(
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
        mock_get = mock.MagicMock()
        mock_get.return_value = {"index_status": "READY"}
        test_client = copy.deepcopy(self.client)
        test_client.config.api_key = 'some-super-secret-API-key'
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        @mock.patch("marqo._httprequests.HttpRequests.get", mock_get)
        def run():
            test_client.create_index(index_name=self.generic_test_index_name)
            args, kwargs = mock__post.call_args
            # this is specific to cloud
            assert kwargs['body']['number_of_shards'] == 1
            assert kwargs['body']['number_of_replicas'] == 0
            assert kwargs['body']['index_defaults']['treat_urls_and_pointers_as_images'] is False
            return True
        assert run()

    def test_create_cloud_index_non_default_param(self):
        mock__post = mock.MagicMock()
        mock_get = mock.MagicMock()
        mock_get.return_value = {"index_status": "READY"}
        test_client = copy.deepcopy(self.client)
        test_client.config.api_key = 'some-super-secret-API-key'
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        @mock.patch("marqo._httprequests.HttpRequests.get", mock_get)
        def run():
            # this is overridden by a create_index() default parameter
            test_client.create_index(
                index_name=self.generic_test_index_name, model='sentence-transformers/stsb-xlm-r-multilingual')
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
        mock_get = mock.MagicMock()
        mock_get.return_value = {"index_status": "READY"}
        test_client = copy.deepcopy(self.client)
        test_client.config.api_key = 'some-super-secret-API-key'
        test_client.config.is_marqo_cloud = True

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        @mock.patch("marqo._httprequests.HttpRequests.get", mock_get)
        def run():
            # this is overridden by a create_index() default parameter
            test_client.create_index(
                index_name=self.generic_test_index_name, settings_dict={
                    'index_defaults': {"treat_urls_and_pointers_as_images": True}
                })
            args, kwargs = mock__post.call_args
            assert kwargs['body'] == {
                    'index_defaults': {"treat_urls_and_pointers_as_images": True}
                }
            return True
        assert run()

    @mark.ignore_during_cloud_tests
    def test_create_custom_number_of_replicas(self):
        intended_replicas = 1
        settings = {
            "number_of_replicas": intended_replicas,
        }
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_settings=settings
        )
        index_setting = self.client.index(test_index_name).get_settings()
        print(index_setting)
        assert intended_replicas == index_setting['number_of_replicas']

    @mock.patch("marqo._httprequests.HttpRequests.post", return_value={"acknowledged": True})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index(self, mock_get, mock_post):
        self.client.config.instance_mapping = MarqoCloudInstanceMappings("https://api.marqo.ai")
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.is_marqo_cloud = True

        result = self.client.create_index(
            index_name=self.generic_test_index_name, inference_node_type="marqo.CPU.large", inference_node_count=1,
            storage_node_type="marqo.basic"
        )

        mock_post.assert_called_with('indexes/test-index', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': 'hf/all_datasets_v4_MiniLM-L6', 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': "marqo.CPU.large", 'storage_class': "marqo.basic", 'number_of_inferences': 1})
        mock_get.assert_called_with("indexes/test-index/status")
        assert result == {"acknowledged": True}

    @mock.patch("marqo._httprequests.HttpRequests.post", return_value={"error": "inference_type is required"})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index_wrong_inference_settings(self, mock_get, mock_post):
        self.client.config.instance_mapping = MarqoCloudInstanceMappings("https://api.marqo.ai")
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.is_marqo_cloud = True

        result = self.client.create_index(
            index_name=self.generic_test_index_name, inference_node_type=None, inference_node_count=1,
            storage_node_type="marqo.basic"
        )

        mock_post.assert_called_with('indexes/test-index', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': 'hf/all_datasets_v4_MiniLM-L6', 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': None, 'storage_class': "marqo.basic", 'number_of_inferences': 1})
        mock_get.assert_called_with("indexes/test-index/status")
        assert result == {"error": "inference_type is required"}

    @mock.patch("marqo._httprequests.HttpRequests.post", return_value={"error": "storage_class is required"})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index_wrong_storage_settings(self, mock_get, mock_post):
        self.client.config.instance_mapping = MarqoCloudInstanceMappings("https://api.marqo.ai")
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.is_marqo_cloud = True

        result = self.client.create_index(
            index_name=self.generic_test_index_name, inference_node_type="marqo.CPU.large", inference_node_count=1,
            storage_node_type=None
        )

        mock_post.assert_called_with('indexes/test-index', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': 'hf/all_datasets_v4_MiniLM-L6', 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': "marqo.CPU.large", 'storage_class': None, 'number_of_inferences': 1})
        mock_get.assert_called_with("indexes/test-index/status")
        assert result == {"error": "storage_class is required"}

    @mock.patch("marqo._httprequests.HttpRequests.post",
                return_value={"error": "inference_node_count must be greater than 0"})
    @mock.patch("marqo._httprequests.HttpRequests.get", return_value={"index_status": "READY"})
    def test_create_marqo_cloud_index_wrong_inference_node_count(self, mock_get, mock_post):
        self.client.config.instance_mapping = MarqoCloudInstanceMappings("https://api.marqo.ai")
        self.client.config.api_key = 'some-super-secret-API-key'
        self.client.config.is_marqo_cloud = True

        result = self.client.create_index(
            index_name=self.generic_test_index_name, inference_node_type="marqo.CPU.large", inference_node_count=-1,
            storage_node_type="marqo.basic"
        )

        mock_post.assert_called_with('indexes/test-index', body={
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False, 'model': 'hf/all_datasets_v4_MiniLM-L6', 'normalize_embeddings': True,
                'text_preprocessing': {'split_length': 2, 'split_overlap': 0, 'split_method': 'sentence'},
                'image_preprocessing': {'patch_method': None}
            },
            'number_of_shards': 1, 'number_of_replicas': 0,
            'inference_type': "marqo.CPU.large", 'storage_class': "marqo.basic", 'number_of_inferences': -1})
        mock_get.assert_called_with("indexes/test-index/status")
        assert result == {"error": "inference_node_count must be greater than 0"}

    @mock.patch("marqo.index.mq_logger.warning")
    def test_version_check_multiple_instantiation(self, mock_warning):
        """Ensure that duplicated instantiation of the client does not result in multiple APIs calls of get_marqo()

        Also ensure we only log a version check warning once.
        """
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        marqo_url_and_version_cache.clear()
        with mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                mock.patch("marqo.index.Index.get_status") as mock_get_status:
            mock_get_status.return_value = {'index_status': 'READY'}
            mock_get_marqo.return_value = {'version': '0.0.0'}
            index = self.client.index(test_index_name)

            mock_get_marqo.assert_called_once()
            mock_warning.assert_called_once()
            mock_warning.reset_mock()
            mock_get_marqo.reset_mock()

        for _ in range(10):
            with mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo:
                index = self.client.index(test_index_name)

                mock_get_marqo.assert_not_called()
                mock_warning.assert_not_called()

    def test_warning_not_printed_for_ready_index(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test only applicable for Marqo Cloud")
        with mock.patch("marqo.index.mq_logger.warning") as mock_warning:
            self.client.index(self.create_test_index(
                cloud_test_index_to_use=CloudTestIndex.basic_index,
                open_source_test_index_name=self.generic_test_index_name,
            ))
            mock_warning.assert_not_called()

    def test_warning_not_printed_for_not_ready_index(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test only applicable for Marqo Cloud")
        with mock.patch("marqo.index.mq_logger.warning") as mock_warning:
            self.client.index("not-ready-index")
            mock_warning.assert_not_called()

    def test_skipped_version_check_multiple_instantiation(self):
        """Ensure that the url labelled as `_skipped` only call get_marqo() once"""
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        marqo_url_and_version_cache.clear()
        with mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                mock.patch("marqo.index.Index.get_status") as mock_get_status:
            mock_get_status.return_value = {'index_status': 'READY'}
            mock_get_marqo.side_effect = requests.exceptions.RequestException("test")

            index = self.client.index(test_index_name)

            mock_get_marqo.assert_called_once()
            mock_get_marqo.reset_mock()
            assert ('_skipped' ==
                    marqo_url_and_version_cache[index.config.instance_mapping.get_index_base_url(test_index_name)])

        for _ in range(10):
            with mock.patch("marqo.index.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo:
                index = self.client.index(self.generic_test_index_name)

                mock_get_marqo.assert_not_called()
                mock_warning.assert_not_called()

    def test_error_handling_in_version_check(self):
        side_effect_list = [requests.exceptions.JSONDecodeError("test", "test", 1), BackendCommunicationError("test"),
                            BackendTimeoutError("test"), requests.exceptions.RequestException("test"),
                            KeyError("test"), KeyError("test"), requests.exceptions.Timeout("test")]
        # we must use a real index name that can appear in urls_mappings, otherwise the version
        # check won't be attempted
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        for i, side_effect in enumerate(side_effect_list):
            with mock.patch("marqo.index.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                    mock.patch("marqo.index.Index.get_status") as mock_get_status: #, \
                mock_get_marqo.side_effect = side_effect
                mock_get_status.return_value = {'index_status': 'READY'}
                marqo_url_and_version_cache.clear()

                index = self.client.index(test_index_name)
                mock_get_marqo.assert_called_once()

                # Check the warning was logged
                mock_warning.assert_called_once()

                # Get the warning message
                warning_message = mock_warning.call_args[0][0]

                # Assert the message is what you expect
                self.assertIn("Marqo encountered a problem trying to check the Marqo version found", warning_message)
                self.assertEqual(marqo_url_and_version_cache, dict(
                    {index.config.instance_mapping.get_index_base_url(index_name=test_index_name): "_skipped"}))

                marqo_url_and_version_cache.clear()

    def test_error_handling_in_version_check_already_instantiated(self):
        # once cached, there should be no warning or request to get version
        side_effect_list = [requests.exceptions.JSONDecodeError("test", "test", 1), BackendCommunicationError("test"),
                            BackendTimeoutError("test"), requests.exceptions.RequestException("test"),
                            KeyError("test"), KeyError("test"), requests.exceptions.Timeout("test")]
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        index = self.client.index(test_index_name)

        for i, side_effect in enumerate(side_effect_list):
            with mock.patch("marqo.index.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                    mock.patch("marqo.index.Index.get_status") as mock_get_status:
                mock_get_marqo.side_effect = side_effect
                mock_get_status.return_value = {'index_status': 'READY'}

                index = self.client.index(test_index_name)
                mock_get_marqo.assert_not_called()

                # Check the warning was logged
                mock_warning.assert_not_called()

    def test_version_check_instantiation(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        marqo_url_and_version_cache.clear()
        with mock.patch("marqo.index.mq_logger.warning") as mock_warning, \
                mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                mock.patch("marqo.index.Index.get_status") as mock_get_status:
            mock_get_marqo.return_value = {'version': '0.0.0'}
            mock_get_status.return_value = {'index_status': 'READY'}

            index = self.client.index(test_index_name)

            mock_get_marqo.assert_called_once()

            # Check the warning was logged
            mock_warning.assert_called_once()

            # Get the warning message
            warning_message = mock_warning.call_args[0][0]

            # Assert the message is what you expect
            self.assertIn("Please upgrade your Marqo instance to avoid potential errors.", warning_message)

            # Assert the url is in the cache
            self.assertIn(
                self.client.config.instance_mapping.get_index_base_url(test_index_name),
                marqo_url_and_version_cache
            )
            assert ('0.0.0' ==
                    marqo_url_and_version_cache[
                        self.client.config.instance_mapping.get_index_base_url(test_index_name)]
                    )

    def test_skip_version_check_for_previously_labelled_url(self):
        with mock.patch.dict("marqo.index.marqo_url_and_version_cache",
                             {self.client_settings["url"]: "_skipped"}) as mock_cache, \
                mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo:
            index = self.client.index(self.generic_test_index_name)

            mock_get_marqo.assert_not_called()

    def test_get_health(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        res = self.client.index(test_index_name).health()
        assert 'status' in res
        assert 'status' in res['backend']

    @mark.ignore_during_cloud_tests
    def test_get_status_raises_error_on_local_index(self):
        index = self.client.index(self.generic_test_index_name)
        with self.assertRaises(UnsupportedOperationError):
            index.get_status()

    def test_version_check_skip_if_marked_as_skipped(self):
        index = self.client.index(self.generic_test_index_name)
        with mock.patch.dict("marqo.index.marqo_url_and_version_cache",
                             {self.client_settings["url"]: "_skipped"}) as mock_cache, \
                mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                mock.patch("marqo.index.mq_logger.warning") as mock_warning:

            index._marqo_minimum_supported_version_check()
            mock_warning.assert_not_called()
            mock_get_marqo.assert_not_called()

    def test_version_check_handle_garbage_value(self):
        index = self.client.index(self.generic_test_index_name)
        with mock.patch.dict("marqo.index.marqo_url_and_version_cache",
                             {self.client_settings["url"]: "garbage value"}) as mock_cache, \
                mock.patch("marqo.index.Index.get_marqo") as mock_get_marqo, \
                mock.patch("marqo.index.mq_logger.warning") as mock_warning:

            index._marqo_minimum_supported_version_check()
            mock_warning.assert_not_called()
            mock_get_marqo.assert_not_called()


