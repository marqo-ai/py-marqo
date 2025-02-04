import os
import unittest
from unittest.mock import patch, MagicMock

import pytest
import requests.exceptions
import requests_mock

from marqo._httprequests import HttpRequests
from marqo.config import Config
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.errors import MarqoWebError
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
import marqo.constants as constants


@pytest.mark.fixed
class TestConstructLocalPath(unittest.TestCase):

    def setUp(self):
        self.base_url = "http://localhost:8882"

    def construct_path_helper(self, path, use_telemetry=None):
        r = HttpRequests(
            config=Config(use_telemetry=use_telemetry, instance_mappings=DefaultInstanceMappings(self.base_url))
        )
        return r._construct_path(path)

    def test_construct_path_with_telemetry_enabled(self):
        result = self.construct_path_helper("testpath", True)
        self.assertEqual(result, f"{self.base_url}/testpath?telemetry=True")

    def test_construct_path_with_query_string_and_telemetry_enabled(self):
        result = self.construct_path_helper("testpath?param=value", True)
        self.assertEqual(result, f"{self.base_url}/testpath?param=value&telemetry=True")

    def test_construct_path_with_telemetry_disabled(self):
        result = self.construct_path_helper("testpath", False)
        self.assertEqual(result, f"{self.base_url}/testpath")

    def test_construct_path_with_no_telemetry_parameter(self):
        result = self.construct_path_helper("testpath")
        self.assertEqual(result, f"{self.base_url}/testpath")

    @patch("requests.sessions.Session.request")
    @patch("marqo._httprequests.HttpRequests._validate")
    @patch("marqo.default_instance_mappings.DefaultInstanceMappings.index_http_error_handler")
    def test_send_request_calls_index_http_error_handler(self, mock_index_http_error_handler: MagicMock, mock_validate: MagicMock,
                                                  mock_requests: MagicMock, ):
        # Set up mock behavior to raise MarqoWebError
        mock_validate.side_effect = requests.exceptions.ConnectionError()

        http_requests = HttpRequests(config=Config(instance_mappings=DefaultInstanceMappings(self.base_url)))

        with self.assertRaises(MarqoWebError):
            http_requests.get('/', index_name="test_index")

        mock_index_http_error_handler.assert_called_once()
        mock_index_http_error_handler.assert_called_with("test_index")


@pytest.mark.fixed
class TestConstructCloudPath(unittest.TestCase):
    """If the request is sent to the cloud (e.g.,"https://api.marqo.ai"), and the API starts with
    indexes/, we should send api/v2/indexes instead

    e.g., for search api, we send
    POST https://api.marqo.ai/api/v2/indexes/{index_name}
    """

    def construct_path_helper(self, base_path: str, path: str, use_telemetry=None, index_name: str = "") -> str:
        r = HttpRequests(
            config=Config(use_telemetry=use_telemetry, instance_mappings=MarqoCloudInstanceMappings(base_path))
        )
        return r._construct_path(path, index_name=index_name)

    def test_path_start_with_indexes(self):
        test_cases = [
            "indexes/", "indexes/my_index_name/documents", "indexes/my_index_name/search",
            "indexes/my_index_name/stats", "indexes/my_index_name/settings",
            "indexes/my_index_name/health"
        ]
        for path in test_cases:
            for cloud_url in constants.CLOUD_API_ENDPOINTS:
                with self.subTest(f"self.cloud_url={cloud_url}, path={path}"):
                    result = self.construct_path_helper(cloud_url, path)
                    self.assertEqual(f"{cloud_url}/api/v2/{path}", result)

    def test_path_not_start_with_indexes(self):
        test_cases = [
            "indexe/", "test_indexes/my_index_name/documents", "bill_indexes/my_index_name/search",
            "indexs/my_index_name/stats", "test/indexes/my_index_name/settings",
            "not_indexes/my_index_name/health"
        ]
        for path in test_cases:
            for cloud_url in constants.CLOUD_API_ENDPOINTS:
                with self.subTest(f"cloud_url={cloud_url}, path={path}"):
                    result = self.construct_path_helper(cloud_url, path)
                    self.assertEqual(f"{cloud_url}/api/{path}", result)

    def test_path_start_with_indexes_but_sent_to_dataplane_endpoints(self):
        """Test to ensure dataplane endpoints are not affected by the v2/ prefix for construct path"""
        test_cases = [
            "indexes/", "indexes/my_index_name/documents", "indexes/my_index_name/search",
        ]

        mock_dataplane_endpoint = "https://my-indx-abcdef-hijklm.marqo.ai"
        for path in test_cases:
            for cloud_url in constants.CLOUD_API_ENDPOINTS:
                with self.subTest(f"cloud_url={cloud_url}, path={path}"):
                    with patch("marqo.marqo_cloud_instance_mappings.MarqoCloudInstanceMappings.get_index_base_url",
                       return_value=mock_dataplane_endpoint) as mock_get_index_base_url:
                        result = self.construct_path_helper(cloud_url, path, index_name="my_index")
                        self.assertEqual(f"{mock_dataplane_endpoint}/{path}", result)
                    mock_get_index_base_url.assert_called_once_with(index_name="my_index")

    def test_environment_variable_can_affect_construct_path(self):
        """Test to ensure environment variable MARQO_CLOUD_URL enable v2/ prefix for construct path"""
        custom_cloud_url = "https://custom.cloud.url"
        with patch.dict(os.environ, {"MARQO_CLOUD_URL": custom_cloud_url}):
            test_cases = [
                "indexes/", "indexes/my_index_name/documents", "indexes/my_index_name/search",
                "indexes/my_index_name/stats", "indexes/my_index_name/settings",
                "indexes/my_index_name/health"
            ]
            for path in test_cases:
                with self.subTest(f"base_url={custom_cloud_url}, path={path}"):
                    result=self.construct_path_helper(custom_cloud_url, path)
                    self.assertEqual(f"{custom_cloud_url}/api/v2/{path}", result)

    def test_http_request_raiseProperErrorIfResponseNotInJsonFormat(self):
        with requests_mock.Mocker() as m:
            m.get('http://example.com/api/endpoint', text='Not a JSON response')
            response = requests.get('http://example.com/api/endpoint')
            with self.assertRaises(MarqoWebError) as cm:
                HttpRequests._validate(response)
            self.assertEqual(cm.exception.code, "response_not_in_json_format")

@pytest.mark.fixed
class TestHttpRequests(unittest.TestCase):
    """
    Tests basic functionality of HttpRequests class. Should work with both local and cloud endpoints.
    """

    local_urls = ["http://localhost:8882"]
    urls_to_test = local_urls + constants.CLOUD_API_ENDPOINTS
    def test_send_request_uses_correct_headers(self):
        """
        Every request should have the index name as header.
        Every request should have api key if included in config.
        Test all urls and all operations.
        """
        operations_to_test = ['get', 'post', 'put', 'patch', 'delete']
        for url in self.urls_to_test:
            with self.subTest(url=url):
                for operation in operations_to_test:
                    with self.subTest(operation=operation):
                        with requests_mock.Mocker() as m:
                            # set up mock response
                            m.request(operation, f'{url}/test', text='{"status": "ok"}')

                            for has_api_key in [True, False]:
                                with self.subTest(has_api_key=has_api_key):
                                    if has_api_key:
                                        config = Config(instance_mappings=DefaultInstanceMappings(url),
                                                        api_key="test-api-key")
                                    else:
                                        config = Config(instance_mappings=DefaultInstanceMappings(url))

                                    http_requests = HttpRequests(config=config)

                                    # Index name should always be in header.
                                    # If not provided in request, it should be ""
                                    for has_index_name in [True, False]:
                                        with self.subTest(has_index_name=has_index_name):
                                            if has_index_name:
                                                http_requests.send_request(operation,
                                                                           'test',
                                                                           index_name="my-test-index")
                                                self.assertEqual(m.last_request.headers['x-marqo-index-name'],
                                                                 "my-test-index")

                                            else:
                                                http_requests.send_request(operation, 'test')
                                                self.assertEqual(m.last_request.headers['x-marqo-index-name'],
                                                                 "")

                                    # If config has api key, it should be in request header. Otherwise, key should not
                                    # exist in header.
                                    if has_api_key:
                                        self.assertEqual(m.last_request.headers['x-api-key'], config.api_key)
                                    else:
                                        self.assertNotIn('x-api-key', m.last_request.headers)


