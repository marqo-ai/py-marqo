import os
import unittest
from unittest.mock import patch, MagicMock

import requests.exceptions

from marqo._httprequests import HttpRequests
from marqo.config import Config
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from marqo.errors import MarqoWebError


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


class TestConstructCloudPath(unittest.TestCase):
    """If the request is sent to the cloud (e.g.,"https://api.marqo.ai"), and the API starts with
    indexes/, we should send api/v2/indexes instead

    e.g., for search api, we send
    POST https://api.marqo.ai/api/v2/indexes/{index_name}
    """

    cloud_url = "https://api.marqo.ai"

    def construct_path_helper(self, base_path: str, path: str, use_telemetry=None) -> str:
        r = HttpRequests(
            config=Config(use_telemetry=use_telemetry, instance_mappings=MarqoCloudInstanceMappings(base_path))
        )
        return r._construct_path(path)

    def test_path_start_with_indexes(self):
        test_cases = [
            "indexes/", "indexes/my_index_name/documents", "indexes/my_index_name/search",
            "indexes/my_index_name/stats", "indexes/my_index_name/settings",
            "indexes/my_index_name/health"
        ]
        for path in test_cases:
            with self.subTest(f"self.cloud_url={self.cloud_url}, path={path}"):
                result = self.construct_path_helper(self.cloud_url, path)
                self.assertEqual(f"{self.cloud_url}/api/v2/{path}", result)


    def test_path_not_start_with_indexes(self):
        test_cases = [
            "indexe/", "test_indexes/my_index_name/documents", "bill_indexes/my_index_name/search",
            "indexs/my_index_name/stats", "test/indexes/my_index_name/settings",
            "not_indexes/my_index_name/health"
        ]
        for path in test_cases:
            with self.subTest(f"self.cloud_url={self.cloud_url}, path={path}"):
                result = self.construct_path_helper(self.cloud_url, path)
                self.assertEqual(f"{self.cloud_url}/api/{path}", result)

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