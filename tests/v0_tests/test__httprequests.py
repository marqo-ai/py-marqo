import unittest
from unittest.mock import patch, MagicMock

import requests.exceptions

from marqo1._httprequests import HttpRequests
from marqo1.config import Config
from marqo1.default_instance_mappings import DefaultInstanceMappings
from marqo1.errors import MarqoWebError


class TestConstructPath(unittest.TestCase):

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
