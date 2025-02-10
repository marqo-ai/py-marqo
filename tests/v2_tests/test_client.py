import os

import requests
from unittest import mock
from unittest.mock import patch
from pytest import mark

from marqo.client import Client
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo.errors import BadRequestError
from marqo.errors import BackendTimeoutError, BackendCommunicationError
import warnings
import marqo.constants as constants
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from marqo.default_instance_mappings import DefaultInstanceMappings


@mark.fixed
class TestClient(MarqoTestCase):
    def test_check_index_health_response(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            res = self.client.index(test_index_name).health()
            assert 'status' in res
            assert 'status' in res['backend']

    def test_check_index_health_query(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with patch("marqo._httprequests.HttpRequests.get") as mock_get:
                res = self.client.index(test_index_name).health()
                args, kwargs = mock_get.call_args
                self.assertIn(f"health", kwargs["path"])

    def test_overwrite_cloud_url_and_client_is_set_to_marqo(self):
        current = os.environ.get("MARQO_CLOUD_URL", constants.CLOUD_AWS_API_ENDPOINT)
        os.environ["MARQO_CLOUD_URL"] = "https://cloud.url.com"
        client = Client(url="https://cloud.url.com", api_key="test")
        self.assertTrue(client.config.is_marqo_cloud)
        os.environ["MARQO_CLOUD_URL"] = current

    def test_default_cloud_endpoint_client_is_marqo_cloud(self):
        """
        Checks that if a client is created with one of the 2 cloud URLs (AWS or GCP),
        the client is set to Marqo Cloud.
        """
        for cloud_url in constants.CLOUD_API_ENDPOINTS:
            client = Client(url=cloud_url)
            self.assertTrue(client.config.is_marqo_cloud)
            self.assertIsInstance(client.config.instance_mapping, MarqoCloudInstanceMappings)
            self.assertEqual(client.config.instance_mapping.get_control_base_url(), cloud_url + "/api")

    def test_non_cloud_endpoint_client_is_not_marqo_cloud(self):
        """
        Checks that if a client is created with a non-cloud URL, the client is not set to Marqo Cloud.
        """
        client = Client(url="https://arandomsite.ai")
        self.assertFalse(client.config.is_marqo_cloud)
        self.assertIsInstance(client.config.instance_mapping, DefaultInstanceMappings)
        self.assertEqual(client.config.instance_mapping.get_control_base_url(), "https://arandomsite.ai")


@mark.fixed
class TestClientMethods(MarqoTestCase):
    def setUp(self) -> None:
        self.initial_marqo_cloud_url = os.environ.get("MARQO_CLOUD_URL", constants.CLOUD_AWS_API_ENDPOINT)

    def tearDown(self) -> None:
        os.environ["MARQO_CLOUD_URL"] = self.initial_marqo_cloud_url

    def test_is_cloud_api_endpoint(self):
        # Custom Marqo Cloud URL
        os.environ["MARQO_CLOUD_URL"] = "https://arandomsite.ai"
        with self.subTest("Env var set, matches."):
            self.assertTrue(Client._is_cloud_api_endpoint("https://arandomsite.ai"))
        with self.subTest("Env var set, does not match."):
            self.assertFalse(Client._is_cloud_api_endpoint(constants.CLOUD_AWS_API_ENDPOINT))

        # Default Marqo Cloud URL
        del os.environ["MARQO_CLOUD_URL"]
        with self.subTest("Env var not set, matches AWS."):
            self.assertTrue(Client._is_cloud_api_endpoint(constants.CLOUD_AWS_API_ENDPOINT))
        with self.subTest("Env var not set, matches GCP."):
            self.assertTrue(Client._is_cloud_api_endpoint(constants.CLOUD_GCP_API_ENDPOINT))
        with self.subTest("Env var not set, does not match."):
            self.assertFalse(Client._is_cloud_api_endpoint("https://arandomsite.ai"))