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
        current = os.environ.get("MARQO_CLOUD_URL", "api.marqo.ai")
        os.environ["MARQO_CLOUD_URL"] = "https://cloud.url.com"
        client = Client(url="https://cloud.url.com", api_key="test")
        self.assertTrue(client.config.is_marqo_cloud)
        os.environ["MARQO_CLOUD_URL"] = current
