import os

import requests
from unittest import mock
from unittest.mock import patch
from pytest import mark

from marqo1.client import Client
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo1.errors import BadRequestError
from marqo1.errors import BackendTimeoutError, BackendCommunicationError
import warnings


class TestClient(MarqoTestCase):

    @mark.ignore_during_cloud_tests
    def test_get_marqo(self):
        res = self.client.get_marqo()
        assert 'Welcome to Marqo' == res['message']

    @mark.ignore_during_cloud_tests
    def test_health(self):
        res = self.client.health()
        assert 'status' in res
        assert 'status' in res['backend']

    @mark.ignore_during_cloud_tests
    def test_error_handling_in_health_check(self):
        client = Client(**self.client_settings)
        side_effect_list = [requests.exceptions.JSONDecodeError("test", "test", 1), BackendCommunicationError("test"),
                            BackendTimeoutError("test"), requests.exceptions.RequestException("test"),
                            KeyError("test"), KeyError("test"), requests.exceptions.Timeout("test")]
        for i, side_effect in enumerate(side_effect_list):
            with mock.patch("marqo._httprequests.HttpRequests.get") as mock_get:
                mock_get.side_effect = side_effect

                with self.assertRaises(BadRequestError) as cm:
                    res = client.health()

                # Assert the error message is what you expect
                self.assertIn("If you are trying to check the health on Marqo Cloud", cm.exception.message)

    def test_check_index_health_response(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        res = self.client.index(test_index_name).health()
        assert 'status' in res
        assert 'status' in res['backend']

    def test_check_index_health_query(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
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

    def test_raises_deprecation_warning_and_error_for_cloud(self):
        mq = Client(**self.client_settings)
        for operation in [mq.health, mq.get_marqo, mq.get_cpu_info, mq.get_loaded_models]:
            if not mq.config.is_marqo_cloud:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    operation()
                    assert len(w) == 1
                    assert issubclass(w[-1].category, DeprecationWarning)
                    assert "mq.index(index_name)." in str(w[-1].message)
            else:
                with self.assertRaises(BadRequestError):
                    operation()
