import requests
from json import JSONDecodeError
from unittest import mock
from unittest.mock import patch

from marqo.client import Client
from tests.marqo_test import MarqoTestCase
from marqo.errors import MarqoApiError, BadRequestError
from marqo.client import marqo_url_and_version_cache
from marqo.errors import BackendTimeoutError, BackendCommunicationError


class TestClient(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass
        marqo_url_and_version_cache.clear()

    def tearDown(self) -> None:
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass
        marqo_url_and_version_cache.clear()

    def test_get_marqo(self):
        res = self.client.get_marqo()
        assert 'Welcome to Marqo' == res['message']

    def test_health(self):
        res = self.client.health()
        assert 'status' in res
        assert 'status' in res['backend']

    def test_health_deprecation_warning(self):
        with mock.patch("marqo.client.mq_logger.warning") as mock_warning:
            res = self.client.health()

            # Check the warning was logged
            mock_warning.assert_called_once()
            warning_message = mock_warning.call_args[0][0]
            self.assertIn("The `client.health()` API has been deprecated and will be removed in", warning_message)

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
        self.client.create_index(self.index_name_1)
        res = self.client.index(self.index_name_1).health()
        assert 'status' in res
        assert 'status' in res['backend']

    def test_check_index_health_query(self):
        with patch("marqo._httprequests.HttpRequests.get") as mock_get:
            self.client.create_index(self.index_name_1)
            res = self.client.index(self.index_name_1).health()
            args, kwargs = mock_get.call_args
            self.assertIn(f"/{self.index_name_1}/health", kwargs["path"])

    def test_version_check_instantiation(self):
        with mock.patch("marqo.client.mq_logger.warning") as mock_warning, \
                mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
            mock_get_marqo.return_value = {'version': '0.0.0'}
            client = Client(**self.client_settings)

            mock_get_marqo.assert_called_once()

            # Check the warning was logged
            mock_warning.assert_called_once()

            # Get the warning message
            warning_message = mock_warning.call_args[0][0]

            # Assert the message is what you expect
            self.assertIn("Please upgrade your Marqo instance to avoid potential errors.", warning_message)

            # Assert the url is in the cache
            self.assertIn(self.client_settings['url'], marqo_url_and_version_cache)
            assert marqo_url_and_version_cache[self.client_settings['url']] == '0.0.0'

    def test_skip_version_check_for_cloud_v2(self):
        for url in ["https://api.marqo.ai", "https://cloud.marqo.ai"]:
            with mock.patch("marqo.client.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                mock_get_marqo.return_value = {'version': '0.0.0'}
                client = Client(url=url)

                mock_get_marqo.assert_not_called()
                if url == "https://api.marqo.ai":
                    url += '/api'
                assert url in marqo_url_and_version_cache
                assert marqo_url_and_version_cache[url] == '_skipped'

    def test_skip_version_check_for_previously_labelled_url(self):
        with mock.patch.dict("marqo.client.marqo_url_and_version_cache",
                             {self.client_settings["url"]: "_skipped"}) as mock_cache, \
                mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
            client = Client(**self.client_settings)

            mock_get_marqo.assert_not_called()

    def test_error_handling_in_version_check(self):
        side_effect_list = [requests.exceptions.JSONDecodeError("test", "test", 1), BackendCommunicationError("test"),
                            BackendTimeoutError("test"), requests.exceptions.RequestException("test"),
                            KeyError("test"), KeyError("test"), requests.exceptions.Timeout("test")]
        for i, side_effect in enumerate(side_effect_list):
            with mock.patch("marqo.client.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                mock_get_marqo.side_effect = side_effect
                client = Client(**self.client_settings)

                mock_get_marqo.assert_called_once()

                # Check the warning was logged
                mock_warning.assert_called_once()

                # Get the warning message
                warning_message = mock_warning.call_args[0][0]

                # Assert the message is what you expect
                self.assertIn("Marqo encountered a problem trying to check the Marqo version found", warning_message)
                self.assertEqual(marqo_url_and_version_cache, dict({self.client_settings["url"]: "_skipped"}))

                marqo_url_and_version_cache.clear()

    def test_version_check_multiple_instantiation(self):
        """Ensure that duplicated instantiation of the client does not result in multiple APIs calls of get_marqo()"""
        with mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
            mock_get_marqo.return_value = {'version': '0.0.0'}
            client = Client(**self.client_settings)

            mock_get_marqo.assert_called_once()
            mock_get_marqo.reset_mock()

        for _ in range(10):
            with mock.patch("marqo.client.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                client = Client(**self.client_settings)

                mock_get_marqo.assert_not_called()
                mock_warning.assert_called_once()

    def test_skipped_version_check_multiple_instantiation(self):
        """Ensure that the url labelled as `_skipped` only call get_marqo() once"""
        url = "https://dummy/url"
        with mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
            mock_get_marqo.side_effect = requests.exceptions.RequestException("test")
            client = Client(url=url)

            mock_get_marqo.assert_called_once()
            mock_get_marqo.reset_mock()
            assert marqo_url_and_version_cache[url] == '_skipped'

        for _ in range(10):
            with mock.patch("marqo.client.mq_logger.warning") as mock_warning, \
                    mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                client = Client(url=url)

                mock_get_marqo.assert_not_called()
                # Check the warning was logged every instantiation
                mock_warning.assert_called_once()
