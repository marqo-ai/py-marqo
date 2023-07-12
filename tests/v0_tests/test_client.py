from unittest import mock
from marqo.client import Client
from tests.marqo_test import MarqoTestCase
from marqo.errors import MarqoApiError
from marqo.client import marqo_url_and_version_cache


class TestClient(MarqoTestCase):

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

    def test_get_marqo(self):
        res = self.client.get_marqo()
        assert 'Welcome to Marqo' == res['message']

    def test_health(self):
        res = self.client.health()
        assert 'status' in res
        assert 'status' in res['backend']


class TestMinimumSupportedMarqoVersion(MarqoTestCase):

    def setUp(self) -> None:
        self.client =   Client(**self.client_settings)
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

    def test_version_check_instantiation(self):
        with mock.patch("marqo.client.mq_logger.warning") as mock_warning:
            with mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                mock_get_marqo.return_value = {'version': '0.0.0'}
                client = Client(**self.client_settings)

        mock_get_marqo.assert_called_once()

        # Check the warning was logged
        mock_warning.assert_called_once()

        # Get the warning message
        warning_message = mock_warning.call_args[0][0]

        # Assert the message is what you expect
        self.assertIn("Please upgrade your Marqo instance to avoid potential errors.",  warning_message)

        # Assert the url is in the cache
        self.assertIn(self.client_settings['url'], marqo_url_and_version_cache)
        assert marqo_url_and_version_cache[self.client_settings['url']] == '0.0.0'

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

            mock_get_marqo.reset_mock()
            mock_warning.reset_mock()