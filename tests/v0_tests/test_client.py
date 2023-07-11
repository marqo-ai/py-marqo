import pprint
import unittest
from unittest import mock
from marqo import config
from marqo import enums
from marqo.client import Client
from marqo import utils
from tests.marqo_test import MarqoTestCase
from marqo.errors import MarqoApiError


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

    def test_version_check_initialization(self):
        with mock.patch("marqo.client.mq_logger.warning") as mock_warning:
            with mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                mock_get_marqo.return_value = {'version': '0.0.0'}
                client = Client(**self.client_settings)

        # Check the warning was logged
        mock_warning.assert_called_once()

        # Get the warning message
        warning_message = mock_warning.call_args[0][0]

        # Assert the message is what you expect
        self.assertIn("Please upgrade your Marqo instance to avoid potential errors.",  warning_message)

    def test_version_check_in_add_documents(self):
        with mock.patch("marqo.client.mq_logger.warning") as mock_warning:
            with mock.patch("marqo.client.Client.get_marqo") as mock_get_marqo:
                mock_get_marqo.return_value = {'version': '0.0.0'}
                client = Client(**self.client_settings)
                client.create_index("my_test_index")
                client.index("my_test_index").add_documents([{"name": "test"}])

        # Ensure get_marqo is only called once
        mock_get_marqo.assert_called_once()

        # Check the warning was called twice
        self.assertEqual(mock_warning.call_count, 2)

        # Get the warning message
        warning_message = mock_warning.call_args[0][0]

        # Assert the message is what you expect
        self.assertIn("Please upgrade your Marqo instance to avoid potential errors.",  warning_message)