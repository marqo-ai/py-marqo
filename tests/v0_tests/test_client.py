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
