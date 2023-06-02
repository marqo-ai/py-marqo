import logging
import subprocess
import time
from requests import HTTPError
from tests import marqo_test
from marqo import Client
from marqo.errors import MarqoApiError, BackendCommunicationError, MarqoWebError


class TestThrottling(marqo_test.MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def test_index_limit(self):
        pass
