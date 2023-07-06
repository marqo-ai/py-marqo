from marqo.client import Client
from marqo.errors import MarqoApiError
from tests.marqo_test import MarqoTestCase
import time
from typing import Dict, List

class TestAddDocuments(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        self.index_name_2 = "my-test-index-2"
        self.index_names = [self.index_name_1, self.index_name_2]
        for ix_name in self.index_names:
            try:
                self.client.delete_index(ix_name)
            except MarqoApiError:
                pass

    def tearDown(self) -> None:
        for ix_name in self.index_names:
            try:
                self.client.delete_index(ix_name)
            except MarqoApiError:
                pass

    def _is_index_name_in_get_indexes_response(self, index_name: str, get_indexes_response: Dict[str, List[Dict[str, str]]]):
        for found_index in get_indexes_response['results']:
            if index_name == found_index["index_name"]:
                return True
        return False

    def test_get_indexes(self):
        """Asserts that the results grow after each create_index request
        If this test breaks, ensure another user isn't using the same Marqo
        instance.
        """
        ix_0 = self.client.get_indexes()
        assert not self._is_index_name_in_get_indexes_response(self.index_name_1, ix_0)

        self.client.create_index(self.index_name_1)
        ix_1 = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(self.index_name_1, ix_1)

        self.client.create_index(self.index_name_2)
        ix_2 = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(self.index_name_2, ix_2)

        assert len(ix_2['results']) > len(ix_1['results'])
        assert len(ix_1['results']) > len(ix_0['results'])

        for found_index in ix_2['results']:
            assert isinstance(found_index, dict)