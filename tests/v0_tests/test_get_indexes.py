import time
import marqo.index
from tests.marqo_test import MarqoTestCase
from pytest import mark

class TestGetIndexes(MarqoTestCase):
    def _is_index_name_in_get_indexes_response(self, index_name, get_indexes_response):
        for found_index in get_indexes_response['results']:
            if index_name == found_index.index_name:
                return True
        return False

    def test_get_indexes_simple(self):
        """this can be run on the cloud"""
        test_index_name = self.create_test_index(self.generic_test_index_name)
        assert self._is_index_name_in_get_indexes_response(test_index_name, self.client.get_indexes())

    @mark.ignore_during_cloud_tests
    def test_get_indexes(self):
        """Asserts that the results grow after each create_index request
        If this test breaks, ensure another user isn't using the same Marqo
        instance.
        """
        ix_0 = self.client.get_indexes()
        assert not self._is_index_name_in_get_indexes_response(self.generic_test_index_name, ix_0)

        self.test_index_name = self.create_test_index(self.generic_test_index_name)
        ix_1 = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(self.test_index_name, ix_1)

        self.test_index_name_2 = self.create_test_index(self.generic_test_index_name_2)
        ix_2 = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(self.test_index_name_2, ix_2)

        # since indexes are not deleted after each test for cloud instances, this assert may not be correct.
        if not self.client.config.is_marqo_cloud:
            assert len(ix_2['results']) > len(ix_1['results'])
            assert len(ix_1['results']) > len(ix_0['results'])

        for found_index in ix_2['results']:
            assert isinstance(found_index, marqo.index.Index)

    def test_get_indexes_usable(self):
        """Are the indices we get back usable? """
        self.test_index_name = self.create_test_index(self.generic_test_index_name)
        get_ixes_res = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(self.test_index_name, get_ixes_res)

        my_ix = None
        for found_index in get_ixes_res['results']:
            if self.test_index_name == found_index.index_name:
                my_ix = found_index

        if my_ix is None:
            raise AssertionError

        assert my_ix.get_stats()['numberOfDocuments'] == 0
        my_ix.add_documents([{'some doc': 'gold fish'}], tensor_fields=['some doc'])

        if self.IS_MULTI_INSTANCE:
            time.sleep(1)

        assert my_ix.get_stats()['numberOfDocuments'] == 1

        if self.IS_MULTI_INSTANCE:
            self.warm_request(my_ix.search,q='aquatic animal')

        assert len(my_ix.search(q='aquatic animal')['hits']) == 1
