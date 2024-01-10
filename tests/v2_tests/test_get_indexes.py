import time
import marqo.index
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


class TestGetIndexes(MarqoTestCase):
    def _is_index_name_in_get_indexes_response(self, index_name, get_indexes_response):
        for found_index in get_indexes_response['results']:
            if index_name == found_index["indexName"]:
                return True
        return False

    def test_get_indexes_simple(self):
        """this can be run on the cloud"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            assert self._is_index_name_in_get_indexes_response(test_index_name, self.client.get_indexes())

    @mark.ignore_during_cloud_tests
    def test_get_indexes(self):
        """Asserts that the results grow after each create_index request
        If this test breaks, ensure another user isn't using the same Marqo
        instance.
        """
        ix_0 = self.client.get_indexes()
        assert not self._is_index_name_in_get_indexes_response(self.generic_test_index_name, ix_0)

        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        ix_1 = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(test_index_name, ix_1)

        test_index_name_2 = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name_2,
        )
        ix_2 = self.client.get_indexes()
        assert self._is_index_name_in_get_indexes_response(test_index_name_2, ix_2)

        # since indexes are not deleted after each test for cloud instances, this assert may not be correct.
        if not self.client.config.is_marqo_cloud:
            assert len(ix_2['results']) > len(ix_1['results'])
            assert len(ix_1['results']) > len(ix_0['results'])

        for found_index in ix_2['results']:
            assert isinstance(found_index, marqo.index.Index)

    @mark.fixed
    def test_get_indexes_usable(self):
        """Are the indices we get back usable? """
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            get_ixes_res = self.client.get_indexes()
            assert self._is_index_name_in_get_indexes_response(test_index_name, get_ixes_res)

            my_ix = None
            for found_index in get_ixes_res['results']:
                if test_index_name == found_index["indexName"]:
                    my_ix = self.client.index(found_index["indexName"])

            if my_ix is None:
                raise AssertionError

            assert my_ix.get_stats()['numberOfDocuments'] == 0
            my_ix.add_documents([{'some_doc': 'gold fish'}], tensor_fields=['some_doc'])

            if self.IS_MULTI_INSTANCE:
                time.sleep(1)

            assert my_ix.get_stats()['numberOfDocuments'] == 1

            if self.IS_MULTI_INSTANCE:
                self.warm_request(my_ix.search, q='aquatic animal')

            assert len(my_ix.search(q='aquatic animal')['hits']) == 1
