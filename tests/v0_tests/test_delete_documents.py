import copy
import functools
import math
import pprint
import random
import pytest
import requests
import time

from pytest import mark

from marqo.errors import MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo import enums
from unittest import mock


class TestDeleteDocuments(MarqoTestCase):
    def test_delete_docs(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        self.client.index(test_index_name).add_documents([
            {"abc": "wow camel", "_id": "123"},
            {"abc": "camels are cool", "_id": "foo"}
        ], tensor_fields=["abc"], auto_refresh=True)

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(test_index_name).search, "wow camel")

        res0 = self.client.index(test_index_name).search("wow camel")
        assert res0['hits'][0]["_id"] == "123"
        assert len(res0['hits']) == 2
        self.client.index(test_index_name).delete_documents(["123"], auto_refresh=True)

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(test_index_name).search, "wow camel")
        res1 = self.client.index(test_index_name).search("wow camel")
        assert res1['hits'][0]["_id"] == "foo"
        assert len(res1['hits']) == 1
    
    def test_delete_documents_defaults(self):
        """
        Ensure that the expected default values are used for the delete documents API call
        Note that some parameters should have no default created by the client, thus should
        not be present in the request.
        """
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).delete_documents(
                ids=['0', '1', '2']
            )
            return True

        assert run()

        args, kwargs0 = mock__post.call_args_list[0]

       # Ensure client does NOT autofill refresh parameter
        assert "refresh" not in kwargs0["path"]
        # These parameters were explicitly defined:
        assert kwargs0["body"] == ['0', '1', '2']

    def test_delete_docs_with_refresh(self):
        """
        Ensure that refresh parameter for delete docs is properly set in API call
        """
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).delete_documents(
                ids=['0', '1', '2'], auto_refresh=True
            )
            temp_client.index(self.generic_test_index_name).delete_documents(
                ids=['0', '1', '2'], auto_refresh=False
            )
            return True

        assert run()

        args, kwargs0 = mock__post.call_args_list[0]
        args, kwargs1 = mock__post.call_args_list[1]

        assert "refresh=true" in kwargs0["path"]
        assert "refresh=false" in kwargs1["path"]


    def test_delete_docs_empty_ids(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        self.client.index(test_index_name).add_documents([{"abc": "efg", "_id": "123"}], tensor_fields=["abc"], auto_refresh=True)
        try:
            self.client.index(test_index_name).delete_documents([])
            raise AssertionError
        except MarqoWebError as e:
            assert "can't be empty" in str(e) or "value_error.missing" in str(e)
        res = self.client.index(test_index_name).get_document("123")
        assert "abc" in res