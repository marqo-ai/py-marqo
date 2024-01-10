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
    @mark.fixed
    def test_delete_docs(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents([
                {"abc": "wow camel", "_id": "123"},
                {"abc": "camels are cool", "_id": "foo"}
            ], tensor_fields=["abc"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search, "wow camel")

            res0 = self.client.index(test_index_name).search("wow camel")
            assert res0['hits'][0]["_id"] == "123"
            assert len(res0['hits']) == 2
            self.client.index(test_index_name).delete_documents(["123"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search, "wow camel")
            res1 = self.client.index(test_index_name).search("wow camel")
            assert res1['hits'][0]["_id"] == "foo"
            assert len(res1['hits']) == 1

    @mark.fixed
    def test_delete_documents_defaults(self):
        """
        Ensure that the expected default values are used for the delete documents API call
        Note that some parameters should have no default created by the client, thus should
        not be present in the request.
        """
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()

        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
            def run():
                temp_client.index(test_index_name).delete_documents(
                    ids=['0', '1', '2']
                )
                return True

            assert run()

            args, kwargs0 = mock__post.call_args_list[0]

            # These parameters were explicitly defined:
            assert kwargs0["body"] == ['0', '1', '2']

    @mark.fixed
    def test_delete_docs_empty_ids(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents([{"abc": "efg", "_id": "123"}], tensor_fields=["abc"])
            try:
                self.client.index(test_index_name).delete_documents([])
                raise AssertionError
            except MarqoWebError as e:
                assert "can't be empty" in str(e) or "value_error.missing" in str(e)
            res = self.client.index(test_index_name).get_document("123")
            assert "abc" in res

    def test_delete_docs_response(self):
        """
        Ensure that delete docs response has the correct format
        items list, index_name, status, type, details, duration, startedAt, finishedAt
        """

        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents([
                {"_id": "doc1", "abc": "wow camel"},
                {"_id": "doc2", "abc": "camels are cool"},
                {"_id": "doc3", "abc": "wow camels again"}
            ], tensor_fields=[])

            res = self.client.index(test_index_name).delete_documents(["doc1", "doc2", "missingdoc"])

            assert "duration" in res
            assert "startedAt" in res
            assert "finishedAt" in res

            print(res)
            assert res["indexName"] == test_index_name
            assert res["type"] == "documentDeletion"
            assert res["status"] == "succeeded"
            assert res["details"] == {
                "receivedDocumentIds":3,
                "deletedDocuments":2
            }
            assert len(res["items"]) == 3

            for item in res["items"]:
                assert "_id" in item
                assert "_shards" in item
                if item["_id"] in {"doc1", "doc2"}:
                    assert item["status"] == 200
                    assert item["result"] == "deleted"
                elif item["_id"] == "missingdoc":
                    assert item["status"] == 404
                    assert item["result"] == "not_found"
