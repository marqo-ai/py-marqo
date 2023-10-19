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
        ], tensor_fields=["abc"])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(test_index_name).search, "wow camel")

        res0 = self.client.index(test_index_name).search("wow camel")
        print("res0res0")
        pprint.pprint(res0)
        assert res0['hits'][0]["_id"] == "123"
        assert len(res0['hits']) == 2
        self.client.index(test_index_name).delete_documents(["123"])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(test_index_name).search, "wow camel")
        res1 = self.client.index(test_index_name).search("wow camel")
        assert res1['hits'][0]["_id"] == "foo"
        assert len(res1['hits']) == 1

    def test_delete_docs_empty_ids(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        self.client.index(test_index_name).add_documents([{"abc": "efg", "_id": "123"}], tensor_fields=["abc"])
        try:
            self.client.index(test_index_name).delete_documents([])
            raise AssertionError
        except MarqoWebError as e:
            assert "can't be empty" in str(e) or "value_error.missing" in str(e)
        res = self.client.index(test_index_name).get_document("123")
        print(res)
        assert "abc" in res
    
    def test_delete_docs_response(self):
        """
        Ensure that delete docs response has the correct format
        items list, index_name, status, type, details, duration, startedAt, finishedAt
        """

        self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        self.client.index(self.generic_test_index_name).add_documents([
            {"_id": "doc1", "abc": "wow camel"},
            {"_id": "doc2", "abc": "camels are cool"},
            {"_id": "doc3", "abc": "wow camels again"}
        ], tensor_fields=[], auto_refresh=True)

        res = self.client.index(self.generic_test_index_name).delete_documents(["doc1", "doc2", "missingdoc"], auto_refresh=True)
        
        assert "duration" in res
        assert "startedAt" in res
        assert "finishedAt" in res

        assert res["index_name"] == self.generic_test_index_name
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