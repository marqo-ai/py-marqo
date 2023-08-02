import copy
import math
import pprint
import random
import requests
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase
from marqo import enums
from unittest import mock


class TestBoostSearch(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        if not self.client.config.is_marqo_cloud:
            try:
                self.client.delete_index(self.index_name_1)
            except MarqoApiError as s:
                pass
        self.create_index(index_name=self.index_name_1)
        self.client.index(index_name=self.index_name_1).add_documents(
            [
                {
                    "Title": "A comparison of the best pets",
                    "Description": "Animals",
                    "_id": "d1"
                },
                {
                    "Title": "The history of dogs",
                    "Description": "A history of household pets",
                    "_id": "d2"
                }
            ],
            tensor_fields=["Title", "Description"]
        )

        self.query = "What are the best pets"

    def tearDown(self) -> None:
        if not self.client.config.is_marqo_cloud:
            try:
                self.client.delete_index(self.index_name_1)
            except MarqoApiError as s:
                pass
        else:
            self.delete_documents(self.index_name_1)

    def test_boost_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,q= self.query, boost = {"Title": [1,0], "Description" : [1,0]})

        boost_res = self.client.index(self.index_name_1).search(q= self.query, boost = {"Title": [1,0], "Description" : [1,0]})

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,q= self.query, boost = {"void": [10,10], "void2" : [10,20]})

        no_matched_res = self.client.index(self.index_name_1).search(q= self.query, boost = {"void": [10,10], "void2" : [10,20]})
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,q= self.query)

        res = self.client.index(self.index_name_1).search(q= self.query)

        del boost_res['processingTimeMs']
        del res['processingTimeMs']
        del no_matched_res['processingTimeMs']

        self.assertEqual(boost_res, res)
        self.assertEqual(no_matched_res, res)


    def test_boost_search_results(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,q=self.query,
                                                                     boost={"Title": [1, 1]})
        
        boost_res = self.client.index(self.index_name_1).search(q=self.query,
                                                                     boost={"Title": [1, 1]})
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,q=self.query)

        res = self.client.index(self.index_name_1).search(q=self.query)

        boost_score = boost_res["hits"][0]["_score"]
        res_score_positive = res["hits"][0]["_score"]

        self.assertEqual(boost_score, res_score_positive * 1 + 1)


    def test_boost_search_query_format(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.index_name_1).search,q=self.query,
                                                                         boost=["Title", [1,2]])
                
            boost_res = self.client.index(self.index_name_1).search(q=self.query,
                                                                         boost=["Title", [1,2]])
            raise AssertionError
        except MarqoWebError:
            pass


    def test_boost_search_searchable_attributes_mismatch(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.index_name_1).search,q=self.query,
                                                                         boost={"Title": [1, 1]}
                                                                         ,searchable_attributes=["Description"])
                
            boost_res = self.client.index(self.index_name_1).search(q=self.query,
                                                                         boost={"Title": [1, 1]}
                                                                         ,searchable_attributes=["Description"])
            raise AssertionError
        except MarqoWebError:
            pass

