import time
import copy
import os

import marqo
from marqo.index import marqo_url_and_version_cache
from marqo import enums
from typing import Any, Dict, List
from unittest import mock
from pytest import mark
from marqo.errors import InvalidArgError
import requests
import random
import math

from marqo.models.search_models import BulkSearchBody
from tests.marqo_test import (mock_http_traffic, with_documents, MockHTTPTraffic, MarqoTestCase, CloudTestIndex
                              )
from tests.cloud_test_logic.cloud_instance_mappings import GetIndexesIndexResponseObject, mock_get_indexes_response


class TestBulkSearch(MarqoTestCase):
    @staticmethod
    def strip_marqo_fields(doc, strip_id=True):
        """Strips Marqo fields from a returned doc to get the original doc"""
        copied = copy.deepcopy(doc)

        strip_fields = ["_highlights", "_score"]
        if strip_id:
            strip_fields += ["_id"]

        for to_strip in strip_fields:
            del copied[to_strip]

        return copied

    @mock_http_traffic([
        MockHTTPTraffic(
            http_operation="post",
            path="indexes/bulk/search?&device=cpu",
            content_type='application/json',
            body={
                "queries": [
                    {
                        "q": "title about some doc",
                        "searchableAttributes": None,
                        "searchMethod": "TENSOR",
                        "limit": 10,
                        "offset": 0,
                        "showHighlights": True,
                        "reRanker": None,
                        "filter": None,
                        "attributesToRetrieve": None,
                        "boost": None,
                        "image_download_headers": None,
                        "context": {
                            "tensor": [
                                {"vector": [1, 1, 1], "weight": 0},
                                {"vector": [2, 2], "weight": 0}
                            ]
                        },
                        "scoreModifiers": None,
                        "modelAuth": None,
                        "index": "test-index"
                    }
                ]
            }
        )
    ], forbid_extra_calls=True)
    @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    def test_bulk_search_with_context(self):
        """Check that context is passed to HTTP request correctly"""
        marqo_url_and_version_cache.clear()
        # this is done to prevent extra call to get_marqo in _marqo_minimum_supported_version_check
        # which would raise issue due to forbid_extra_calls=True
        cache_url = self.client.config.instance_mapping.get_index_base_url(self.generic_test_index_name)
        marqo_url_and_version_cache[cache_url] = "v0"
        self.client.bulk_search([{
            "index": self.generic_test_index_name,
            "q": "title about some doc",
            "context": {"tensor": [{"vector": [1, ] * 3, "weight": 0}, {"vector": [2, ] * 2, "weight": 0}], }
        }], device="cpu")
        marqo_url_and_version_cache.clear()

    @mock_http_traffic([
        MockHTTPTraffic(
            http_operation="post",
            path="indexes/bulk/search?&device=cpu",
            content_type='application/json',
            body={
                "queries": [
                    {
                        "q": "title about some doc",
                        "searchableAttributes": None,
                        "searchMethod": "TENSOR",
                        "limit": 10,
                        "offset": 0,
                        "showHighlights": True,
                        "reRanker": None,
                        "filter": None,
                        "attributesToRetrieve": None,
                        "boost": None,
                        "image_download_headers": None,
                        "context": None,
                        "scoreModifiers": {
                            "multiply_score_by": [
                                {"field_name": "multiply_1", "weight": 1,},
                                {"field_name": "multiply_2",}
                            ],
                            "add_to_score": [
                                {"field_name": "add_1", "weight" : -3},
                                {"field_name": "add_2", "weight": 1}
                            ]
                        },
                        "modelAuth": None,
                        "index": "test-index"
                    }
                ]
            }
        )
    ], forbid_extra_calls=True)
    @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    def test_bulk_search_with_scoreModifiers(self):
        """Check that context is passed to HTTP request correctly"""
        marqo_url_and_version_cache.clear()
        # this is done to prevent extra call to get_marqo in _marqo_minimum_supported_version_check
        # which would raise issue due to forbid_extra_calls=True
        cache_url = self.client.config.instance_mapping.get_index_base_url(self.generic_test_index_name)
        marqo_url_and_version_cache[cache_url] = "v0"
        client = marqo.Client(**self.client_settings)
        client.bulk_search([{
            "index": self.generic_test_index_name,
            "q": "title about some doc",
            "scoreModifiers": {
                "multiply_score_by": [
                    {"field_name": "multiply_1", "weight": 1,},
                    {"field_name": "multiply_2",}
                ],
                "add_to_score": [
                    {"field_name": "add_1", "weight" : -3},
                    {"field_name": "add_2", "weight": 1}
                ]
            }
        }], device="cpu")
        marqo_url_and_version_cache.clear()

    @with_documents(lambda self: {self.generic_test_index_name: [{
        "Title": "This is a title about some doc. ",
        "Description": """The Guardian is a British daily newspaper. It was founded in 1821 as The Manchester Guardian, and changed its name in 1959.[5] Along with its sister papers The Observer and The Guardian Weekly, The Guardian is part of the Guardian Media Group, owned by the Scott Trust.[6] The trust was created in 1936 to "secure the financial and editorial independence of The Guardian in perpetuity and to safeguard the journalistic freedom and liberal values of The Guardian free from commercial or political interference".[7] The trust was converted into a limited company in 2008, with a constitution written so as to maintain for The Guardian the same protections as were built into the structure of the Scott Trust by its creators. Profits are reinvested in journalism rather than distributed to owners or shareholders.[7] It is considered a newspaper of record in the UK.[8][9]
        The editor-in-chief Katharine Viner succeeded Alan Rusbridger in 2015.[10][11] Since 2018, the paper's main newsprint sections have been published in tabloid format. As of July 2021, its print edition had a daily circulation of 105,134.[4] The newspaper has an online edition, TheGuardian.com, as well as two international websites, Guardian Australia (founded in 2013) and Guardian US (founded in 2011). The paper's readership is generally on the mainstream left of British political opinion,[12][13][14][15] and the term "Guardian reader" is used to imply a stereotype of liberal, left-wing or "politically correct" views.[3] Frequent typographical errors during the age of manual typesetting led Private Eye magazine to dub the paper the "Grauniad" in the 1960s, a nickname still used occasionally by the editors for self-mockery.[16]
        """
    }]}, warmup_query="title about some doc")
    def test_bulk_search_single(self, index_1_docs: List[Dict[str, Any]]):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            delete_index_documents_before_test=False
        )
        resp = self.client.bulk_search([{
            "index": test_index_name,
            "q": "title about some doc"
        }])
        assert len(resp['result']) == 1
        search_res = resp['result'][0]

        assert len(search_res["hits"]) == 1
        assert self.strip_marqo_fields(search_res["hits"][0]) == index_1_docs[0]
        assert len(search_res["hits"][0]["_highlights"]) > 0
        assert {"Title", "Description"} & set(search_res["hits"][0]["_highlights"])

    def test_search_empty_index(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        resp = self.client.bulk_search([{
            "index": test_index_name,
            "q": "title about some doc"
        }])
        assert len(resp['result']) == 1
        search_res = resp['result'][0]

        assert len(search_res["hits"]) == 0

    def test_search__extra_parameters_raise_exception(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )

        with self.assertRaises(InvalidArgError):
            self.client.bulk_search([{
                "index": test_index_name,
                "q": "title about some doc",
                "parameter-not-expected": 1,
            }])

    def test_search_highlights(self):
        """Tests if show_highlights works and if the deprecation behaviour is expected"""
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        self.client.index(index_name=test_index_name).add_documents([{"f1": "some doc"}], tensor_fields=["f1"], auto_refresh=True)
        for params, expected_highlights_presence in [
                ({}, True),
                ({"showHighlights": False}, False),
                ({"showHighlights": True}, True)
            ]:

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.bulk_search, [{**{
                    "index": test_index_name,
                    "q": "title about some doc"
                }, **params}])

            resp = self.client.bulk_search([{**{
                "index": test_index_name,
                "q": "title about some doc"
            }, **params}])
            assert len(resp['result']) == 1
            search_res = resp['result'][0]
            assert ("_highlights" in search_res["hits"][0]) is expected_highlights_presence

    @with_documents(lambda self: {self.generic_test_index_name: [{
            "doc title": "Cool Document 1",
            "field 1": "some extra info",
            "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
        },{
            "doc title": "Just Your Average Doc",
            "field X": "this is a solid doc",
            "_id": "123456"
    }]}, warmup_query="this is a solid doc")
    def test_bulk_search_multi(self, index_1_docs):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            delete_index_documents_before_test=False
        )
        resp = self.client.bulk_search([{
            "index": test_index_name,
            "q": "this is a solid doc"
        }])
        assert len(resp['result']) == 1
        search_res = resp['result'][0]

        assert index_1_docs[1] == self.strip_marqo_fields(search_res['hits'][0], strip_id=False)
        assert search_res['hits'][0]['_highlights']["field X"] == "this is a solid doc"

    @with_documents(lambda self: {self.generic_test_index_name: [{
            "doc title": "Very heavy, dense metallic lead.",
            "field 1": "some extra info",
            "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
        },{
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc",
            "_id": "123456"
        }]}, warmup_query="Examples of leadership")
    def test_select_lexical(self, index_1_docs):
        # Ensure that vector search works
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            delete_index_documents_before_test=False
        )
        resp = self.client.bulk_search([{
            "index": test_index_name,
            "q": "Examples of leadership"
        }])
        assert len(resp['result']) == 1
        search_res = resp['result'][0]

        assert index_1_docs[1] == self.strip_marqo_fields(search_res["hits"][0], strip_id=False)
        assert search_res["hits"][0]['_highlights']["doc title"].startswith("The captain bravely lead her followers")

        # try it with lexical search:
        #    can't find the above with synonym
        assert len(self.client.index(test_index_name).search(
            "Examples of leadership", search_method=marqo.SearchMethods.LEXICAL)["hits"]) == 0
        #    but can look for a word
        assert self.client.index(test_index_name).search(
            '"captain"')["hits"][0]["_id"] == "123456"

    @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    def test_bulk_search_with_device(self):
        """use default as defined in config unless overridden"""
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.bulk_search([{
                "index": self.generic_test_index_name,
                "q": "my search term"
            }], device="cuda:2")
            return True
        assert run()

        args, _ = mock__post.call_args_list[0]
        assert "device=cuda2" in args[0]

    @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    def test_bulk_search_with_no_device(self):
        """if no device is set, device should not be in path"""
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.bulk_search([{
                "index": self.generic_test_index_name,
                "q": "my search term"
            }])
            return True
        assert run()

        args, _ = mock__post.call_args_list[0]
        assert "device" not in args[0]

    def test_filter_string_and_searchable_attributes(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        docs = [
            {
                "_id": "0",                     # content in field_a
                "field_a": "random content",
                "str_for_filtering": "apple",
                "int_for_filtering": 0,
            },
            {
                "_id": "1",                     # content in field_b
                "field_b": "random content",
                "str_for_filtering": "banana",
                "int_for_filtering": 0,
            },
            {
                "_id": "2",                     # content in both
                "field_a": "random content",
                "field_b": "random content",
                "str_for_filtering": "apple",
                "int_for_filtering": 1,
            },
            {
                "_id": "3",                     # content in both
                "field_a": "random content",
                "field_b": "random content",
                "str_for_filtering": "banana",
                "int_for_filtering": 1,
            }
        ]
        res = self.client.index(test_index_name).add_documents(docs, auto_refresh=True, tensor_fields=["field_a", "field_b"])

        test_cases = (
            {   # filter string only (str)
                "query": "random content", 
                "filter_string": "str_for_filtering:apple", 
                "searchable_attributes": None,
                "expected": ["0", "2"]
            },  
            {   # filter string only (int)
                "query": "random content", 
                "filter_string": "int_for_filtering:0", 
                "searchable_attributes": None,
                "expected": ["0", "1"]
            },  
            {   # filter string only (str and int)
                "query": "random content", 
                "filter_string": "str_for_filtering:banana AND int_for_filtering:1", 
                "searchable_attributes": None,
                "expected": ["3"]
            },  
            {   # searchable attributes only (one)
                "query": "random content", 
                "filter_string": None,
                "searchable_attributes": ["field_b"], 
                "expected": ["1", "2", "3"]
            },   
            {   # searchable attributes only (both)
                "query": "random content", 
                "filter_string": None,
                "searchable_attributes": ["field_a", "field_b"], 
                "expected": ["0", "1", "2", "3"]
            },         
            {   # filter string and searchable attributes (one)
                "query": "random content",
                "filter_string": "str_for_filtering:apple",
                "searchable_attributes": ["field_b"],
                "expected": ["2"]
            },
            {   # filter string and searchable attributes (both)
                "query": "random content",
                "filter_string": "str_for_filtering:banana AND int_for_filtering:0",
                "searchable_attributes": ["field_a"],
                "expected": []
            }
        )

        for case in test_cases:
            query_object_0 = {
                "index": test_index_name,
                "q": case["query"],
                "filter": case.get("filter_string", ""),
                "searchableAttributes": case.get("searchable_attributes", None)
            }
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.bulk_search, [query_object_0])
            search_res = self.client.bulk_search([query_object_0])["result"][0]
            assert len(search_res["hits"]) == len(case["expected"])
            assert set([hit["_id"] for hit in search_res["hits"]]) == set(case["expected"])

    def test_attributes_to_retrieve(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "abc-123": "some text blah",
            "an_int": 2,
            "_id": "my-cool-doc"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc blah",
            "field1": "other things",
            "an_int": 2345678,
            "_id": "123456"
        }
        x = self.client.index(test_index_name).add_documents([
            d1, d2
        ], auto_refresh=True, tensor_fields=["doc title", "field X", "abc-123", "field1", "an_int"])
        atts = ["doc title", "an_int"]
        for search_method in [enums.SearchMethods.TENSOR,
                              enums.SearchMethods.LEXICAL]:
            
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.bulk_search,[{
                    "index": test_index_name,
                    "q": "blah blah",
                    "searchMethod": search_method,
                    "attributesToRetrieve": atts
                }])

            resp = self.client.bulk_search([{
                "index": test_index_name,
                "q": "blah blah",
                "searchMethod": search_method,
                "attributesToRetrieve": atts
            }])
            assert len(resp['result']) == 1
            search_res = resp['result'][0]

            assert len(search_res['hits']) == 2
            for hit in search_res['hits']:
                assert {k for k in hit.keys() if not k.startswith('_')} == set(atts)

    def test_pagination_single_field(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        
        # 100 random words
        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
        vocab = requests.get(vocab_source).text.splitlines()
        num_docs = 100
        docs = [{"Title": "a " + (" ".join(random.choices(population=vocab, k=25))),
                            "_id": str(i)
                            }
                        for i in range(num_docs)]
        
        self.client.index(test_index_name).add_documents(
            docs, auto_refresh=False, client_batch_size=50, tensor_fields=["Title"]
        )
        self.client.index(test_index_name).refresh()
        time.sleep(3)

        for search_method in (enums.SearchMethods.TENSOR, enums.SearchMethods.LEXICAL):
            for doc_count in [100]:
                # Query full results
                resp = self.client.bulk_search([{
                    "index": test_index_name,
                    "q": "a",
                    "searchMethod": search_method,
                    "limit":doc_count
                }])
                assert len(resp['result']) == 1
                full_search_results = resp['result'][0]
                
                for page_size in [1, 5, 10, 100]:
                    paginated_search_results = {"hits": []}

                    for page_num in range(math.ceil(num_docs / page_size)):
                        lim = page_size
                        off = page_num * page_size
                        resp = self.client.bulk_search([{
                            "index": test_index_name,
                            "q": "a",
                            "searchMethod": search_method,
                            "limit": lim,
                            "offset": off
                        }])
                        assert len(resp['result']) == 1
                        page_res = resp['result'][0]
                        
                        paginated_search_results["hits"].extend(page_res["hits"])

                    # Compare paginated to full results (length only for now)
                    assert len(full_search_results["hits"]) == len(paginated_search_results["hits"])

                    # TODO: re-add this assert when KNN incosistency bug is fixed
                    # assert full_search_results["hits"] == paginated_search_results["hits"]

    def test_multi_queries(self):
        docs = [
            {
                "loc a": "https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_realistic.png",
                "_id": 'realistic_hippo'},
            {"loc b": "https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_statue.png",
             "_id": 'artefact_hippo'}
        ]
        image_index_config = {
            'index_defaults': {
                'model': "ViT-B/16",
                'treat_urls_and_pointers_as_images': True
            }
        }
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.image_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_settings_dict=image_index_config
        )
        self.client.index(index_name=test_index_name).add_documents(
            documents=docs, auto_refresh=True, tensor_fields=["loc a", "loc b"]
        )
        queries_expected_ordering = [
            ({"Nature photography": 2.0, "Artefact": -2}, ['realistic_hippo', 'artefact_hippo']),
            ({"Nature photography": -1.0, "Artefact": 1.0}, ['artefact_hippo', 'realistic_hippo']),
            ({"https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_statue.png": -1.0,
              "blah": 1.0}, ['realistic_hippo', 'artefact_hippo']),
            ({"https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_statue.png": 2.0,
              "https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_realistic.png": -1.0},
             ['artefact_hippo', 'realistic_hippo']),
        ]
        for query, expected_ordering in queries_expected_ordering:
            resp = self.client.bulk_search([{
                "index": test_index_name,
                "q": query
            }])
            assert len(resp['result']) == 1
            res = resp['result'][0]

            # the poodle doc should be lower ranked than the irrelevant doc
            for hit_position, _ in enumerate(res['hits']):
                assert res['hits'][hit_position]['_id'] == expected_ordering[hit_position]

    def test_score_modifiers_in_bulk_search_end_to_end(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name
        )
        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "abc-123": "some text blah",
            "multiply": 2,
            "add": 1,
            "_id": "my-cool-doc"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc blah",
            "field1": "other things",
            "multiply": 2,
            "add": 1,
            "_id": "123456"
        }
        x = self.client.index(test_index_name).add_documents([
            d1, d2
        ], auto_refresh=True, tensor_fields=["doc title", "abc-123", "field X", "field1"])

        score_modifiers = {
            "multiply_score_by":[{"field_name": "multiply", "weight": 1}],
            "add_to_score": [{"field_name": "add", "weight": 2}]
        }

        for search_method in [enums.SearchMethods.TENSOR]:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.bulk_search, [{
                    "index": test_index_name,
                    "q": "blah blah",
                    "searchMethod": search_method,
                }])

            resp = self.client.bulk_search([{
                "index": test_index_name,
                "q": "blah blah",
                "searchMethod": search_method,
                "scoreModifiers": score_modifiers,
            }])
            assert len(resp['result']) == 1
            search_res = resp['result'][0]

            assert len(search_res['hits']) == 2
            for hit in search_res['hits']:
                assert hit["_score"] > 2

    @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    def test_query_validation_single_index_success(self):
        cloud_client_config = self.client_settings.copy()
        cloud_client_config["url"] = "cloud.url.marqo.ai"
        marqo_cloud_url_before_test = os.environ.get("MARQO_CLOUD_URL")
        os.environ["MARQO_CLOUD_URL"] = "cloud.url.marqo.ai"
        client = marqo.Client(**cloud_client_config)
        queries = [
            {
                "index": "test-index",
                "q": "blah blah",
                "searchMethod": enums.SearchMethods.TENSOR,
            },
            ]
        parsed_queries = [BulkSearchBody(**q) for q in queries]
        assert client._validate_all_indexes_belong_to_the_same_cluster(parsed_queries) is True

        if marqo_cloud_url_before_test:
            os.environ["MARQO_CLOUD_URL"] = marqo_cloud_url_before_test

    @mock_get_indexes_response([GetIndexesIndexResponseObject("test-index", "READY", "example.com"),
                                GetIndexesIndexResponseObject("test-index-2", "READY", "example2.com")])
    def test_query_validation_two_different_urls_fails(self):
        cloud_client_config = self.client_settings.copy()
        cloud_client_config["url"] = "cloud.url.marqo.ai"
        marqo_cloud_url_before_test = os.environ.get("MARQO_CLOUD_URL")
        os.environ["MARQO_CLOUD_URL"] = "cloud.url.marqo.ai"
        client = marqo.Client(**cloud_client_config)
        queries = [
            {
                "index": "test-index",
                "q": "blah blah",
                "searchMethod": enums.SearchMethods.TENSOR,
            },
            {
                "index": "test-index-2",
                "q": "blah blah",
                "searchMethod": enums.SearchMethods.TENSOR,
            },
            ]
        parsed_queries = [BulkSearchBody(**q) for q in queries]
        with self.assertRaises(InvalidArgError):
            client._validate_all_indexes_belong_to_the_same_cluster(parsed_queries)

        if marqo_cloud_url_before_test:
            os.environ["MARQO_CLOUD_URL"] = marqo_cloud_url_before_test

    @mock_get_indexes_response([GetIndexesIndexResponseObject("test-index", "READY", "example.com"),
                                GetIndexesIndexResponseObject("test-index-2", "READY", "example.com")])
    def test_query_validation_two_same_urls_success(self):
        cloud_client_config = self.client_settings.copy()
        cloud_client_config["url"] = "cloud.url.marqo.ai"
        marqo_cloud_url_before_test = os.environ.get("MARQO_CLOUD_URL")
        os.environ["MARQO_CLOUD_URL"] = "cloud.url.marqo.ai"
        client = marqo.Client(**cloud_client_config)
        queries = [
            {
                "index": "test-index",
                "q": "blah blah",
                "searchMethod": enums.SearchMethods.TENSOR,
            },
            {
                "index": "test-index-2",
                "q": "blah blah",
                "searchMethod": enums.SearchMethods.TENSOR,
            },
            ]
        parsed_queries = [BulkSearchBody(**q) for q in queries]
        assert client._validate_all_indexes_belong_to_the_same_cluster(parsed_queries) is True

        if marqo_cloud_url_before_test:
            os.environ["MARQO_CLOUD_URL"] = marqo_cloud_url_before_test

    @mark.ignore_during_cloud_tests
    def test_opensource_can_bulk_search_2_indices(self):
        self.client.create_index(self.generic_test_index_name)
        self.client.create_index(self.generic_test_index_name_2)

        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "abc-123": "some text blah",
            "multiply": 2,
            "add": 1,
            "_id": "my-cool-doc"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc blah",
            "field1": "other things",
            "multiply": 2,
            "add": 1,
            "_id": "123456"
        }

        self.client.index(self.generic_test_index_name).add_documents([d1], tensor_fields=["doc title"], auto_refresh=True)
        self.client.index(self.generic_test_index_name_2).add_documents([d2], tensor_fields=["doc title"], auto_refresh=True)

        for search_method in [enums.SearchMethods.TENSOR, enums.SearchMethods.LEXICAL]:
            resp = self.client.bulk_search([{
                "index": self.generic_test_index_name,
                "q": "blah blah",
                "searchMethod": search_method,
            }, {
                "index": self.generic_test_index_name_2,
                "q": "blah blah",
                "searchMethod": search_method,
            }])
            assert len(resp['result']) == 2
            for search_res in resp['result']:
                assert len(search_res['hits']) == 1

    def test_cloud_fails_to_bulk_search_2_different_indices(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("This test is only for cloud")

        test_index_name_1 = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=None
        )
        test_index_name_2 = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.text_index_with_custom_model,
            open_source_test_index_name=None
        )

        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "abc-123": "some text blah",
            "multiply": 2,
            "add": 1,
            "_id": "my-cool-doc"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc blah",
            "field1": "other things",
            "multiply": 2,
            "add": 1,
            "_id": "123456"
        }

        self.client.index(test_index_name_1).add_documents([d1], tensor_fields=["doc title"], auto_refresh=True)
        self.client.index(test_index_name_2).add_documents([d2], tensor_fields=["doc title"], auto_refresh=True)

        for search_method in [enums.SearchMethods.TENSOR]:
            with self.assertRaises(InvalidArgError):
                resp = self.client.bulk_search([{
                    "index": test_index_name_1,
                    "q": "blah blah",
                    "searchMethod": search_method,
                }, {
                    "index": test_index_name_2,
                    "q": "blah blah",
                    "searchMethod": search_method,
                }])


    @mock_get_indexes_response([GetIndexesIndexResponseObject("test-index", "NOT READY", "example.com")])
    def test_query_validation_index_not_ready_fails(self):
        cloud_client_config = self.client_settings.copy()
        cloud_client_config["url"] = "cloud.url.marqo.ai"
        marqo_cloud_url_before_test = os.environ.get("MARQO_CLOUD_URL")
        os.environ["MARQO_CLOUD_URL"] = "cloud.url.marqo.ai"
        client = marqo.Client(**cloud_client_config)
        queries = [
            {
                "index": "test-index",
                "q": "blah blah",
                "searchMethod": enums.SearchMethods.TENSOR,
            },
            ]
        parsed_queries = [BulkSearchBody(**q) for q in queries]
        with self.assertRaises(marqo.errors.MarqoCloudIndexNotFoundError):
            client._validate_all_indexes_belong_to_the_same_cluster(parsed_queries)

        if marqo_cloud_url_before_test:
            os.environ["MARQO_CLOUD_URL"] = marqo_cloud_url_before_test
