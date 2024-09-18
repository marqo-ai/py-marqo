import copy
import marqo
from marqo import enums
from unittest import mock
import requests
import random
import math
import time
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo.errors import MarqoWebError
from pytest import mark


@mark.fixed
class TestHybridSearch(MarqoTestCase):
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

    def setUp(self):
        super().setUp()
        self.docs_list = [
            # TODO: add score modifiers
            # similar semantics to dogs
            {"_id": "doc1", "text_field_1": "dogs"},
            {"_id": "doc2", "text_field_1": "puppies"},
            {"_id": "doc3", "text_field_1": "canines"},
            {"_id": "doc4", "text_field_1": "huskies"},
            {"_id": "doc5", "text_field_1": "four-legged animals"},

            # shares lexical token with dogs
            {"_id": "doc6", "text_field_1": "hot dogs"},
            {"_id": "doc7", "text_field_1": "dogs is a word"},
            {"_id": "doc8", "text_field_1": "something something dogs"},
            {"_id": "doc9", "text_field_1": "dogs random words"},
            {"_id": "doc10", "text_field_1": "dogs dogs dogs"},

            {"_id": "doc11", "text_field_2": "dogs but wrong field"},
            {"_id": "doc12", "text_field_2": "puppies puppies"},
            {"_id": "doc13", "text_field_2": "canines canines"},
        ]

    def test_hybrid_search_searchable_attributes(self):
        """
        Tests that searchable attributes work as expected for all methods
        """

        index_test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name)    # TODO: add unstructured when supported
        ]
        for cloud_test_index_to_use, open_source_test_index_name in index_test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents(self.docs_list)

            with self.subTest("retrieval: disjunction, ranking: rrf"):
                hybrid_res = self.client.index(test_index_name).search(
                    "puppies",
                    search_method="HYBRID",
                    hybrid_parameters={
                        "retrievalMethod": "disjunction",
                        "rankingMethod": "rrf",
                        "alpha": 0.5,
                        "searchableAttributesLexical": ["text_field_2"],
                        "searchableAttributesTensor": ["text_field_2"]
                    },
                    limit=10
                )
                self.assertEqual(len(hybrid_res["hits"]), 3)  # Only 3 documents have text_field_2 at all
                self.assertEqual(hybrid_res["hits"][0]["_id"], "doc12")  # puppies puppies in text field 2
                self.assertEqual(hybrid_res["hits"][1]["_id"], "doc13")
                self.assertEqual(hybrid_res["hits"][2]["_id"], "doc11")

            with self.subTest("retrieval: lexical, ranking: tensor"):
                hybrid_res = self.client.index(test_index_name).search(
                    "puppies",
                    search_method="HYBRID",
                    hybrid_parameters={
                        "retrievalMethod": "lexical",
                        "rankingMethod": "tensor",
                        "searchableAttributesLexical": ["text_field_2"]
                    },
                    limit=10
                )
                self.assertEqual(len(hybrid_res["hits"]),
                                    1)  # Only 1 document has puppies in text_field_2. Lexical retrieval will only get this one.
                self.assertEqual(hybrid_res["hits"][0]["_id"], "doc12")

            with self.subTest("retrieval: tensor, ranking: lexical"):
                hybrid_res = self.client.index(test_index_name).search(
                    "puppies",
                    search_method="HYBRID",
                    hybrid_parameters={
                        "retrievalMethod": "tensor",
                        "rankingMethod": "lexical",
                        "searchableAttributesTensor": ["text_field_2"]
                    },
                    limit=10
                )
                self.assertEqual(len(hybrid_res["hits"]),
                                    3)  # Only 3 documents have text field 2. Tensor retrieval will get them all.
                # TODO: Put these checks back when lexical search with replicas is consistent.
                # self.assertEqual(hybrid_res["hits"][0]["_id"], "doc12")
                # self.assertEqual(hybrid_res["hits"][1]["_id"], "doc11")
                # self.assertEqual(hybrid_res["hits"][2]["_id"], "doc13")

    def test_hybrid_search_with_custom_vector_query(self):
        """
        Custom Vectory q should work similar to None q with a context vector
        """

        index_test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name)  # TODO: add unstructured when supported
        ]
        for cloud_test_index_to_use, open_source_test_index_name in index_test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents(self.docs_list)
            sample_vector = [0.5 for _ in range(768)]

            res_custom_vector = self.client.index(test_index_name).search(
                q={"customVector": {"content": None, "vector": sample_vector}},
                search_method="HYBRID",
                hybrid_parameters={
                    "retrievalMethod": "tensor",
                    "rankingMethod": "tensor"
                }
            )

            res_context = self.client.index(test_index_name).search(
                q=None,
                search_method="TENSOR",
                context={"tensor": [{"vector": sample_vector, "weight": 1}]}
            )
            self.assertEqual(len(res_custom_vector["hits"]), len(res_context["hits"]))
            for i in range(len(res_custom_vector["hits"])):
                self.assertEqual(res_custom_vector["hits"][i]["_id"], res_context["hits"][i]["_id"])

    def test_hybrid_search_same_retrieval_and_ranking_matches_original_method(self):
        """
        Tests that hybrid search with:
        retrievalMethod = "lexical", rankingMethod = "lexical" and
        retrievalMethod = "tensor", rankingMethod = "tensor"

        Results must be the same as lexical search and tensor search respectively.
        """

        index_test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name)  # TODO: add unstructured when supported
        ]
        for cloud_test_index_to_use, open_source_test_index_name in index_test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents(self.docs_list)

            test_cases = [
                ("lexical", "lexical"),
                ("tensor", "tensor")
            ]

            for retrievalMethod, rankingMethod in test_cases:
                with self.subTest(retrieval=retrievalMethod, ranking=rankingMethod):
                    hybrid_res = self.client.index(test_index_name).search(
                        "dogs",
                        search_method="HYBRID",
                        hybrid_parameters={
                            "retrievalMethod": retrievalMethod,
                            "rankingMethod": rankingMethod
                        },
                        limit=10
                    )

                    base_res = self.client.index(test_index_name).search(
                        "dogs",
                        search_method=retrievalMethod,     # will be either lexical or tensor
                        limit=10
                    )

                    self.assertEqual(len(hybrid_res["hits"]), len(base_res["hits"]))
                    # TODO: Put these checks back when lexical search with replicas is consistent.
                    #for i in range(len(hybrid_res["hits"])):
                    #    self.assertEqual(hybrid_res["hits"][i]["_id"], base_res["hits"][i]["_id"])

    def test_hybrid_search_with_filter(self):
        """
        Tests that filter is applied correctly in hybrid search.
        """

        index_test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name)  # TODO: add unstructured when supported
        ]
        for cloud_test_index_to_use, open_source_test_index_name in index_test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents(self.docs_list)

            test_cases = [
                ("disjunction", "rrf"),
                ("lexical", "lexical"),
                ("tensor", "tensor")
            ]

            for retrievalMethod, rankingMethod in test_cases:
                with self.subTest(retrieval=retrievalMethod, ranking=rankingMethod):
                    hybrid_res = self.client.index(test_index_name).search(
                        "dogs",
                        search_method="HYBRID",
                        filter_string="text_field_1:(something something dogs)",
                        hybrid_parameters={
                            "retrievalMethod": retrievalMethod,
                            "rankingMethod": rankingMethod
                        },
                        limit=10
                    )

                    self.assertEqual(len(hybrid_res["hits"]), 1)
                    self.assertEqual(hybrid_res["hits"][0]["_id"], "doc8")

    def test_hybrid_search_structured_rrf_with_replicas_has_no_duplicates(self):
        """
        Tests that show that running 100 searches on indexes with 3 replicas (structured text & unstructured text)
        will not have duplicates in results.
        Only relevant for cloud tests.
        """

        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")

        # Split into 2 separate blocks to unblock (looping error occurring)
        cloud_test_index_to_use = CloudTestIndex.structured_text
        test_index_name = self.get_test_index_name(
            cloud_test_index_to_use=cloud_test_index_to_use,
            open_source_test_index_name=None
        )
        print(f"Running test for index: {test_index_name}", flush=True)
        add_docs_res = self.client.index(test_index_name).add_documents(self.docs_list)
        print(f"Add docs result: {add_docs_res}", flush=True)
        for _ in range(100):
            hybrid_res = self.client.index(test_index_name).search(
                "dogs",
                search_method="HYBRID",
                limit=10
            )

            # check for duplicates
            hit_ids = [hit["_id"] for hit in hybrid_res["hits"]]
            self.assertEqual(len(hit_ids), len(set(hit_ids)),
                             f"Duplicates found in results. Only {len(set(hit_ids))} unique results out of "
                             f"{len(hit_ids)}")

    def test_hybrid_search_unstructured_rrf_with_replicas_has_no_duplicates(self):
        """
        Tests that show that running 100 searches on indexes with 3 replicas (structured text & unstructured text)
        will not have duplicates in results.
        Only relevant for cloud tests.
        """

        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")

        cloud_test_index_to_use = CloudTestIndex.unstructured_text
        test_index_name = self.get_test_index_name(
            cloud_test_index_to_use=cloud_test_index_to_use,
            open_source_test_index_name=None
        )
        print(f"Running test for index: {test_index_name}", flush=True)
        add_docs_res = self.client.index(test_index_name).add_documents(
            self.docs_list,
            tensor_fields=["text_field_1", "text_field_2", "text_field_3"]
        )
        print(f"Add docs result: {add_docs_res}", flush=True)
        for _ in range(100):
            hybrid_res = self.client.index(test_index_name).search(
                "dogs",
                search_method="HYBRID",
                limit=10
            )

            # check for duplicates
            hit_ids = [hit["_id"] for hit in hybrid_res["hits"]]
            self.assertEqual(len(hit_ids), len(set(hit_ids)),
                             f"Duplicates found in results. Only {len(set(hit_ids))} unique results out of "
                             f"{len(hit_ids)}")