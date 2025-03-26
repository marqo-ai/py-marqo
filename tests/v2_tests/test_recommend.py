from pytest import mark

from marqo.enums import InterpolationMethod
from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase
from tests.cloud_test_logic.cloud_test_index import CloudTestIndex

@mark.fixed
class TestRecommend(MarqoTestCase):

    def test_recommend_defaults(self):
        """
        Test recommend with only required fields provided
        """
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            docs = [
                {
                    "_id": "1",
                    "Title": "Red orchid",
                    "tags": ["flower", "orchid"],
                },
                {
                    "_id": "2",
                    "Title": "Red rose",
                    "tags": ["flower"],
                },
                {
                    "_id": "3",
                    "Title": "Europe",
                    "tags": ["continent"],
                },
            ]

            add_docs_results = self.client.index(test_index_name).add_documents(docs, tensor_fields=["Title"])

            if add_docs_results["errors"]:
                raise Exception(f"Failed to add documents to index {test_index_name}")

            res = self.client.index(test_index_name).recommend(
                documents=['1', '2']
            )

            ids = [doc["_id"] for doc in res["hits"]]

            self.assertEqual(set(ids), {"3"})

    def test_recommend_allFields(self):
        """
        Test recommend with all fields provided
        """

        self.test_cases = [(CloudTestIndex.structured_text, self.structured_index_name), ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            docs = [
                {
                    "_id": "1",
                    "text_field_1": "Red orchid",
                    "text_field_2": "flower",
                },
                {
                    "_id": "2",
                    "text_field_1": "Red rose",
                    "text_field_2": "flower"
                },
                {
                    "_id": "3",
                    "text_field_1": "Europe",
                    "text_field_2": "continent",
                }
            ]

            add_docs_results = self.client.index(test_index_name).add_documents(docs)

            if add_docs_results["errors"]:
                raise Exception(f"Failed to add documents to index {test_index_name}")

            res = self.client.index(test_index_name).recommend(
                documents=['1', '2'],
                tensor_fields=["text_field_1"],
                interpolation_method=InterpolationMethod.SLERP,
                exclude_input_documents=True,
                limit=10,
                offset=0,
                ef_search=100,
                approximate=True,
                searchable_attributes=["text_field_1"],
                show_highlights=True,
                # reranker='google/owlvit-base-patch32', can't rerank text
                filter_string='text_field_2:(continent)',
                attributes_to_retrieve=["text_field_1"],
                score_modifiers={
                    "multiply_score_by":
                        [
                            {
                                "field_name": "int_filter_field_1",
                                "weight": 1
                            }
                        ]
                }
            )

            ids = [doc["_id"] for doc in res["hits"]]

            self.assertEqual(set(ids), {"3"})

    def test_recommend_rerank_depth_behavior(self):
        """API-level test that rerank_depth affects recommender results according to expected behavior."""
        self.test_cases = [(CloudTestIndex.structured_text, self.structured_index_name)]
        docs = [{
            "_id": str(i),
            "text_field_1": f"Document {i}",
            "text_field_2": "test content"
        } for i in range(10)]

        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            self.client.index(test_index_name).add_documents(docs)

            base_kwargs = dict(
                documents=["1", "2"],
                tensor_fields=["text_field_1"],
                interpolation_method=InterpolationMethod.SLERP,
                searchable_attributes=["text_field_1"],
                exclude_input_documents=True,
                attributes_to_retrieve=["text_field_1"],
                show_highlights=True,
                approximate=True
            )

            # Case 1: result_count < rerank_depth → limit is respected
            with self.subTest(case="limit_less_than_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    **base_kwargs, limit=3, offset=0, rerank_depth=5
                )
                self.assertEqual(len(res["hits"]), 3)

            # Case 2: rerank_depth < offset + result_count → offset respected
            with self.subTest(case="offset_beyond_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    **base_kwargs, limit=2, offset=4, rerank_depth=3
                )
                self.assertGreaterEqual(len(res["hits"]), 1)

            # Case 3: offset + limit <= rerank_depth → all hits returned
            with self.subTest(case="offset_within_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    **base_kwargs, limit=2, offset=2, rerank_depth=5
                )
                self.assertEqual(len(res["hits"]), 2)

            # Case 4: result_count > rerank_depth → rerank_depth is ignored, limit is respected
            with self.subTest(case="limit_exceeds_rerank_depth"):
                res = self.client.index(test_index_name).recommend(
                    **base_kwargs, limit=5, offset=0, rerank_depth=3
                )
                self.assertEqual(len(res["hits"]), 5)

            # Case 5: ef_search < rerank_depth → ef_search becomes the rerank limit
            with self.subTest(case="ef_search_limits_rerank_pool"):
                res = self.client.index(test_index_name).recommend(
                    **base_kwargs, limit=10, offset=0, rerank_depth=5, ef_search=2
                )
                self.assertEqual(len(res["hits"]), 2)

            # Case 6: rerank_depth is negative → should raise MarqoWebError
            with self.subTest(case="invalid_negative_rerank_depth"):
                with self.assertRaises(MarqoWebError):
                    self.client.index(test_index_name).recommend(
                        **base_kwargs, limit=10, offset=0, rerank_depth=-1
                    )
