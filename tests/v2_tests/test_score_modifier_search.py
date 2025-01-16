from typing import Any, Dict, List, Optional

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


@mark.fixed
class TestScoreModifierSearch(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            self.test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=self.test_index_name).add_documents(
                documents=[
                    {"my_text_field": "A rider is riding a horse jumping over the barrier.",
                     "my_image_field": "https://marqo-assets.s3.amazonaws.com/tests/images/image2.jpg",
                     # 4 fields
                     "multiply_1": 1,
                     "multiply_2": {"a": 20.0},
                     "add_1": 1.0,
                     "add_2": {"a": 30.0},
                     "_id": "1"
                     },
                    {"my_text_field": "A rider is riding a horse jumping over the barrier.",
                     "my_image_field": "https://marqo-assets.s3.amazonaws.com/tests/images/image2.jpg",
                     "_id": "0",
                     "filter": "original"
                     },
                ],
                tensor_fields=["my_image_field", "my_text_field"]
            )
            self.query = "what is the rider doing?"
    
    def search_with_score_modifier(self, score_modifiers: Optional[Dict[str, List[Dict[str, Any]]]] = None, **kwargs) -> Dict[str, Any]:
        return self.client.index(self.test_index_name).search(
            q = self.query,
            score_modifiers = score_modifiers,
            **kwargs
        )

    def test_score_modifier_search_results(self):
        score_modifiers = {
                # miss one weight
                "multiply_score_by":
                    [{"field_name": "multiply_1",
                      "weight": 1,},
                     {"field_name": "multiply_2.a",}],
                "add_to_score": [
                    {"field_name": "add_1", "weight" : -3,
                     },
                    {"field_name": "add_2.a", "weight": 1,
                     }]
            }

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_score_modifier(score_modifiers=None, filter_string="filter:original"))
        
        original_res = self.search_with_score_modifier(score_modifiers=None, filter_string="filter:original")
        original_score = original_res["hits"][0]["_score"]

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_score_modifier(score_modifiers=score_modifiers))
        modifiers_res = self.search_with_score_modifier(score_modifiers=score_modifiers)

        modifiers_score = modifiers_res["hits"][0]["_score"]
        expected_sore = original_score * 20 * 1 + 1 * -3 + 30 * 1
        assert abs(expected_sore -modifiers_score) < 1e-5

    def test_invalid_score_modifiers_format(self):
        invalid_score_modifiers = {
                # typo in multiply score by
                "multiply_score_bys":
                    [{"field_name": "multiply_1",
                      "weight": 1,},
                     {"field_name": "multiply_2.a",}],
                "add_to_score": [
                    {"field_name": "add_1", "weight" : 4,
                     },
                    {"field_name": "add_2.a", "weight": 1,
                     }]
            }

        try:
            self.search_with_score_modifier(score_modifiers=invalid_score_modifiers)
            raise AssertionError
        except MarqoWebError:
            pass

    def test_valid_score_modifiers_format(self):
        valid_score_modifiers = {
                # missing one part
                "add_to_score": [
                    {"field_name": "add_1", "weight" : -3,
                     },
                    {"field_name": "add_2.a", "weight": 1,
                     }]
            }
        self.search_with_score_modifier(score_modifiers=valid_score_modifiers)


@mark.fixed
class TestScoreModifierWithRerankCountSearch(MarqoTestCase):
    def test_hybrid_search_rrf_score_modifiers_with_rerank_count(self):
        """
        Test that hybrid search with RRF can use root level score_modifiers and rerank_count
        """
        test_cases = [
            (CloudTestIndex.unstructured_text, self.unstructured_index_name),
            (CloudTestIndex.structured_text, self.structured_index_name)
        ]

        docs_list = [
            {"_id": "both1", "text_field_1": "dogs", "int_field_1": -1},           # HIGH tensor, LOW lexical
            {"_id": "tensor1", "text_field_1": "puppies", "int_field_1": 2},         # MID tensor
            {"_id": "tensor2", "text_field_1": "random words", "int_field_1": 3},    # LOW tensor
        ]

        for cloud_test_index_to_use, open_source_test_index_name in test_cases:
            with self.subTest(cloud_test_index_to_use=cloud_test_index_to_use,
                              open_source_test_index_name=open_source_test_index_name):
                test_index_name = self.get_test_index_name(
                    cloud_test_index_to_use=cloud_test_index_to_use,
                    open_source_test_index_name=open_source_test_index_name
                )
                self.client.index(test_index_name).add_documents(
                    docs_list,
                    tensor_fields=["text_field_1"] if "unstr" in cloud_test_index_to_use or
                                                      "unstr" in open_source_test_index_name else None)

                # Get unmodified scores
                # Unmodified result order should be: both1, tensor1, tensor2
                unmodified_results = self.client.index(test_index_name).search(q="dogs", search_method="HYBRID",limit=3)
                unmodified_scores = {hit["_id"]: hit["_score"] for hit in unmodified_results["hits"]}
                self.assertEqual(["both1", "tensor1", "tensor2"], [hit["_id"] for hit in unmodified_results["hits"]])

                # Get modified scores (rank all 3)
                # Modified result order should be: tensor2, tensor1, both1
                score_modifiers = {
                    "multiply_score_by": [
                        {"field_name": "int_field_1", "weight": 1}
                    ],
                    "add_to_score": [
                        {"field_name": "int_field_1", "weight": 1}
                    ]
                }
                modified_results = self.client.index(test_index_name).search(
                    q="dogs", search_method="HYBRID",
                    limit=3, rerank_count=3, score_modifiers=score_modifiers
                )
                self.assertEqual(["tensor2", "tensor1", "both1"], [hit["_id"] for hit in modified_results["hits"]])
                self.assertAlmostEqual(modified_results["hits"][0]["_score"], 3*unmodified_scores["tensor2"] + 3)
                self.assertAlmostEqual(modified_results["hits"][1]["_score"], 2*unmodified_scores["tensor1"] + 2)
                self.assertAlmostEqual(modified_results["hits"][2]["_score"], -1*unmodified_scores["both1"] - 1)

                # Get modified scores (rank only 1). Only both1 should be rescored (goes to the bottom)
                # Modified result order should be: tensor1, tensor2, both1
                modified_results = self.client.index(test_index_name).search(
                    q="dogs", search_method="HYBRID",
                    limit=3, rerank_count=1, score_modifiers=score_modifiers
                )
                self.assertEqual(["tensor1", "tensor2", "both1"], [hit["_id"] for hit in modified_results["hits"]])
                self.assertAlmostEqual(modified_results["hits"][0]["_score"], unmodified_scores["tensor1"])     # unmodified
                self.assertAlmostEqual(modified_results["hits"][1]["_score"], unmodified_scores["tensor2"])     # unmodified
                self.assertAlmostEqual(modified_results["hits"][2]["_score"], -1*unmodified_scores["both1"] - 1)    # modified