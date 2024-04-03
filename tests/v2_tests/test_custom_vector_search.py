from typing import Any, Dict, List, Optional

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


@mark.fixed
class TestCustomVectorSearch(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.test_cases = [
            (CloudTestIndex.unstructured_image, self.unstructured_image_index_name)
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            open_source_test_index_name = self.unstructured_image_index_name

            self.test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=self.test_index_name).add_documents(
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
                ], tensor_fields=["Title", "Description"]
            )
        self.vector_dim = 512

        self.query = {"What are the best pets": 1}

    def search_with_context(self, context_vector: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
        return self.client.index(self.test_index_name).search(
            q=self.query,
            context=context_vector
        )

    def test_custom_vector_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context({"tensor": [
                {"vector": [1, ] * self.vector_dim, "weight": 0}, {"vector": [2, ] * self.vector_dim, "weight": 0}], }))

        custom_res = self.search_with_context({"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 0},
                                                          {"vector": [2, ] * self.vector_dim, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context())

        original_res = self.search_with_context()

        original_res.pop('processingTimeMs', None)
        custom_res.pop('processingTimeMs', None)

        self.assertEqual(custom_res, original_res)

    def test_custom_search_results(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context({"tensor": [
                {"vector": [1, ] * self.vector_dim, "weight": 0}, {"vector": [2, ] * self.vector_dim, "weight": 0}], }))

        custom_res = self.search_with_context({"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 0},
                                                          {"vector": [2, ] * self.vector_dim, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context())

        original_res = self.search_with_context()

        original_score = original_res["hits"][0]["_score"]
        custom_score = custom_res["hits"][0]["_score"]

        self.assertEqual(custom_score, original_score)

    def test_custom_vector_search_query_format(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(lambda: self.search_with_context({
                    "tensor": [
                        {"vector": [1, ] * self.vector_dim, "weight": 0},
                        {"vector": [2, ] * self.vector_dim, "weight": 0}
                    ],
                }))

            self.search_with_context({
                "tensorss": [
                    {"vector": [1, ] * self.vector_dim, "weight": 0},
                    {"vector": [2, ] * self.vector_dim, "weight": 0}
                ],
            })
            raise AssertionError
        except MarqoWebError:
            pass

    def test_context_dimension_have_different_dimensions_to_index(self):
        correct_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1}]}
        wrong_context = {"tensor": [{"vector": [1, ] * 2, "weight": 1}]}
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context(correct_context))
        with self.assertRaises(MarqoWebError) as e:
            self.search_with_context(wrong_context)
        self.assertIn("The dimension of the vectors returned by the model or given by the context "
                      "vectors does not match the expected dimension", str(e.exception))

    def test_context_dimension_have_inconsistent_dimensions(self):
        correct_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1},
                                      {"vector": [2, ] * self.vector_dim, "weight": 0}]}
        wrong_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1},
                                    {"vector": [2, ] * (self.vector_dim + 1), "weight": 0}]}
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context(correct_context))
        with self.assertRaises(MarqoWebError) as e:
            self.search_with_context(wrong_context)
        self.assertIn("The dimension of the vectors returned by the model or given by the context "
                      "vectors does not match the expected dimension", str(e.exception))

    def test_context_vector_with_flat_query(self):
        self.query = "What are the best pets"
        context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1},
                              {"vector": [2, ] * self.vector_dim, "weight": 0}]}
        try:
            result = self.search_with_context(context)
            raise AssertionError(f"The query should not be accepted. Returned: {result}")
        except MarqoWebError as e:
            assert "This is not supported as the context only works when the query is a dictionary." in str(e)
        finally:

            ## Ensure other tests are not affected
            self.query = {"What are the best pets": 1}
