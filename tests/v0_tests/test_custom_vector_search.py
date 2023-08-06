from typing import Any, Dict, List, Optional

from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoWebError
from tests.marqo_test import MarqoTestCase
from pytest import mark


class TestCustomVectorSearch(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name, model="ViT-B/32")
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
            context = context_vector
        )

    def test_custom_vector_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context({"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 0}, {"vector": [2, ] * self.vector_dim, "weight": 0}], }))

        custom_res = self.search_with_context({"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 0}, {"vector": [2, ] * self.vector_dim, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context())

        original_res = self.search_with_context()
        
        original_res.pop('processingTimeMs', None)
        custom_res.pop('processingTimeMs', None)

        self.assertEqual(custom_res, original_res)

    def test_custom_search_results(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context({"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 0}, {"vector": [2, ] * self.vector_dim, "weight": 0}], }))
            
        custom_res = self.search_with_context({"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 0}, {"vector": [2, ] * self.vector_dim, "weight": 0}], })

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
         try:
             self.search_with_context(wrong_context)
             raise AssertionError
         except MarqoWebError as e:
            assert "The provided vectors are not in the same dimension of the index" in str(e)

    def test_context_dimension_have_inconsistent_dimensions(self):
         correct_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1}, {"vector": [2, ] * self.vector_dim, "weight": 0}]}
         wrong_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1}, {"vector": [2, ] * (self.vector_dim + 1), "weight": 0}]}
         if self.IS_MULTI_INSTANCE:
             self.warm_request(lambda: self.search_with_context(correct_context))
         try:
             self.search_with_context(wrong_context)
             raise AssertionError
         except MarqoWebError as e:
            assert "The provided vectors are not in the same dimension of the index" in str(e)

    def test_context_vector_with_flat_query(self):
        self.query = "What are the best pets"
        context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1}, {"vector": [2, ] * self.vector_dim, "weight": 0}]}
        try:
            result = self.search_with_context(context)
            raise AssertionError(f"The query should not be accepted. Returned: {result}")
        except MarqoWebError as e:
            assert "This is not supported as the context only works when the query is a dictionary." in str(e)
        finally:

            ## Ensure other tests are not affected
            self.query = {"What are the best pets": 1}


@mark.ignore_cloud_tests
class TestCustomBulkVectorSearch(TestCustomVectorSearch):

    def search_with_context(self, context_vector: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
        resp = self.client.bulk_search([{
            "index": self.test_index_name,
            "q": self.query,
            "context": context_vector
        }])
        if len(resp.get("result", [])) > 0:
            return resp['result'][0]
        return {}

    def test_context_dimension_error_in_bulk_search(self):
        correct_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1}, {"vector": [2, ] * self.vector_dim, "weight": 0}]}
        wrong_context = {"tensor": [{"vector": [1, ] * (self.vector_dim + 2), "weight": 1}, {"vector": [2, ] * (self.vector_dim + 3), "weight": 0}]}
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.bulk_search, [{
                "index": self.test_index_name,
                "q": {"blah blah" :1},
                "context": correct_context,
            }])
        try:
            self.client.bulk_search([{
                "index": self.test_index_name,
                "q": {"blah blah": 1},
                "context": wrong_context, # the dimension mismatches the index
            }])
            raise AssertionError
        except MarqoWebError as e:
            assert "The provided vectors are not in the same dimension of the index" in str(e)

    def test_context_with_query_string_in_bulk_search(self):
        correct_context = {"tensor": [{"vector": [1, ] * self.vector_dim, "weight": 1}, {"vector": [2, ] * self.vector_dim, "weight": 0}]}
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.bulk_search, [{
                "index": self.test_index_name,
                "q": {"blah blah" :1},
                "context": correct_context,
            }])
        try:
            self.client.bulk_search([{
                "index": self.test_index_name,
                "q": "blah blah",
                "context": correct_context, # the dimension mismatches the index
            }])
            raise AssertionError
        except MarqoWebError as e:
            assert "This is not supported as the context only works when the query is a dictionary." in str(e)