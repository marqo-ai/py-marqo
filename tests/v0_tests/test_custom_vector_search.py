from typing import Any, Dict, List, Optional

from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoWebError
from tests.marqo_test import MarqoTestCase


class TestCustomVectorSearch(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass
        self.client.create_index(index_name=self.index_name_1, model="ViT-B/32")
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
            ]
        )

        self.query = {"What are the best pets": 1}

    def tearDown(self) -> None:
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def search_with_context(self, context_vector: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
        return self.client.index(self.index_name_1).search(
            q=self.query,
            context = context_vector
        )

    def test_custom_vector_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda _: self.search_with_context({"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], }))

        custom_res = self.search_with_context({"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda _: self.search_with_context())

        original_res = self.search_with_context()
        
        original_res.pop('processingTimeMs', None)
        custom_res.pop('processingTimeMs', None)

        self.assertEqual(custom_res, original_res)

    def test_custom_search_results(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda _: self.search_with_context({"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], }))
            
        custom_res = self.search_with_context({"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda _: self.search_with_context())

        original_res = self.search_with_context()

        original_score = original_res["hits"][0]["_score"]
        custom_score = custom_res["hits"][0]["_score"]

        self.assertEqual(custom_score, original_score)

    def test_custom_vector_search_query_format(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(lambda _: self.search_with_context({
                "tensor": [
                    {"vector": [1, ] * 512, "weight": 0},
                    {"vector": [2, ] * 512, "weight": 0}
                ], 
            }))

            self.search_with_context({
                "tensorss": [
                    {"vector": [1, ] * 512, "weight": 0},
                    {"vector": [2, ] * 512, "weight": 0}
                ], 
            })
            raise AssertionError
        except MarqoWebError:
            pass

    def test_context_dimension_have_different_dimensions_to_index(self):
         correct_context = {"tensor": [{"vector": [1, ] * 384, "weight": 1}]}
         wrong_context = {"tensor": [{"vector": [1, ] * 2, "weight": 1}]}
         if self.IS_MULTI_INSTANCE:
             self.warm_request(lambda _: self.search_with_context(correct_context))
         try:
             self.search_with_context(wrong_context)
             raise AssertionError
         except MarqoWebError as e:
            assert "The provided vectors are not in the same dimension of the index" in str(e)

    def test_context_dimension_have_inconsistent_dimensions(self):
         correct_context = {"tensor": [{"vector": [1, ] * 384, "weight": 1}, {"vector": [2, ] * 384, "weight": 0}]}
         wrong_context = {"tensor": [{"vector": [1, ] * 384, "weight": 1}, {"vector": [2, ] * 385, "weight": 0}]}
         if self.IS_MULTI_INSTANCE:
             self.warm_request(lambda _: self.search_with_context(correct_context))
         try:
             self.search_with_context(wrong_context)
             raise AssertionError
         except MarqoWebError as e:
            assert "The provided vectors are not in the same dimension of the index" in str(e)

    def test_context_vector_with_flat_query(self):
        self.query = "What are the best pets"
        context = {"tensor": [{"vector": [1, ] * 384, "weight": 1}, {"vector": [2, ] * 384, "weight": 0}]}
        try:
            result = self.search_with_context(context)
            raise AssertionError(f"The query should not be accepted. Returned: {result}")
        except MarqoWebError as e:
            assert "This is not supported as the context only works when the query is a dictionary." in str(e)
        finally:

            ## Ensure other tests are not affected
            self.query = {"What are the best pets": 1}

class TestCustomBulkVectorSearch(TestCustomVectorSearch):

    def search_with_context(self, context_vector: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
        resp = self.client.bulk_search([{
            "index": self.index_name_1,
            "q": self.query,
            "context": context_vector
        }])
        if len(resp.get("result", [])) > 0:
            return resp['result'][0]
        return {}

