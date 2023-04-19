from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoWebError

from tests.marqo_test import MarqoTestCase


class TestCustomVectorSearch(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError:
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
        except MarqoApiError:
            pass

    def test_custome_vector_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search, q=self.query,
                              context = {"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], })

        custom_res = self.client.index(self.index_name_1).search(q=self.query,
                context = {"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search, q=self.query)

        original_res = self.client.index(self.index_name_1).search(q=self.query)

        del custom_res['processingTimeMs']
        del original_res['processingTimeMs']

        self.assertEqual(custom_res, original_res)

    def test_custom_search_results(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search, q=self.query,
                              context = {"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], })

        custom_res = self.client.index(self.index_name_1).search(q=self.query,
                context = {"tensor": [{"vector": [1, ] * 512, "weight": 0}, {"vector": [2, ] * 512, "weight": 0}], })

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search, q=self.query)

        original_res = self.client.index(self.index_name_1).search(q=self.query)

        original_score = original_res["hits"][0]["_score"]
        custom_score = custom_res["hits"][0]["_score"]

        self.assertEqual(custom_score, original_score)

    def test_custom_vector_search_query_format(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.index_name_1).search, q=self.query,
                                  context={"tensor": [{"vector": [1, ] * 512, "weight": 0},
                                                      {"vector": [2, ] * 512, "weight": 0}], })

            self.client.index(self.index_name_1).search(q=self.query,
                                                                     context={"tensorsss": [
                                                                         {"vector": [1, ] * 512, "weight": 0},
                                                                         {"vector": [2, ] * 512, "weight": 0}], })
            raise AssertionError
        except MarqoWebError:
            pass


