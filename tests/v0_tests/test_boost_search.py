from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase


class TestBoostSearch(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
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
            ],
            tensor_fields=["Title", "Description"]
        )

        self.query = "What are the best pets"

    def test_boost_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q= self.query, boost = {"Title": [1,0], "Description" : [1,0]})

        boost_res = self.client.index(self.test_index_name).search(q= self.query, boost = {"Title": [1,0], "Description" : [1,0]})

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q= self.query, boost = {"void": [10,10], "void2" : [10,20]})

        no_matched_res = self.client.index(self.test_index_name).search(q= self.query, boost = {"void": [10,10], "void2" : [10,20]})
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q= self.query)

        res = self.client.index(self.test_index_name).search(q= self.query)

        del boost_res['processingTimeMs']
        del res['processingTimeMs']
        del no_matched_res['processingTimeMs']

        self.assertEqual(boost_res, res)
        self.assertEqual(no_matched_res, res)


    def test_boost_search_results(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q=self.query,
                                                                     boost={"Title": [1, 1]})
        
        boost_res = self.client.index(self.test_index_name).search(q=self.query,
                                                                     boost={"Title": [1, 1]})
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q=self.query)

        res = self.client.index(self.test_index_name).search(q=self.query)

        boost_score = boost_res["hits"][0]["_score"]
        res_score_positive = res["hits"][0]["_score"]

        self.assertEqual(boost_score, res_score_positive * 1 + 1)


    def test_boost_search_query_format(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.test_index_name).search,q=self.query,
                                                                         boost=["Title", [1,2]])
                
            boost_res = self.client.index(self.test_index_name).search(q=self.query,
                                                                         boost=["Title", [1,2]])
            raise AssertionError
        except MarqoWebError:
            pass


    def test_boost_search_searchable_attributes_mismatch(self):
        try:
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.test_index_name).search,q=self.query,
                                                                         boost={"Title": [1, 1]}
                                                                         ,searchable_attributes=["Description"])
                
            boost_res = self.client.index(self.test_index_name).search(q=self.query,
                                                                         boost={"Title": [1, 1]}
                                                                         ,searchable_attributes=["Description"])
            raise AssertionError
        except MarqoWebError:
            pass

