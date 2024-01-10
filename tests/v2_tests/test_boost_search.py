from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


class TestBoostSearch(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()

        if not self.client.config.is_marqo_cloud:
            self.create_open_source_indexes([
                {
                    "indexName": self.unstructured_index_name,
                    "type": "unstructured"
                },
                {
                    "indexName": self.structured_index_name,
                    "type": "structured",
                    "allFields": [{"name": "text_field_1", "type": "text"},
                                  {"name": "text_field_2", "type": "text"},
                                  {"name": "text_field_3", "type": "text"}],
                    "tensorFields": ["text_field_1", "text_field_2", "text_field_3"]
                }
            ])
        self.test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=CloudTestIndex.unstructured_image,
                open_source_test_index_name=self.unstructured_index_name
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
            ],
            tensor_fields=["Title", "Description"],
        )

        self.query = "What are the best pets"

    def test_boost_search_format(self):
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q= self.query, boost = {"Title": [1,0], "Description" : [1,0]})
        try:
            boost_res = self.client.index(self.test_index_name).search(q= self.query, boost = {"Title": [1,0], "Description" : [1,0]})
        except MarqoWebError as e:
            assert "Boosting is not currently supported with Vespa" in e.message["message"]

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q= self.query, boost = {"void": [10,10], "void2" : [10,20]})

        try:
            no_matched_res = self.client.index(self.test_index_name).search(q= self.query, boost = {"void": [10,10], "void2" : [10,20]})
        except MarqoWebError as e:
            assert "Boosting is not currently supported with Vespa" in e.message["message"]
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,q= self.query)

        res = self.client.index(self.test_index_name).search(q= self.query)

        del res['processingTimeMs']

    # TODO: Fix test when boost is supported
    # def test_boost_search_results(self):
    #     if self.IS_MULTI_INSTANCE:
    #         self.warm_request(self.client.index(self.test_index_name).search,q=self.query,
    #                                                                  boost={"Title": [1, 1]})
    #
    #     boost_res = self.client.index(self.test_index_name).search(q=self.query,
    #                                                                  boost={"Title": [1, 1]})
    #
    #     if self.IS_MULTI_INSTANCE:
    #         self.warm_request(self.client.index(self.test_index_name).search,q=self.query)
    #
    #     res = self.client.index(self.test_index_name).search(q=self.query)
    #
    #     boost_score = boost_res["hits"][0]["_score"]
    #     res_score_positive = res["hits"][0]["_score"]
    #
    #     self.assertEqual(boost_score, res_score_positive * 1 + 1)


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

