from marqo.client import Client
from tests.marqo_test import MarqoTestCase
import math
from pytest import mark


class TestTelemetry(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.client = Client(**self.client_settings, return_telemetry=True)

    def test_telemetry_add_documents(self):
        number_of_docs = 10
        test_index_name = self.create_test_index(self.generic_test_index_name)
        doc = [{"Title": "Marqo is useful",
                "Description": "Marqo is a very useful tool"}, ] * number_of_docs

        kwargs_list = [
            {"documents": doc, "auto_refresh": True, "client_batch_size": None, "non_tensor_fields": []},
            {"documents": doc, "auto_refresh": True, "client_batch_size": 5, "non_tensor_fields": []},
            {"documents": doc, "auto_refresh": False, "client_batch_size": None, "non_tensor_fields": []},
            {"documents": doc, "auto_refresh": True, "client_batch_size": 2, "non_tensor_fields": ["Description"]},
            {"documents": doc, "auto_refresh": False, "client_batch_size": 3, "non_tensor_fields": ["Title"]},
        ]

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(test_index_name).add_documents, **kwargs_list[0])

        for kwargs in kwargs_list:
            res = self.client.index(test_index_name).add_documents(**kwargs)
            if kwargs["client_batch_size"] is not None:
                assert len(res) == math.ceil(float(number_of_docs) / kwargs["client_batch_size"])
                assert all(["telemetry" in i for i in res])
            else:
                assert isinstance(res, dict)
                assert "telemetry" in res

    def test_telemetry_search(self):
        search_kwargs_list = [
            {"q": "marqo is good"},
            {"q": "good search","limit": 5,},
            {"q": "try search me","filter_string": "filter expression"},
            {"q": "try search me","show_highlights": False},
            {"q": "search query","search_method": "LEXICAL"},
            {"q": "search query","searchable_attributes": ["Description"]}]

        test_index_name = self.create_test_index(self.generic_test_index_name)
        self.client.index(test_index_name).add_documents([{"Title": "A dummy document",}], tensor_fields=["Title"])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(test_index_name).search, **search_kwargs_list[0])

        for kwargs in search_kwargs_list:
            res = self.client.index(test_index_name).search(**kwargs)
            self.assertIn("telemetry", res)
            self.assertIn("timesMs", res["telemetry"])

    @mark.ignore_during_cloud_tests
    def test_telemetry_bulk_search(self):
        test_index_name = self.create_test_index(self.generic_test_index_name)
        self.client.index(test_index_name).add_documents([{"Title": "A dummy document",}], tensor_fields=["Title"])
        bulk_search_query = [
            {
                "index": test_index_name,
                "q": "what is the best outfit to wear on the moon?",
                "searchableAttributes": ["Description"],
                "limit": 10,
                "offset": 0,
                "showHighlights": True,
                "filter": "*:*",
                "searchMethod": "TENSOR",
                "attributesToRetrieve": ["Title", "Description"]
            },
            {
                "index": test_index_name,
                "attributesToRetrieve": ["_id"],
                "q": {"what is the best outfit to wear on mars?": 0.5, "what is the worst outfit to wear on mars?": 0.3}
            }]

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.bulk_search, bulk_search_query)

        res = self.client.bulk_search(bulk_search_query)
        self.assertIn("telemetry", res)
        self.assertIn("timesMs", res["telemetry"])

        expected_fields = ['bulk_search.vector_inference_full_pipeline',
                           'bulk_search.vector.processing_before_opensearch',
                           'search.opensearch._msearch', 'search.opensearch._msearch.internal',
                           'bulk_search.vector.postprocess', 'bulk_search.rerank', 'POST /indexes/bulk/search']
        assert all([field in res["telemetry"]["timesMs"] for field in expected_fields])

    def test_telemetry_get_document(self):
        test_index_name = self.create_test_index(self.generic_test_index_name)
        self.client.index(test_index_name).add_documents([{"_id": "123321", "Title": "Marqo is useful",}],
                                                           tensor_fields=["Title"])
        res = self.client.index(test_index_name).get_document("123321")
        self.assertIn("telemetry", res)
        self.assertEqual(res["telemetry"], dict())

    def test_delete_documents(self):
        test_index_name = self.create_test_index(self.generic_test_index_name)
        self.client.index(test_index_name).add_documents([{"_id": "123321", "Title": "Marqo is useful",}],
                                                           tensor_fields=["Title"])
        res = self.client.index(test_index_name).delete_documents(["123321"])
        self.assertIn("telemetry", res)
        self.assertEqual(res["telemetry"], dict())


