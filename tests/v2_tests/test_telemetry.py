from marqo.client import Client
from tests.marqo_test import MarqoTestCase, CloudTestIndex
import math
from pytest import mark


@mark.fixed
class TestTelemetry(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.client = Client(**self.client_settings, return_telemetry=True)

    def test_telemetry_add_documents(self):
        number_of_docs = 10
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            doc = [{"Title": "Marqo is useful",
                    "Description": "Marqo is a very useful tool"}, ] * number_of_docs

            kwargs_list = [
                {"documents": doc, "client_batch_size": None, "tensor_fields": ["Title", "Description"]},
                {"documents": doc, "client_batch_size": 5, "tensor_fields": ["Title", "Description"]},
                {"documents": doc, "client_batch_size": None, "tensor_fields": ["Title", "Description"]},
                {"documents": doc, "client_batch_size": 2, "tensor_fields": ["Title"]},
                {"documents": doc, "client_batch_size": 3, "tensor_fields": ["Description"]},
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
        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            search_kwargs_list = [
                {"q": "marqo is good"},
                {"q": "good search", "limit": 5,},
                {"q": "try search me", "filter_string": "text_field_1:(dummy)"},
                {"q": "try search me", "show_highlights": False},
                {"q": "search query", "search_method": "LEXICAL"},
                {"q": "search query", "searchable_attributes": ["text_field_1"]},
            ]

            self.client.index(test_index_name).add_documents([{"text_field_1": "A dummy document",}])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search, **search_kwargs_list[0])

            for kwargs in search_kwargs_list:
                res = self.client.index(test_index_name).search(**kwargs)
                self.assertIn("telemetry", res)
                self.assertIn("timesMs", res["telemetry"])

    def test_telemetry_get_document(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents([{"_id": "123321", "Title": "Marqo is useful",}],
                                                               tensor_fields=["Title"])
            res = self.client.index(test_index_name).get_document("123321")
            self.assertIn("telemetry", res)
            self.assertEqual(res["telemetry"], dict())

    def test_delete_documents(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents([{"_id": "123321", "Title": "Marqo is useful",}],
                                                               tensor_fields=["Title"])
            res = self.client.index(test_index_name).delete_documents(["123321"])
            self.assertIn("telemetry", res)
            self.assertEqual(res["telemetry"], dict())


