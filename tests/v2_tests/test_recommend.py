from pytest import mark

from marqo.enums import InterpolationMethod
from tests.marqo_test import MarqoTestCase


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
                    "title": "Red orchid",
                    "tags": ["flower", "orchid"],
                },
                {
                    "_id": "2",
                    "title": "Red rose",
                    "tags": ["flower"],
                },
                {
                    "_id": "3",
                    "title": "Europe",
                    "tags": ["continent"],
                },
            ]

            add_docs_results = self.client.index(test_index_name).add_documents(docs, tensor_fields=["title"])

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
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            open_source_test_index_name = self.structured_index_name
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
                },
                # can't test model_auth here
            )

            ids = [doc["_id"] for doc in res["hits"]]

            self.assertEqual(set(ids), {"3"})
