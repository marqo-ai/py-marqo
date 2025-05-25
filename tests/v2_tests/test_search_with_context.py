from typing import Any, Dict, List, Optional

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


@mark.fixed
class TestSearchWithContext(MarqoTestCase):

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
                    },
                    {
                        "Title": "Another history of dogs",
                        "Description": "Second history of household pets",
                        "_id": "d3"
                    }
                ], tensor_fields=["Title", "Description"]
            )
        self.vector_dim = 512

        self.query = {"What are the best pets": 1}

    def search_with_context(self, context_object: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
        return self.client.index(self.test_index_name).search(
            q=self.query,
            context=context_object
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

    def test_context_documents_alone_full_parameters_succeeds(self):
        """
        Test that the context documents alone are sufficient to return results
        """
        context = {
            "documents": {
                "ids": {
                    "d2": 1
                },
                "parameters": {
                    "tensorFields": ["Title", "Description"],
                    "excludeInputDocuments": True
                }
            }
        }
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context(context))

        custom_res = self.search_with_context(context)

        # Result should be d3 (about dogs), then d1. d2 should be excluded.
        self.assertEqual(
            [h["_id"] for h in custom_res["hits"]],
            ["d3", "d1"]
        )

    def test_context_documents_alone_empty_parameters_succeeds(self):
        """
        Test that the context documents alone are sufficient to return results
        """
        context = {
            "documents": {
                "ids": {
                    "d2": 1
                },
                "parameters": {}
            }
        }
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context(context))

        custom_res = self.search_with_context(context)

        # Result should be d3 (about dogs), then d1. d2 should be excluded.
        self.assertEqual(
            [h["_id"] for h in custom_res["hits"]],
            ["d3", "d1"]
        )

    def test_context_documents_alone_no_parameters_succeeds(self):
        context = {
            "documents": {
                "ids": {
                    "d2": 1
                }
            }
        }
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context(context))

        custom_res = self.search_with_context(context)

        # Result should be d3 (about dogs), then d1. d2 should be excluded.
        self.assertEqual(
            [h["_id"] for h in custom_res["hits"]],
            ["d3", "d1"]
        )

    def test_context_documents_alone_no_ids_fails(self):
        """
        Test that the context documents alone without any ids fails
        """
        context = {
            "documents": {
                "parameters": {
                    "tensorFields": ["Title", "Description"],
                    "excludeInputDocuments": True
                }
            }
        }
        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_context(context))

        with self.assertRaises(MarqoWebError) as e:
            self.search_with_context(context)
        self.assertIn("must be present and a non-empty dict", str(e.exception))

    def test_context_documents_tensors_and_queries_succeeds(self):
        """
        Test that the context documents with tensors and queries are sufficient to return results
        Use all 3 interpolation methods (LERP, NLERP, SLERP)
        Use context.documents.parameters.excludeInputDocuments (True and False)
        Use context.documents.parameters.tensorFields (with or without)
        """
        interpolation_types = ["LERP", "NLERP", "SLERP"]
        exclude_input_documents = [True, False]
        tensor_fields = [["Title", "Description"], None]

        for interpolation_type in interpolation_types:
            for exclude_input_document in exclude_input_documents:
                for tensor_field in tensor_fields:
                    with self.subTest(interpolation_type=interpolation_type,
                                      exclude_input_document=exclude_input_document,
                                      tensor_field=tensor_field):

                        context = {
                            "tensor": [
                                {"vector": [1, ] * self.vector_dim, "weight": 0},
                                {"vector": [2, ] * self.vector_dim, "weight": 1}
                            ],
                            "documents": {
                                "ids": {
                                    "d2": 1
                                },
                                "parameters": {
                                    "excludeInputDocuments": exclude_input_document,
                                    "tensorFields": tensor_field
                                }
                            }
                        }
                        if self.IS_MULTI_INSTANCE:
                            self.warm_request(lambda: self.search_with_context(context))

                        custom_res = self.search_with_context(context)

                        # Result should be d3 (about dogs), then d1. d2 should be excluded.
                        if exclude_input_document:
                            self.assertEqual(
                                set(["d3", "d1"]),
                                set([h["_id"] for h in custom_res["hits"]])
                            )
                        else:
                            # If excludeInputDocuments is False, d2 should be included
                            self.assertEqual(
                                set(["d2", "d3", "d1"]),
                                set([h["_id"] for h in custom_res["hits"]])
                            )


