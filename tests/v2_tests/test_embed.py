from unittest import mock

import numpy as np
from pytest import mark

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex


@mark.fixed
class TestEmbed(MarqoTestCase):

    def setUp(self):
        self.test_cases = [
            (CloudTestIndex.unstructured_text, self.unstructured_index_name),
        ]

    def test_embed_single_string(self):
        """Embeds a string. Use add docs and get docs with tensor facets to ensure the vector is correct.
                Checks the basic functionality and response structure. Also checks that the request level prefix override works."""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                # Add document
                tensor_fields = ["text_field_1"] if "unstr" in test_index_name else None
                d1 = {
                    "_id": "doc1",
                    "text_field_1": "Jimmy Butler is the GOAT."
                }

                res_1 = self.client.index(test_index_name).add_documents([d1], tensor_fields=tensor_fields)

                # Get doc with tensor facets (for reference vector)
                retrieved_d1 = self.client.index(test_index_name).get_document(
                    document_id="doc1", expose_facets=True)

                # Call embed
                embed_res_1 = self.client.index(test_index_name).embed("Jimmy Butler is the GOAT.",
                                                                       content_type="document")

                # Assert that the 
                self.assertIn("processingTimeMs", embed_res_1)
                self.assertEqual(embed_res_1["content"], "Jimmy Butler is the GOAT.")
                self.assertTrue(np.allclose(embed_res_1["embeddings"][0], retrieved_d1["_tensor_facets"][0]["_embedding"],
                                            atol=1e-3))

    def test_request_level_prefix_override_embed_add_docs(self):
        """Checks that the request level prefix override works."""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                # Add document
                tensor_fields = ["text_field_1"] if "unstr" in test_index_name else None
                d1 = {
                    "_id": "doc1",
                    "text_field_1": "Jimmy Butler is the GOAT."
                }
                res = self.client.index(test_index_name).add_documents([d1], tensor_fields=tensor_fields,
                                                                       text_chunk_prefix="test query: ")

                # Get doc with tensor facets (for reference vector)
                retrieved_d1 = self.client.index(test_index_name).get_document(
                    document_id="doc1", expose_facets=True)

                embed_res = self.client.index(test_index_name).embed("test query: Jimmy Butler is the GOAT.",
                                                                     content_type=None)

                # Assert request level prefix override
                self.assertIn("processingTimeMs", embed_res)
                self.assertEqual(embed_res["content"], "test query: Jimmy Butler is the GOAT.")
                self.assertTrue(np.allclose(embed_res["embeddings"][0], retrieved_d1["_tensor_facets"][0]["_embedding"],
                                            atol=1e-3))


    def test_embed_with_device(self):
        """Embeds a string with device parameter. Use add docs and get docs with tensor facets to ensure the vector is correct.
                        Checks the basic functionality and response structure"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                # Add document
                tensor_fields = ["text_field_1"] if "unstr" in test_index_name else None
                d1 = {
                    "_id": "doc1",
                    "text_field_1": "Jimmy Butler is the GOAT."
                }
                res = self.client.index(test_index_name).add_documents([d1], tensor_fields=tensor_fields)

                # Get doc with tensor facets (for reference vector)
                retrieved_d1 = self.client.index(test_index_name).get_document(
                    document_id="doc1", expose_facets=True)

                # Call embed
                embed_res = self.client.index(test_index_name).embed(content="Jimmy Butler is the GOAT.", device="cpu",
                                                                     content_type="document")
                self.assertIn("processingTimeMs", embed_res)
                self.assertEqual(embed_res["content"], "Jimmy Butler is the GOAT.")
                self.assertTrue(np.allclose(embed_res["embeddings"][0], retrieved_d1["_tensor_facets"][0] ["_embedding"],
                                            atol=1e-3))

    def test_embed_single_dict(self):
        """Embeds a dict. Use add docs and get docs with tensor facets to ensure the vector is correct.
                        Checks the basic functionality and response structure"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                # Add document
                tensor_fields = ["text_field_1"] if "unstr" in test_index_name else None
                d1 = {
                    "_id": "doc1",
                    "text_field_1": "Jimmy Butler is the GOAT."
                }
                res = self.client.index(test_index_name).add_documents([d1], tensor_fields=tensor_fields)

                # Get doc with tensor facets (for reference vector)
                retrieved_d1 = self.client.index(test_index_name).get_document(
                    document_id="doc1", expose_facets=True)

                # Call embed
                embed_res = self.client.index(test_index_name).embed(content={"Jimmy Butler is the GOAT.": 1},
                                                                     content_type="document")

                self.assertIn("processingTimeMs", embed_res)
                self.assertEqual(embed_res["content"], {"Jimmy Butler is the GOAT.": 1})
                self.assertTrue(np.allclose(embed_res["embeddings"][0], retrieved_d1["_tensor_facets"][0]["_embedding"],
                                            atol=1e-3))


    def test_embed_list_content(self):
        """Embeds a list with string and dict. Use add docs and get docs with tensor facets to ensure the vector is correct.
                                Checks the basic functionality and response structure"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                # Add document
                tensor_fields = ["text_field_1"] if "unstr" in test_index_name else None
                d1 = {
                    "_id": "doc1",
                    "text_field_1": "Jimmy Butler is the GOAT."
                }
                d2 = {
                    "_id": "doc2",
                    "text_field_1": "Alex Caruso is the GOAT."
                }
                res = self.client.index(test_index_name).add_documents([d1, d2], tensor_fields=tensor_fields)

                # Get doc with tensor facets (for reference vector)
                retrieved_docs = self.client.index(test_index_name).get_documents(
                    document_ids=["doc1", "doc2"], expose_facets=True)

                # Call embed
                embed_res = self.client.index(test_index_name).embed(
                    content=[{"Jimmy Butler is the GOAT.": 1}, "Alex Caruso is the GOAT."], content_type="document"
                )

                self.assertIn("processingTimeMs", embed_res)
                self.assertEqual(embed_res["content"], [{"Jimmy Butler is the GOAT.": 1}, "Alex Caruso is the GOAT."])
                self.assertTrue(
                    np.allclose(embed_res["embeddings"][0], retrieved_docs["results"][0]["_tensor_facets"][0]["_embedding"], atol=1e-3))
                self.assertTrue(
                    np.allclose(embed_res["embeddings"][1], retrieved_docs["results"][1]["_tensor_facets"][0]["_embedding"], atol=1e-3))


    def test_embed_non_numeric_weight_fails(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                with self.assertRaises(MarqoWebError) as e:
                    self.client.index(test_index_name).embed(content={"text to embed": "not a number"})

            self.assertIn("not a valid float", str(e.exception))

    def test_media_download_headers_is_not_included(self):
        """Ensure newly added attributes mediaDownloadHeaders is not included in the request body."""
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            self.client.index(index_name=self.generic_test_index_name).embed(
                content=["something"])
            args, kwargs = mock__post.call_args
            self.assertNotIn("mediaDownloadHeaders", kwargs["body"])
            self.assertNotIn("imageDownloadHeaders", kwargs["body"])
            return True
        run()

    def test_media_download_headers_is_included_if_explicitly_set(self):
        """Ensure newly added attributes mediaDownloadHeaders is included if explicitly set."""
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            self.client.index(index_name=self.generic_test_index_name).embed(
                content=["something"], media_download_headers={"key": "value-1"},
                image_download_headers={"key": "value-2"}
            )
            args, kwargs = mock__post.call_args
            self.assertIn("mediaDownloadHeaders", kwargs["body"])
            self.assertEqual({"key": "value-2"}, kwargs["body"]["imageDownloadHeaders"])
            self.assertEqual({"key": "value-1"}, kwargs["body"]["mediaDownloadHeaders"])
            return True

        run()
