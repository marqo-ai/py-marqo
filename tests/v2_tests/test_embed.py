import numpy as np
from pytest import mark

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex


@mark.fixed
class TestEmbed(MarqoTestCase):

    def setUp(self):
        self.test_cases = [(CloudTestIndex.structured_text, self.unstructured_index_name)]

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
                                            atol=1e-4))

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
                                            atol=1e-4))


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
                                            atol=1e-4))

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
                                            atol=1e-4))


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
                    np.allclose(embed_res["embeddings"][0], retrieved_docs["results"][0]["_tensor_facets"][0]["_embedding"], atol=1e-4))
                self.assertTrue(
                    np.allclose(embed_res["embeddings"][1], retrieved_docs["results"][1]["_tensor_facets"][0]["_embedding"], atol=1e-4))


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

    def test_embed_images_with_languagebind(self):
        """Embeds multiple images using LanguageBind model."""
        test_index_name = self.unstructured_languagebind_index_name
        
        image_urls = [
            "https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_realistic.png",
            "https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_realistic.png",
            "https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_realistic.png"
        ]

        embed_res = self.client.index(test_index_name).embed(content=image_urls)

        self.assertIn("processingTimeMs", embed_res)
        self.assertEqual(embed_res["content"], image_urls)
        self.assertEqual(len(embed_res["embeddings"]), 3)
        
        # Check that embeddings are non-zero and have the expected shape
        for embedding in embed_res["embeddings"]:
            self.assertGreater(len(embedding), 0)
            self.assertTrue(any(abs(x) > 1e-6 for x in embedding))

        # Check that embeddings are close to the expected values
        expected_embedding = [0.019889963790774345, -0.01263524405658245, 
                              0.026028314605355263, 0.005291664972901344, -0.013181567192077637]
        for embedding in embed_res["embeddings"]:
            for i, value in enumerate(expected_embedding):
                self.assertAlmostEqual(embedding[i], value, places=5)


    def test_embed_videos_with_languagebind(self):
        """Embeds multiple videos using LanguageBind model."""
        test_index_name = self.structured_languagebind_index_name
        
        video_urls = [
            "https://marqo-k400-video-test-dataset.s3.amazonaws.com/videos/---QUuC4vJs_000084_000094.mp4",
            "https://marqo-k400-video-test-dataset.s3.amazonaws.com/videos/---QUuC4vJs_000084_000094.mp4",
            "https://marqo-k400-video-test-dataset.s3.amazonaws.com/videos/---QUuC4vJs_000084_000094.mp4"
        ]

        embed_res = self.client.index(test_index_name).embed(content=video_urls)

        self.assertIn("processingTimeMs", embed_res)
        self.assertEqual(embed_res["content"], video_urls)
        self.assertEqual(len(embed_res["embeddings"]), 3)
        
        # Check that embeddings are non-zero and have the expected shape
        for embedding in embed_res["embeddings"]:
            self.assertGreater(len(embedding), 0)
            self.assertTrue(any(abs(x) > 1e-6 for x in embedding))

        # Check that embeddings are close to the expected values
        expected_embedding = [0.0394694060087204, 0.049264926463365555, 
                              -0.014714145101606846, 0.05715121701359749, -0.019508328288793564]
        for embedding in embed_res["embeddings"]:
            for i, value in enumerate(expected_embedding):
                self.assertAlmostEqual(embedding[i], value, places=5)
