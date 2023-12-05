import copy
import functools
import math
import pprint
import random
import pytest
import requests
import time

from pytest import mark

from marqo.errors import MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo import enums
from unittest import mock
import numpy as np


class TestChunkPrefix(MarqoTestCase):
    def setUp(self):
        super().setUp()
        self.test_index_with_model_default_prefix = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_settings_dict={
                "index_defaults": {
                    "model": "prefix-test-model"    # Loads sentence-transformers/all-MiniLM-L6-v1. uses "test query: " and "test passage: "
                }
            }
        )
        self.test_index_with_index_override_prefix = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name_3,
            open_source_index_settings_dict={
                "index_defaults": {
                    "model": "prefix-test-model",    # Loads sentence-transformers/all-MiniLM-L6-v1. No default prefix
                    "text_preprocessing": {
                        "override_text_chunk_prefix": "override passage: ",
                    }
                }
            }
        )
        self.test_index_with_no_prefix = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name_2,
            open_source_index_settings_dict={
                "index_defaults": {
                    "model": "sentence-transformers/test"    # Loads sentence-transformers/all-MiniLM-L6-v1. No default prefix
                }
            }
        )


    @mark.ignore_during_cloud_tests
    def test_chunk_prefix_use_model_default(self):
        # Doc A should have prefix from model default, Doc B should have prefix from built in text
        self.client.index(self.test_index_with_model_default_prefix).add_documents(
            documents=[{"_id": "doc_a", "text": "HELLO"}], 
            auto_refresh=True, tensor_fields=["text"]
        )
        self.client.index(self.test_index_with_no_prefix).add_documents(
            documents=[{"_id": "doc_b", "text": "test passage: HELLO"}], 
            auto_refresh=True, tensor_fields=["text"]
        )

        retrieved_doc_a = self.client.index(self.test_index_with_model_default_prefix).get_document(document_id="doc_a", expose_facets=True)
        retrieved_doc_b = self.client.index(self.test_index_with_no_prefix).get_document(document_id="doc_b", expose_facets=True)
        assert np.allclose(retrieved_doc_a["_tensor_facets"][0]["_embedding"], retrieved_doc_b["_tensor_facets"][0]["_embedding"], atol=1e-5)


    @mark.ignore_during_cloud_tests
    def test_chunk_prefix_use_index_override(self):
        # Doc A should have prefix from index override, Doc B should have prefix from built in text
        self.client.index(self.test_index_with_index_override_prefix).add_documents(
            documents=[{"_id": "doc_a", "text": "HELLO"}], 
            auto_refresh=True, tensor_fields=["text"]
        )
        self.client.index(self.test_index_with_no_prefix).add_documents(
            documents=[{"_id": "doc_b", "text": "override passage: HELLO"}], 
            auto_refresh=True, tensor_fields=["text"]
        )

        retrieved_doc_a = self.client.index(self.test_index_with_index_override_prefix).get_document(document_id="doc_a", expose_facets=True)
        retrieved_doc_b = self.client.index(self.test_index_with_no_prefix).get_document(document_id="doc_b", expose_facets=True)
        assert np.allclose(retrieved_doc_a["_tensor_facets"][0]["_embedding"], retrieved_doc_b["_tensor_facets"][0]["_embedding"], atol=1e-5)


    @mark.ignore_during_cloud_tests
    def test_chunk_prefix_use_add_docs_override(self):
        # Doc A should have prefix from add docs override, Doc B should have prefix from built in text
        self.client.index(self.test_index_with_index_override_prefix).add_documents(
            documents=[{"_id": "doc_a", "text": "HELLO"}], 
            auto_refresh=True, tensor_fields=["text"], text_chunk_prefix="add docs passage: "
        )
        self.client.index(self.test_index_with_no_prefix).add_documents(
            documents=[{"_id": "doc_b", "text": "add docs passage: HELLO"}], 
            auto_refresh=True, tensor_fields=["text"]
        )

        retrieved_doc_a = self.client.index(self.test_index_with_index_override_prefix).get_document(document_id="doc_a", expose_facets=True)
        retrieved_doc_b = self.client.index(self.test_index_with_no_prefix).get_document(document_id="doc_b", expose_facets=True)
        assert np.allclose(retrieved_doc_a["_tensor_facets"][0]["_embedding"], retrieved_doc_b["_tensor_facets"][0]["_embedding"], atol=1e-5)


    def test_cloud_chunk_prefix_parameter(self):
        """
        Using the basic cloud index (with no default prefix), just show that tensors generated from text with prefix parameter are the same
        as tensors generated from text with prefix built in.
        """
        self.client.index(self.test_index_with_no_prefix).add_documents(
            documents=[{"_id": "doc_a", "text": "HELLO"}], 
            auto_refresh=True, tensor_fields=["text"], text_chunk_prefix="add docs passage: "
        )
        self.client.index(self.test_index_with_no_prefix).add_documents(
            documents=[{"_id": "doc_b", "text": "add docs passage: HELLO"}], 
            auto_refresh=True, tensor_fields=["text"]
        )

        retrieved_docs = self.client.index(self.test_index_with_no_prefix).get_documents(document_ids=["doc_a", "doc_b"], expose_facets=True)
        retrieved_doc_a = retrieved_docs["results"][0]
        retrieved_doc_b = retrieved_docs["results"][1]
        assert np.allclose(retrieved_doc_a["_tensor_facets"][0]["_embedding"], retrieved_doc_b["_tensor_facets"][0]["_embedding"], atol=1e-5)

class TestQueryPrefix(MarqoTestCase):
    def setUp(self):
        super().setUp()
        self.test_index_with_model_default_prefix = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_settings_dict={
                "index_defaults": {     # Using custom model to manually define query prefix
                    "model": "generic-clip-test-model-1",
                    "model_properties": {
                        "name": "ViT-B-32-quickgelu",
                        "dimensions": 512,
                        "url": "https://github.com/mlfoundations/open_clip/releases/download/v0.2-weights/vit_b_32-quickgelu-laion400m_avg-8a00ab3c.pt",
                        "type": "open_clip",
                        "text_query_prefix": "test query: ",
                    }
                }
            }
        )
        self.test_index_with_index_override_prefix = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name_3,
            open_source_index_settings_dict={
                "index_defaults": {
                    "model": "generic-clip-test-model-1",
                    "model_properties": {
                        "name": "ViT-B-32-quickgelu",
                        "dimensions": 512,
                        "url": "https://github.com/mlfoundations/open_clip/releases/download/v0.2-weights/vit_b_32-quickgelu-laion400m_avg-8a00ab3c.pt",
                        "type": "open_clip",
                        "text_query_prefix": "test query: ",
                    },
                    "text_preprocessing": {
                        "override_text_query_prefix": "override query: "
                    }
                }
            }
        )
        self.test_index_with_no_prefix = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name_2,
            open_source_index_settings_dict={
                "index_defaults": {
                    "model": "generic-clip-test-model-1",
                    "model_properties": {
                        "name": "ViT-B-32-quickgelu",
                        "dimensions": 512,
                        "url": "https://github.com/mlfoundations/open_clip/releases/download/v0.2-weights/vit_b_32-quickgelu-laion400m_avg-8a00ab3c.pt",
                        "type": "open_clip",
                    }
                }
            }
        )

        # Add 4 docs to each index (for search test options)
        for index_name in [self.test_index_with_model_default_prefix, self.test_index_with_index_override_prefix, self.test_index_with_no_prefix]:
            self.client.index(index_name).add_documents(
                documents=[
                    {"_id": "doc_no_prefix", "text": "HELLO"}, 
                    {"_id": "doc_model_default_prefix", "text": "test query: HELLO"}, 
                    {"_id": "doc_index_override_prefix", "text": "override query: HELLO"},
                    {"_id": "doc_search_override_prefix", "text": "search query: HELLO"}
                ], 
                auto_refresh=True, tensor_fields=["text"]
            )
    

    @mark.ignore_during_cloud_tests
    def test_query_prefix_use_model_default(self):
        res = self.client.index(self.test_index_with_model_default_prefix).search(q="HELLO", search_method="TENSOR")
        assert res["hits"][0]["_id"] == "doc_model_default_prefix"


    @mark.ignore_during_cloud_tests
    def test_query_prefix_use_index_override(self):
        res = self.client.index(self.test_index_with_index_override_prefix).search(q="HELLO", search_method="TENSOR")
        assert res["hits"][0]["_id"] == "doc_index_override_prefix"


    @mark.ignore_during_cloud_tests
    def test_query_prefix_use_search_override(self):
        res = self.client.index(self.test_index_with_index_override_prefix).search(q="HELLO", search_method="TENSOR", text_query_prefix="search query: ")
        assert res["hits"][0]["_id"] == "doc_search_override_prefix"
    
    def test_cloud_query_prefix_parameter(self):
        """
        Using the basic cloud index (with no default prefix), just show that querying with a prefix gets the correct result.
        """
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_with_no_prefix).search, q="HELLO", search_method="TENSOR", text_query_prefix="search query: ")
        
        res = self.client.index(self.test_index_with_no_prefix).search(q="HELLO", search_method="TENSOR", text_query_prefix="search query: ")
        assert res["hits"][0]["_id"] == "doc_search_override_prefix"