import copy
import pprint

import pytest
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
import unittest
from tests.marqo_test import MarqoTestCase
from marqo import enums
from unittest import mock
import numpy as np

@pytest.mark.cuda_test
class TestAddDocuments(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def tearDown(self) -> None:
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass


    # Add documents tests:
    def test_add_documents_with_ids(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
                "doc title": "Cool Document 1",
                "field 1": "some extra info",
                "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
            }
        d2 = {
                "doc title": "Just Your Average Doc",
                "field X": "this is a solid doc",
                "_id": "123456"
        }
        res = self.client.index(self.index_name_1).add_documents([
            d1, d2
        ], device="cuda", non_tensor_fields=[])
        retrieved_d1 = self.client.index(self.index_name_1).get_document(document_id="e197e580-0393-4f4e-90e9-8cdf4b17e339")
        assert retrieved_d1 == d1
        retrieved_d2 = self.client.index(self.index_name_1).get_document(document_id="123456")
        assert retrieved_d2 == d2

    def test_add_documents(self):
        """indexes the documents and retrieves the documents with the generated IDs"""
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
                "doc title": "Cool Document 1",
                "field 1": "some extra info"
            }
        d2 = {
                "doc title": "Just Your Average Doc",
                "field X": "this is a solid doc"
            }
        res = self.client.index(self.index_name_1).add_documents([d1, d2], device="cuda", non_tensor_fields=[])
        ids = [item["_id"] for item in res["items"]]
        assert len(ids) == 2
        assert ids[0] != ids[1]
        retrieved_d0 = self.client.index(self.index_name_1).get_document(ids[0])
        retrieved_d1 = self.client.index(self.index_name_1).get_document(ids[1])
        del retrieved_d0["_id"]
        del retrieved_d1["_id"]
        assert retrieved_d0 == d1 or retrieved_d0 == d2
        assert retrieved_d1 == d1 or retrieved_d1 == d2

    def test_add_documents_with_ids_twice(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "doc title": "Just Your Average Doc",
            "field X": "this is a solid doc",
            "_id": "56"
        }
        self.client.index(self.index_name_1).add_documents([d1], device="cuda", non_tensor_fields=[])
        assert d1 == self.client.index(self.index_name_1).get_document("56")
        d2 = {
            "_id": "56",
            "completely": "different doc.",
            "field X": "this is a solid doc"
        }
        self.client.index(self.index_name_1).add_documents([d2], device="cuda", non_tensor_fields=[])
        assert d2 == self.client.index(self.index_name_1).get_document("56")

    def test_add_batched_documents(self):
        self.client.create_index(self.index_name_1)
        ix = self.client.index(index_name=self.index_name_1)
        doc_ids = [str(num) for num in range(0, 100)]
        docs = [
            {"Title": f"The Title of doc {doc_id}",
             "Generic text": "some text goes here...",
             "_id": doc_id}
            for doc_id in doc_ids]

        ix.add_documents(docs, device="cuda", non_tensor_fields=[])
        ix.refresh()
        # TODO we should do a count in here...
        # takes too long to search for all
        for _id in [0, 19, 20, 99]:
            original_doc = docs[_id].copy()
            assert ix.get_document(document_id=str(_id)) == original_doc

    def test_add_documents_long_fields(self):
        """TODO
        """
    def test_update_docs_updates_chunks(self):
        """TODO"""

    # delete documents tests:

    def test_delete_docs(self):
        self.client.create_index(index_name=self.index_name_1)
        self.client.index(self.index_name_1).add_documents([
            {"abc": "wow camel", "_id": "123"},
            {"abc": "camels are cool", "_id": "foo"}
        ], device="cuda", non_tensor_fields=[])
        res0 = self.client.index(self.index_name_1).search("wow camel", device="cuda")
        print("res0res0")
        pprint.pprint(res0)
        assert res0['hits'][0]["_id"] == "123"
        assert len(res0['hits']) == 2
        self.client.index(self.index_name_1).delete_documents(["123"])
        res1 = self.client.index(self.index_name_1).search("wow camel", device="cuda")
        assert res1['hits'][0]["_id"] == "foo"
        assert len(res1['hits']) == 1

    def test_delete_docs_empty_ids(self):
        self.client.create_index(index_name=self.index_name_1)
        self.client.index(self.index_name_1).add_documents([{"abc": "efg", "_id": "123"}], device="cuda",
                                                           non_tensor_fields=[])
        try:
            self.client.index(self.index_name_1).delete_documents([])
            raise AssertionError
        except MarqoWebError as e:
            assert "can't be empty" in str(e) or "value_error.missing" in str (e)
        res = self.client.index(self.index_name_1).get_document("123")
        print(res)
        assert "abc" in res

    # get documents tests :

    def test_get_document(self):
        """FIXME (do edge cases)"""

    # user experience tests:

    def test_add_documents_with_device(self):
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], device="cuda:45", non_tensor_fields=[])
            return True
        assert run()

        args, kwargs = mock__post.call_args
        assert "device=cuda45" in kwargs["path"]

    def test_add_documents_set_refresh(self):
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], device="cuda", auto_refresh=False, non_tensor_fields=[])
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], device="cuda", auto_refresh=True, non_tensor_fields=[])
            return True
        assert run()

        args, kwargs0 = mock__post.call_args_list[0]
        assert "refresh=false" in kwargs0["path"]
        args, kwargs1 = mock__post.call_args_list[1]
        assert "refresh=true" in kwargs1["path"]

    def test_add_documents_with_no_processes(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            self.client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], device="cuda", non_tensor_fields=[])
            return True
        assert run()

        args, kwargs = mock__post.call_args
        assert "processes=12" not in kwargs["path"]

    def test_add_documents_defaults_to_cuda(self):
        """
            Ensures that when cuda is available, when we send an add docs request with no device,
            cuda is selected as default and used for this.
        """
        index_settings = {
            "index_defaults": {
                # model was chosen due to bigger difference between cuda and cpu vectors
                "model": "open_clip/ViT-B-32-quickgelu/laion400m_e31",
                "normalize_embeddings": True
            }
        }

        self.client.create_index(self.index_name_1, settings_dict=index_settings)

        self.client.index(self.index_name_1).add_documents([{"_id": "explicit_cpu", "title": "blah"}], device="cpu",
                                                           non_tensor_fields=[])
        self.client.index(self.index_name_1).add_documents([{"_id": "explicit_cuda", "title": "blah"}], device="cuda",
                                                           non_tensor_fields=[])
        self.client.index(self.index_name_1).add_documents([{"_id": "default_device", "title": "blah"}],
                                                           non_tensor_fields=[])

        cpu_vec = self.client.index(self.index_name_1).get_document(document_id="explicit_cpu", expose_facets=True)['_tensor_facets'][0]["_embedding"]
        cuda_vec = self.client.index(self.index_name_1).get_document(document_id="explicit_cuda", expose_facets=True)['_tensor_facets'][0]["_embedding"]
        default_vec = self.client.index(self.index_name_1).get_document(document_id="default_device", expose_facets=True)['_tensor_facets'][0]["_embedding"]

        # Confirm that CUDA was used by default.
        # CUDA-computed vectors are slightly different from CPU-computed vectors
        assert not np.allclose(np.array(cpu_vec), np.array(default_vec), atol=1e-5)
        assert np.allclose(np.array(cuda_vec), np.array(default_vec), atol=1e-5)

        
