import copy
import functools
import math
import pprint
import random
import pytest
import requests
import time
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase
from marqo import enums
from unittest import mock


class TestAddDocuments(MarqoTestCase):
    # Create index tests
    def test_create_index(self):
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)

    def test_create_index_double(self):
        if not self.client.config.is_marqo_cloud:
            self.create_test_index(index_name=self.generic_test_index_name)
        try:
            self.create_test_index(index_name=self.generic_test_index_name)
        except MarqoError as e:
            assert e.code in ["index_already_exists", "index_already_exists_cloud"]

    def test_create_index_hnsw(self):
        if not self.client.config.is_marqo_cloud:
            self.client.delete_index(self.generic_test_index_name)
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name, settings_dict={
            "index_defaults": {
                "ann_parameters": {
                    "parameters": {
                        "m": 24
                    }
                }
            }
        })
        assert self.client.get_index(self.test_index_name).get_settings() \
                   ["index_defaults"]["ann_parameters"]["parameters"]["m"] == 24

        # Ensure non-specified values are in default
        assert self.client.get_index(self.test_index_name).get_settings() \
                   ["index_defaults"]["ann_parameters"]["parameters"]["ef_construction"] == 128
        assert self.client.get_index(self.test_index_name).get_settings() \
                   ["index_defaults"]["ann_parameters"]["space_type"] == "cosinesimil"

    # Delete index tests:

    # def test_delete_index(self):
    #     self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
    #     self.client.delete_index(self.test_index_name)
    #     self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)

    # Get index tests:

    def test_get_index(self):
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        index = self.client.get_index(self.test_index_name)
        assert index.index_name == self.test_index_name

    def test_get_index_non_existent(self):
        try:
            index = self.client.get_index("some-non-existent-index")
            raise AssertionError
        except MarqoError as e:
            assert e.code in ["index_not_found", "index_not_found_cloud"]

    # Add documents tests:

    def test_add_documents_with_ids(self):
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
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
        res = self.client.index(self.test_index_name).add_documents([
            d1, d2
        ], tensor_fields=["field X", "field 1", "doc title"])
        retrieved_d1 = self.client.index(self.test_index_name).get_document(
            document_id="e197e580-0393-4f4e-90e9-8cdf4b17e339")
        assert retrieved_d1 == d1
        retrieved_d2 = self.client.index(self.test_index_name).get_document(document_id="123456")
        assert retrieved_d2 == d2

    def test_add_documents(self):
        """indexes the documents and retrieves the documents with the generated IDs"""
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        d1 = {
            "doc title": "Cool Document 1",
            "field 1": "some extra info"
        }
        d2 = {
            "doc title": "Just Your Average Doc",
            "field X": "this is a solid doc"
        }
        res = self.client.index(self.test_index_name).add_documents([d1, d2], tensor_fields=["field X", "field 1", "doc title"])
        ids = [item["_id"] for item in res["items"]]
        assert len(ids) == 2
        assert ids[0] != ids[1]
        retrieved_d0 = self.client.index(self.test_index_name).get_document(ids[0])
        retrieved_d1 = self.client.index(self.test_index_name).get_document(ids[1])
        del retrieved_d0["_id"]
        del retrieved_d1["_id"]
        assert retrieved_d0 == d1 or retrieved_d0 == d2
        assert retrieved_d1 == d1 or retrieved_d1 == d2

    def test_add_documents_with_ids_twice(self):
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        d1 = {
            "doc title": "Just Your Average Doc",
            "field X": "this is a solid doc",
            "_id": "56"
        }
        self.client.index(self.test_index_name).add_documents([d1], tensor_fields=["field X", "doc title"])
        assert d1 == self.client.index(self.test_index_name).get_document("56")
        d2 = {
            "_id": "56",
            "completely": "different doc.",
            "field X": "this is a solid doc"
        }
        self.client.index(self.test_index_name).add_documents([d2], tensor_fields=["field X", "completely"])
        assert d2 == self.client.index(self.test_index_name).get_document("56")

    def test_add_batched_documents(self):
        self.test_index_name = self.create_test_index(self.generic_test_index_name)
        ix = self.client.index(index_name=self.test_index_name)
        doc_ids = [str(num) for num in range(0, 100)]

        docs = [
            {"Title": f"The Title of doc {doc_id}",
             "Generic text": "some text goes here...",
             "_id": doc_id}
            for doc_id in doc_ids]
        assert len(docs) == 100
        ix.add_documents(docs, client_batch_size=4, tensor_fields=["Title", "Generic text"])
        ix.refresh()
        # takes too long to search for all...
        for _id in [0, 19, 20, 99]:
            original_doc = docs[_id].copy()
            assert ix.get_document(document_id=str(_id)) == original_doc
        assert self.client.index(index_name=self.test_index_name).get_stats()['numberOfDocuments'] == 100

    def test_add_documents_long_fields(self):
        """TODO
        """

    def test_update_docs_updates_chunks(self):
        """TODO"""

    # delete documents tests:

    def test_delete_docs(self):
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        self.client.index(self.test_index_name).add_documents([
            {"abc": "wow camel", "_id": "123"},
            {"abc": "camels are cool", "_id": "foo"}
        ], tensor_fields=["abc"])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search, "wow camel")

        res0 = self.client.index(self.test_index_name).search("wow camel")
        print("res0res0")
        pprint.pprint(res0)
        assert res0['hits'][0]["_id"] == "123"
        assert len(res0['hits']) == 2
        self.client.index(self.test_index_name).delete_documents(["123"])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search, "wow camel")
        res1 = self.client.index(self.test_index_name).search("wow camel")
        assert res1['hits'][0]["_id"] == "foo"
        assert len(res1['hits']) == 1

    def test_delete_docs_empty_ids(self):
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        self.client.index(self.test_index_name).add_documents([{"abc": "efg", "_id": "123"}], tensor_fields=["abc"])
        try:
            self.client.index(self.test_index_name).delete_documents([])
            raise AssertionError
        except MarqoWebError as e:
            assert "can't be empty" in str(e) or "value_error.missing" in str(e)
        res = self.client.index(self.test_index_name).get_document("123")
        print(res)
        assert "abc" in res

    def test_get_document(self):
        my_doc = {"abc": "efg", "_id": "123"}
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        self.client.index(self.test_index_name).add_documents([my_doc], tensor_fields=["abc"])
        retrieved = self.client.index(self.test_index_name).get_document(document_id='123')
        assert retrieved == my_doc

    def test_add_documents_missing_index_fails(self):
        with pytest.raises(MarqoError) as ex:
            self.client.index("some-non-existing-index").add_documents([{"abd": "efg"}], tensor_fields=["abc"])

        assert ex.value.code in ["index_not_found", "index_not_found_cloud"]

    def test_add_documents_with_device(self):
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], device="cuda:45", tensor_fields=["d1", "d2"])
            return True

        assert run()

        args, kwargs = mock__post.call_args
        assert "device=cuda45" in kwargs["path"]

    def test_add_documents_with_device_batching(self):
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}, {"d2331": "blah"}, {"45d2", "some data"}
            ], client_batch_size=2, device="cuda:37", tensor_fields=["d1", "d2", "d2331", "45d2"])
            return True

        assert run()
        assert len(mock__post.call_args_list) == 3
        for args, kwargs in mock__post.call_args_list[:-1]:
            assert "device=cuda37" in kwargs["path"]

    def test_add_documents_device_not_set(self):
        """If no device is set, do not even add device parameter to the API call
        """
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], tensor_fields=["d1", "d2"])
            return True

        assert run()

        args, kwargs = mock__post.call_args
        assert "device" not in kwargs["path"]

    def test_add_documents_set_refresh(self):
        temp_client = copy.deepcopy(self.client)
        temp_client.config.search_device = enums.Devices.cpu
        temp_client.config.indexing_device = enums.Devices.cpu

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], auto_refresh=False, tensor_fields=["d1", "d2"])
            temp_client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], auto_refresh=True, tensor_fields=["d1", "d2"])
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
            self.client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], tensor_fields=["d1", "d2"])
            return True

        assert run()

        args, kwargs = mock__post.call_args
        assert "processes=12" not in kwargs["path"]

    def test_resilient_indexing(self):
        self.test_index_name = self.create_test_index(self.generic_test_index_name)

        if self.IS_MULTI_INSTANCE:
            time.sleep(1)

        assert 0 == self.client.index(self.test_index_name).get_stats()['numberOfDocuments']
        d1 = {"d1": "blah", "_id": "1234"}
        d2 = {"d2": "blah", "_id": "5678"}
        docs = [d1, {"content": "some terrible doc", "d3": "blah", "_id": 12345}, d2]
        self.client.index(self.test_index_name).add_documents(documents=docs, tensor_fields=["d1", "d2", "d3", "content"])

        if self.IS_MULTI_INSTANCE:
            time.sleep(1)

        assert 2 == self.client.index(self.test_index_name).get_stats()['numberOfDocuments']
        assert d1 == self.client.index(self.test_index_name).get_document(document_id='1234')
        assert d2 == self.client.index(self.test_index_name).get_document(document_id='5678')

        if self.IS_MULTI_INSTANCE:
            time.sleep(1)
        assert {"1234", "5678"} == {d['_id'] for d in
                                    self.client.index(self.test_index_name).search("blah", limit=3)['hits']}

    def test_batching_add_docs(self):

        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
        docs_to_add = 250
        vocab = requests.get(vocab_source).text.splitlines()
        docs = [{"Title": " ".join(random.choices(population=vocab, k=10)),
                 "Description": " ".join(random.choices(population=vocab, k=25)),
                 } for _ in range(docs_to_add)]

        batches = [None, 1, 2, 50]
        for auto_refresh in (None, True, False):
            for client_batch_size in batches:
                mock__post = mock.MagicMock()
                mock__post.return_value = dict()

                @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
                def run():
                    res = self.client.index(self.generic_test_index_name).add_documents(
                        auto_refresh=auto_refresh, documents=docs, client_batch_size=client_batch_size,
                        tensor_fields=["Title", "Description"])
                    if client_batch_size is not None:
                        assert isinstance(res, list)
                        assert len(res) == math.ceil(docs_to_add / client_batch_size)
                        # should only refresh on the last call, if auto_refresh=True
                        assert all([f'refresh=false' in d[1]['path'] for d in
                                    mock__post.call_args_list][:-1])
                        if auto_refresh:
                            assert [f"{self.generic_test_index_name}/refresh" in d[1]['path']
                                    for d in mock__post.call_args_list][-1]
                    else:
                        assert isinstance(res, dict)
                        # One huge request is made, if there is no client_side_batching:
                        assert all([len(d[1]['body']["documents"]) == docs_to_add for d in mock__post.call_args_list])

                    assert all(['batch' not in d[1]['path'] for d in mock__post.call_args_list])

                    assert all(['processes' not in d[1]['path'] for d in mock__post.call_args_list])

                    return True

                assert run()

    def test_add_lists_non_tensor(self):
        original_doc = {"d1": "blah", "_id": "1234", 'my list': ['tag-1', 'tag-2']}
        self.test_index_name = self.create_test_index(self.generic_test_index_name)
        self.client.index(self.test_index_name).add_documents(documents=[original_doc], non_tensor_fields=['my list'])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,
                              q='something', filter_string='my\ list:tag-1'
                              )

        res = self.client.index(self.test_index_name).search(
            q='something', filter_string='my\ list:tag-1'
        )
        assert res['hits'][0]['_id'] == '1234'

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,
                              q='something', filter_string='my\ list:tag-non-existent'
                              )

        bad_res = self.client.index(self.test_index_name).search(
            q='something', filter_string='my\ list:tag-non-existent'
        )
        assert len(bad_res['hits']) == 0

    def test_use_existing_fields(self):
        self.test_index_name = self.create_test_index(self.generic_test_index_name)
        self.client.index(index_name=self.test_index_name).add_documents(
            documents=[
                {
                    "_id": "123",
                    "title 1": "content 1",
                    "desc 2": "content 2. blah blah blah",
                    "old": "some other cool thing"
                }],
            non_tensor_fields=["desc 2"]
        )

        assert {"title 1", "_embedding", "old"} == functools.reduce(
            lambda x, y: x.union(y),
            [set(facet.keys()) for facet in
             self.client.index(index_name=self.test_index_name).get_document(
                 document_id="123", expose_facets=True)["_tensor_facets"]]
        )

        self.client.index(index_name=self.test_index_name).add_documents(
            documents=[
                {
                    "_id": "123",
                    "title 1": "content 1",
                    "desc 2": "content 2. blah blah blah",
                    "new f": "12345 "
                }], use_existing_tensors=True, tensor_fields=["desc 2", "new f", "title 1"]
        )
        # we don't get desc 2 facets, because it was already a non_tensor_field
        assert {"title 1", "_embedding", "new f"} == functools.reduce(
            lambda x, y: x.union(y),
            [set(facet.keys()) for facet in
             self.client.index(index_name=self.test_index_name).get_document(
                 document_id="123", expose_facets=True)["_tensor_facets"]]
        )

    def test_multimodal_combination_doc(self):
        settings = {
            "treat_urls_and_pointers_as_images": True,
            "model": "ViT-B/32",
        }
        self.test_index_name = self.create_test_index(index_name=self.generic_test_index_name, **settings)

        self.client.index(index_name=self.test_index_name).add_documents(
            documents=[
                {
                    "combo_text_image": {
                        # a space at the end
                        "text_0 ": "A rider is riding a horse jumping over the barrier_0.",
                        "text_1": "A rider is riding a horse jumping over the barrier_1.",
                        "text_2": "A rider is riding a horse jumping over the barrier_2.",
                        "text_3": "A rider is riding a horse jumping over the barrier_3.",
                        "text_4": "A rider is riding a horse jumping over the barrier_4.",
                        "image_0": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image0.jpg",
                        "image_1": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image1.jpg",
                        "image_2": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg",
                        "image_3": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image3.jpg",
                        "image_4": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image4.jpg",
                    },
                    "space field": {
                        "space child 1": "search this with space",
                        "space child 2": "test space",
                    },
                    "_id": "111",
                },

            ], mappings={"combo_text_image": {"type": "multimodal_combination", "weights": {
                "text_0 ": 0.1, "text_1": 0.1, "text_2": 0.1, "text_3": 0.1, "text_4": 0.1,
                "image_0": 0.1, "image_1": 0.1, "image_2": 0.1, "image_3": 0.1, "image_4": 0.1,
            }}, "space field": {
                "type": "multimodal_combination", "weights": {
                    "space child 1": 0.5,
                    "space child 2": 0.5,
                }}}, auto_refresh=True, tensor_fields=["combo_text_image", "space field"])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,
                              "A rider is riding a horse jumping over the barrier_0", search_method="lexical")

        lexical_res = self.client.index(self.test_index_name).search(
            "A rider is riding a horse jumping over the barrier_0", search_method="lexical")
        assert lexical_res["hits"][0]["_id"] == "111"

        # a space at the end
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,
                              "",
                              filter_string="combo_text_image.text_0\ : (A rider is riding a horse jumping over the barrier_0.)")

        filtering_res = self.client.index(self.test_index_name).search(
            "", filter_string="combo_text_image.text_0\ : (A rider is riding a horse jumping over the barrier_0.)")
        assert filtering_res["hits"][0]["_id"] == "111"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search, "")

        tensor_res = self.client.index(self.test_index_name).search("")
        assert tensor_res["hits"][0]["_id"] == "111"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,
                              "search this with space", search_method="lexical")

        space_lexical_res = self.client.index(self.test_index_name).search(
            "search this with space", search_method="lexical")
        assert space_lexical_res["hits"][0]["_id"] == "111"

        # A space in the middle
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search,
                              "", filter_string="space\ field.space\ child\ 1:(search this with space)")

        space_filtering_res = self.client.index(self.test_index_name).search(
            "", filter_string="space\ field.space\ child\ 1:(search this with space)")
        assert space_filtering_res["hits"][0]["_id"] == "111"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.test_index_name).search, "")

        space_tensor_res = self.client.index(self.test_index_name).search("")
        assert space_tensor_res["hits"][0]["_id"] == "111"

    def test_add_docs_image_download_headers(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            image_download_headers = {"Authentication": "my-secret-key"}
            self.client.index(index_name=self.generic_test_index_name).add_documents(
                documents=[{"some": "data"}], image_download_headers=image_download_headers, tensor_fields=["some"])
            args, kwargs = mock__post.call_args
            assert "imageDownloadHeaders" in kwargs['body']
            assert kwargs['body']['imageDownloadHeaders'] == image_download_headers

            return True

        assert run()

    def test_add_docs_logs_deprecation_warning_if_non_tensor_fields(self):
        # Arrange
        documents = [{'id': 'doc1', 'text': 'Test document'}]
        non_tensor_fields = ['text']

        with self.assertLogs('marqo', level='WARNING') as cm:
            self.test_index_name = self.create_test_index(self.generic_test_index_name)
            self.client.index(self.test_index_name).add_documents(documents=documents, non_tensor_fields=non_tensor_fields)
            self.assertTrue({'`non_tensor_fields`', 'Marqo', '2.0.0.'}.issubset(set(cm.output[0].split(" "))))
