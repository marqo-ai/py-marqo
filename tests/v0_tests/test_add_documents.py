import copy
import math
import pprint
import random
import requests
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase
from marqo import enums
from unittest import mock


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

    # Create index tests

    def test_create_index(self):
        self.client.create_index(index_name=self.index_name_1)

    def test_create_index_double(self):
        self.client.create_index(index_name=self.index_name_1)
        try:
            self.client.create_index(index_name=self.index_name_1)
        except MarqoWebError as e:
            assert "index_already_exists" == e.code

    # Delete index tests:

    def test_delete_index(self):
        self.client.create_index(index_name=self.index_name_1)
        self.client.delete_index(self.index_name_1)
        self.client.create_index(index_name=self.index_name_1)

    # Get index tests:

    def test_get_index(self):
        self.client.create_index(index_name=self.index_name_1)
        index = self.client.get_index(self.index_name_1)
        assert index.index_name == self.index_name_1

    def test_get_index_non_existent(self):
        try:
            index = self.client.get_index("some-non-existent-index")
            raise AssertionError
        except MarqoWebError as e:
            assert e.code == "index_not_found"

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
        ])
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
        res = self.client.index(self.index_name_1).add_documents([d1, d2])
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
        self.client.index(self.index_name_1).add_documents([d1])
        assert d1 == self.client.index(self.index_name_1).get_document("56")
        d2 = {
            "_id": "56",
            "completely": "different doc.",
            "field X": "this is a solid doc"
        }
        self.client.index(self.index_name_1).add_documents([d2])
        assert d2 == self.client.index(self.index_name_1).get_document("56")

    def test_add_batched_documents(self):
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError:
            pass
        ix = self.client.index(index_name=self.index_name_1)
        doc_ids = [str(num) for num in range(0, 100)]

        docs = [
            {"Title": f"The Title of doc {doc_id}",
             "Generic text": "some text goes here...",
             "_id": doc_id}
            for doc_id in doc_ids]
        assert len(docs) == 100
        ix.add_documents(docs, server_batch_size=5, client_batch_size=4)
        ix.refresh()
        # takes too long to search for all...
        for _id in [0, 19, 20, 99]:
            original_doc = docs[_id].copy()
            assert ix.get_document(document_id=str(_id)) == original_doc
        assert self.client.index(index_name=self.index_name_1).get_stats()['numberOfDocuments'] == 100

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
        ])
        res0 = self.client.index(self.index_name_1).search("wow camel")
        print("res0res0")
        pprint.pprint(res0)
        assert res0['hits'][0]["_id"] == "123"
        assert len(res0['hits']) == 2
        self.client.index(self.index_name_1).delete_documents(["123"])
        res1 = self.client.index(self.index_name_1).search("wow camel")
        assert res1['hits'][0]["_id"] == "foo"
        assert len(res1['hits']) == 1

    def test_delete_docs_empty_ids(self):
        self.client.create_index(index_name=self.index_name_1)
        self.client.index(self.index_name_1).add_documents([{"abc": "efg", "_id": "123"}])
        try:
            self.client.index(self.index_name_1).delete_documents([])
            raise AssertionError
        except MarqoWebError as e:
            assert "can't be empty" in str(e) or "value_error.missing" in str(e)
        res = self.client.index(self.index_name_1).get_document("123")
        print(res)
        assert "abc" in res

    def test_get_document(self):
        my_doc = {"abc": "efg", "_id": "123"}
        self.client.create_index(index_name=self.index_name_1)
        self.client.index(self.index_name_1).add_documents([my_doc])
        retrieved = self.client.index(self.index_name_1).get_document(document_id='123')
        assert retrieved == my_doc

    def test_add_documents_implicitly_create_index(self):
        try:
            self.client.index(self.index_name_1).search("some str")
            raise AssertionError
        except MarqoWebError as s:
            assert "index_not_found" == s.code
        self.client.index(self.index_name_1).add_documents([{"abd": "efg"}])
        # it works:
        self.client.index(self.index_name_1).search("some str")

    def test_add_documents_with_device(self):
        temp_client = copy.deepcopy(self.client)
        temp_client.config.search_device = enums.Devices.cpu
        temp_client.config.indexing_device = enums.Devices.cpu

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], device="cuda:45")
            return True
        assert run()

        args, kwargs = mock__post.call_args
        assert "device=cuda45" in kwargs["path"]

    def test_add_documents_with_device_batching(self):
        temp_client = copy.deepcopy(self.client)
        temp_client.config.search_device = enums.Devices.cpu
        temp_client.config.indexing_device = enums.Devices.cpu

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}, {"d2331": "blah"}, {"45d2", "some data"}
            ], client_batch_size=2, device="cuda:37")
            return True
        assert run()
        assert len(mock__post.call_args_list) == 3
        for args, kwargs in mock__post.call_args_list[:-1]:
            assert "device=cuda37" in kwargs["path"]

    def test_add_documents_default_device(self):
        """do we use the default device defined in the client, if it isn't
        overridden?
        """
        temp_client = copy.deepcopy(self.client)
        temp_client.config.search_device = enums.Devices.cpu
        temp_client.config.indexing_device = "cuda:28"

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ])
            return True
        assert run()

        args, kwargs = mock__post.call_args
        assert "device=cuda28" in kwargs["path"]

    def test_add_documents_set_refresh(self):
        temp_client = copy.deepcopy(self.client)
        temp_client.config.search_device = enums.Devices.cpu
        temp_client.config.indexing_device = enums.Devices.cpu

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], auto_refresh=False)
            temp_client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], auto_refresh=True)
            return True
        assert run()

        args, kwargs0 = mock__post.call_args_list[0]
        assert "refresh=false" in kwargs0["path"]
        args, kwargs1 = mock__post.call_args_list[1]
        assert "refresh=true" in kwargs1["path"]

    def test_add_documents_with_processes(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            self.client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ], processes=12)
            return True
        assert run()

        args, kwargs = mock__post.call_args
        assert "processes=12" in kwargs["path"]

    def test_add_documents_with_no_processes(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            self.client.index(self.index_name_1).add_documents(documents=[
                {"d1": "blah"}, {"d2", "some data"}
            ])
            return True
        assert run()

        args, kwargs = mock__post.call_args
        assert "processes=12" not in kwargs["path"]

    def test_update_documents(self):
        original_doc = {"d1": "blah", "_id": "1234"}
        self.client.index(self.index_name_1).add_documents(documents=[original_doc])
        assert original_doc == self.client.index(self.index_name_1).get_document(document_id='1234')
        new_doc = {"_id": "brand_new", "Content": "fascinating"}
        self.client.index(self.index_name_1).update_documents(documents=[
            {"_id": "1234", "new_field": "some data"}, new_doc])
        assert {"new_field": "some data", **original_doc} == self.client.index(self.index_name_1).get_document(
            document_id='1234')
        assert new_doc == self.client.index(self.index_name_1).get_document(document_id='brand_new')

    def test_resilient_indexing(self):
        self.client.create_index(self.index_name_1)
        assert 0 == self.client.index(self.index_name_1).get_stats()['numberOfDocuments']
        d1 = {"d1": "blah", "_id": "1234"}
        d2 = {"d2": "blah", "_id": "5678"}
        docs = [d1,  {"content": "some terrible doc", "d3": "blah", "_id": 12345}, d2]
        self.client.index(self.index_name_1).add_documents(documents=docs)
        assert 2 == self.client.index(self.index_name_1).get_stats()['numberOfDocuments']
        assert d1 == self.client.index(self.index_name_1).get_document(document_id='1234')
        assert d2 == self.client.index(self.index_name_1).get_document(document_id='5678')
        assert {"1234", "5678"} == {d['_id'] for d in
                                    self.client.index(self.index_name_1).search("blah", limit=3)['hits']}

    def test_batching_add_docs(self):

        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
        docs_to_add = 250
        vocab = requests.get(vocab_source).text.splitlines()
        docs = [{"Title": " ".join(random.choices(population=vocab, k=10)),
          "Description": " ".join(random.choices(population=vocab, k=25)),
          } for _ in range(docs_to_add)]

        batches = [None, 1, 2, 50]
        for auto_refresh in (None, True, False):
            for processes in [None, 1, 2]:
                for client_batch_size in batches:
                    for server_batch_size in batches:
                        mock__post = mock.MagicMock()
                        mock__post.return_value = dict()
                        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
                        def run():
                            res = self.client.index(self.index_name_1).add_documents(
                                auto_refresh=auto_refresh, documents=docs, client_batch_size=client_batch_size,
                                server_batch_size=server_batch_size, processes=processes)
                            if client_batch_size is not None:
                                assert isinstance(res, list)
                                assert len(res) == math.ceil(docs_to_add/client_batch_size)
                                # should only refresh on the last call, if auto_refresh=True
                                assert all([f'refresh=false' in d[1]['path'] for d in
                                            mock__post.call_args_list][:-1])
                                if auto_refresh:
                                    assert [f"{self.index_name_1}/refresh" in d[1]['path']
                                            for d in mock__post.call_args_list][-1]
                            else:
                                assert isinstance(res, dict)
                                # One huge request is made, if there is no client_side_batching:
                                assert all([len(d[1]['body']) == docs_to_add for d in mock__post.call_args_list])
                            if server_batch_size is not None:
                                if auto_refresh:
                                    assert all([f'batch_size={server_batch_size}' in d[1]['path'] for d in
                                                mock__post.call_args_list][:-1])
                                else:
                                    assert all([f'batch_size={server_batch_size}' in d[1]['path']
                                                for d in mock__post.call_args_list])
                            else:
                                assert all(['batch' not in d[1]['path'] for d in mock__post.call_args_list])

                            if processes is not None:
                                if auto_refresh is True and client_batch_size is not None:
                                    assert [f'processes={processes}' in d[1]['path'] for d in mock__post.call_args_list][: -1]

                                else:
                                    assert all([f'processes={processes}' in d[1]['path']
                                                for d in mock__post.call_args_list])
                            else:
                                assert all(['processes' not in d[1]['path'] for d in mock__post.call_args_list])

                            return True
                        assert run()

    def test_batching_update_docs(self):

        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
        docs_to_add = 250
        vocab = requests.get(vocab_source).text.splitlines()
        docs = [{"Title": " ".join(random.choices(population=vocab, k=10)),
          "Description": " ".join(random.choices(population=vocab, k=25)),
          } for _ in range(docs_to_add)]

        batches = [None, 1, 2, 50]
        for auto_refresh in (None, True, False):
            for processes in [None, 1, 2]:
                for client_batch_size in batches:
                    for server_batch_size in batches:
                        mock__put = mock.MagicMock()
                        mock__put.return_value = dict()
                        mock__post = copy.deepcopy(mock__put)

                        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
                        @mock.patch("marqo._httprequests.HttpRequests.put", mock__put)
                        def run():
                            res = self.client.index(self.index_name_1).update_documents(
                                auto_refresh=auto_refresh, documents=docs, client_batch_size=client_batch_size,
                                server_batch_size=server_batch_size, processes=processes)
                            if client_batch_size is not None:
                                assert isinstance(res, list)
                                assert len(res) == math.ceil(docs_to_add/client_batch_size)
                                # should only refresh on the last call, if auto_refresh=True
                                assert all([f'refresh=false' in d[1]['path'] for d in
                                            mock__put.call_args_list][:-1])
                                if auto_refresh:
                                    assert [f"{self.index_name_1}/refresh" in d[1]['path']
                                            for d in mock__post.call_args_list][-1]
                            else:
                                assert isinstance(res, dict)
                                # One huge request is made, if there is no client_side_batching:
                                assert all([len(d[1]['body']) == docs_to_add for d in mock__put.call_args_list])
                            if server_batch_size is not None:
                                if auto_refresh:
                                    assert all([f'batch_size={server_batch_size}' in d[1]['path'] for d in
                                                mock__put.call_args_list][:-1])
                                else:
                                    assert all([f'batch_size={server_batch_size}' in d[1]['path']
                                                for d in mock__put.call_args_list])
                            else:
                                assert all(['batch' not in d[1]['path'] for d in mock__put.call_args_list])

                            if processes is not None:
                                if auto_refresh is True and client_batch_size is not None:
                                    assert [f'processes={processes}' in d[1]['path'] for d in mock__put.call_args_list][: -1]

                                else:
                                    assert all([f'processes={processes}' in d[1]['path']
                                                for d in mock__put.call_args_list])
                            else:
                                assert all(['processes' not in d[1]['path'] for d in mock__put.call_args_list])

                            return True
                        assert run()
