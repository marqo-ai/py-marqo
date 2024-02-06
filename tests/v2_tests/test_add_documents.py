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


@mark.fixed
class TestAddDocuments(MarqoTestCase):
    # Add documents tests:
    def test_add_documents_with_ids(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                tensor_fields = ["text_field_1", "text_field_2", "text_field_3"] if "unstr" \
                                                                                    in test_index_name else None
                d1 = {
                    "text_field_1": "Cool Document 1",
                    "text_field_2": "some extra info",
                    "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
                }
                d2 = {
                    "text_field_1": "Just Your Average Doc",
                    "text_field_2": "this is a solid doc",
                    "_id": "123456"
                }
                res = self.client.index(test_index_name).add_documents([
                    d1, d2
                ], tensor_fields=tensor_fields)
                retrieved_d1 = self.client.index(test_index_name).get_document(
                    document_id="e197e580-0393-4f4e-90e9-8cdf4b17e339")
                assert retrieved_d1 == d1
                retrieved_d2 = self.client.index(test_index_name).get_document(document_id="123456")
                assert retrieved_d2 == d2

    def test_add_documents(self):
        """indexes the documents and retrieves the documents with the generated IDs"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                tensor_fields = ["text_field_1", "text_field_2", "text_field_3"] if "unstr" \
                                                                                    in test_index_name else None
                d1 = {
                    "text_field_1": "Cool Document 1",
                    "text_field_2": "some extra info"
                }
                d2 = {
                    "text_field_1": "Just Your Average Doc",
                    "text_field_2": "this is a solid doc"
                }
                res = self.client.index(test_index_name).add_documents([d1, d2],
                                                                       tensor_fields=tensor_fields)
                ids = [item["_id"] for item in res["items"]]
                assert len(ids) == 2
                assert ids[0] != ids[1]
                retrieved_d0 = self.client.index(test_index_name).get_document(ids[0])
                retrieved_d1 = self.client.index(test_index_name).get_document(ids[1])
                del retrieved_d0["_id"]
                del retrieved_d1["_id"]
                assert retrieved_d0 == d1 or retrieved_d0 == d2
                assert retrieved_d1 == d1 or retrieved_d1 == d2

    def test_add_documents_with_ids_twice(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                tensor_fields = ["text_field_1", "text_field_2", "text_field_3"] if "unstr" \
                                                                                    in test_index_name else None
                d1 = {
                    "text_field_1": "Just Your Average Doc",
                    "text_field_2": "this is a solid doc",
                    "_id": "56"
                }
                self.client.index(test_index_name).add_documents([d1], tensor_fields=tensor_fields)
                assert d1 == self.client.index(test_index_name).get_document("56")
                d2 = {
                    "_id": "56",
                    "text_field_1": "different doc.",
                    "text_field_2": "this is a solid doc"
                }
                self.client.index(test_index_name).add_documents([d2], tensor_fields=tensor_fields)
                assert d2 == self.client.index(test_index_name).get_document("56")

    def test_add_batched_documents(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                tensor_fields = ["text_field_1", "text_field_2", "text_field_3"] if "unstr" \
                                                                                    in test_index_name else None
                doc_ids = [str(num) for num in range(0, 100)]

                docs = [
                    {"text_field_1": f"The Title of doc {doc_id}",
                     "text_field_2": "some text goes here...",
                     "_id": doc_id}
                    for doc_id in doc_ids]
                assert len(docs) == 100
                self.client.index(test_index_name).add_documents(docs,
                                                                 client_batch_size=4, tensor_fields=tensor_fields)
                time.sleep(3)
                # takes too long to search for all...
                for _id in [0, 19, 20, 99]:
                    original_doc = docs[_id].copy()
                    assert self.client.index(test_index_name).get_document(document_id=str(_id)) == original_doc
                assert self.client.index(index_name=test_index_name).get_stats()['numberOfDocuments'] == 100

    def test_add_documents_long_fields(self):
        """TODO
        """

    def test_update_docs_updates_chunks(self):
        """TODO"""

    def test_get_document(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                tensor_fields = ["text_field_1", "text_field_2", "text_field_3"] if "unstr" \
                                                                                    in test_index_name else None
                my_doc = {"text_field_1": "efg", "_id": "123"}
                self.client.index(test_index_name).add_documents([my_doc], tensor_fields=tensor_fields)
                retrieved = self.client.index(test_index_name).get_document(document_id='123')
                assert retrieved == my_doc

    def test_add_documents_missing_index_fails(self):
        with pytest.raises((MarqoError, MarqoWebError)) as ex:
            self.client.index("somenonexistingindex").add_documents([{"abd": "efg"}])

        assert ex.value.code in ["index_not_found", "index_not_found_cloud"]

    def test_add_documents_with_device(self):
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.unstructured_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2": "some data"}
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
            temp_client.index(self.unstructured_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2": "some data"}, {"d2331": "blah"}, {"45d2": "some data"}
            ], client_batch_size=2, device="cuda:37", tensor_fields=["d1", "d2", "d2331", "45d2"])
            return True

        assert run()

        print(mock__post.call_args_list)
        assert len(mock__post.call_args_list) == 2  # 2 batches, no refresh
        for args, kwargs in mock__post.call_args_list:
            assert "device=cuda37" in kwargs["path"]

    def test_add_documents_query_string_unbatched(self):
        """
        Ensures that the query string (no client batching) is properly constructed.
        This string consists of refresh and device parameters.
        """
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            # Neither device nor auto-refresh set
            temp_client.index(self.generic_test_index_name).add_documents(
                documents=[{"d1": "blah"}],
                tensor_fields=["d1"],
            )

            # Only device set
            temp_client.index(self.generic_test_index_name).add_documents(
                documents=[{"d1": "blah"}],
                tensor_fields=["d1"],
                device="cpu"
            )

            # Only auto-refresh set
            temp_client.index(self.generic_test_index_name).add_documents(
                documents=[{"d1": "blah"}],
                tensor_fields=["d1"]
            )

            # Both device and auto-refresh set
            temp_client.index(self.generic_test_index_name).add_documents(
                documents=[{"d1": "blah"}],
                tensor_fields=["d1"],
                device="cpu"
            )
            return True

        assert run()

        args, kwargs0 = mock__post.call_args_list[0]
        assert "device" not in kwargs0["path"]
        args, kwargs1 = mock__post.call_args_list[1]
        assert "?device=cpu" in kwargs1["path"]
        args, kwargs2 = mock__post.call_args_list[2]
        assert "device" not in kwargs2["path"]
        args, kwargs3 = mock__post.call_args_list[3]
        assert "?device=cpu" in kwargs3["path"]

    def test_add_documents_defaults(self):
        """
        Ensure that the expected default values are used for the add documents API call
        Note that some parameters should have no default created by the client, thus should
        not be present in the request.
        """
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).add_documents(
                documents=[{"d1": "blah"}, {"d2": "some data"}],
                tensor_fields=["d1", "d2"]
            )
            return True

        assert run()

        args, kwargs0 = mock__post.call_args_list[0]

        # Ensure client does NOT autofill refresh and device parameters
        assert "refresh" not in kwargs0["path"]
        assert "device" not in kwargs0["path"]

        assert kwargs0["body"]["useExistingTensors"] == False
        assert kwargs0["body"]["imageDownloadHeaders"] == {}
        assert kwargs0["body"]["mappings"] is None
        assert kwargs0["body"]["modelAuth"] is None

        # These parameters were explicitly defined:
        assert kwargs0["body"]["documents"] == [{'d1': 'blah'}, {'d2': 'some data'}]
        assert kwargs0["body"]["tensorFields"] == ['d1', 'd2']

    def test_add_documents_with_no_processes(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            self.client.index(self.generic_test_index_name).add_documents(documents=[
                {"d1": "blah"}, {"d2": "some data"}
            ], tensor_fields=["d1", "d2"])
            return True

        assert run()

        args, kwargs = mock__post.call_args
        assert "processes=12" not in kwargs["path"]

    def test_resilient_indexing(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            if self.IS_MULTI_INSTANCE:
                time.sleep(1)

            assert 0 == self.client.index(test_index_name).get_stats()['numberOfDocuments']
            d1 = {"d1": "blah", "_id": "1234"}
            d2 = {"d2": "blah", "_id": "5678"}
            docs = [d1, {"content": "some terrible doc", "d3": "blah", "_id": 12345}, d2]
            self.client.index(test_index_name).add_documents(documents=docs, tensor_fields=["d1", "d2", "d3", "content"],)

            if self.IS_MULTI_INSTANCE:
                time.sleep(1)

            assert 2 == self.client.index(test_index_name).get_stats()['numberOfDocuments']
            assert d1 == self.client.index(test_index_name).get_document(document_id='1234')
            assert d2 == self.client.index(test_index_name).get_document(document_id='5678')

            if self.IS_MULTI_INSTANCE:
                time.sleep(1)
            assert {"1234", "5678"} == {d['_id'] for d in
                                        self.client.index(test_index_name).search("blah", limit=3)['hits']}

    def test_batching_add_docs(self):

        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
        docs_to_add = 250
        vocab = requests.get(vocab_source).text.splitlines()
        docs = [{"Title": " ".join(random.choices(population=vocab, k=10)),
                 "Description": " ".join(random.choices(population=vocab, k=25)),
                 } for _ in range(docs_to_add)]

        batches = [None, 1, 2, 50]
        for client_batch_size in batches:
            mock__post = mock.MagicMock()
            mock__post.return_value = dict()

            @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
            def run():
                res = self.client.index(self.generic_test_index_name).add_documents(
                    documents=docs, client_batch_size=client_batch_size,
                    tensor_fields=["Title", "Description"]
                )
                if client_batch_size is not None:
                    assert isinstance(res, list)
                    assert len(res) == math.ceil(docs_to_add / client_batch_size)
                    # should only refresh on the last call, if auto_refresh=True
                else:
                    assert isinstance(res, dict)
                    # One huge request is made, if there is no client_side_batching:
                    assert all([len(d[1]['body']["documents"]) == docs_to_add for d in mock__post.call_args_list])

                assert all(['batch' not in d[1]['path'] for d in mock__post.call_args_list])

                assert all(['processes' not in d[1]['path'] for d in mock__post.call_args_list])

                return True

            assert run()

    def test_add_lists(self):
        original_doc = {"d1": "blah", "_id": "1234", 'my_list': ['tag-1', 'tag-2']}
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents(documents=[original_doc], tensor_fields=['d1'])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                                  q='something', filter_string='my_list:tag-1'
                                  )

            res = self.client.index(test_index_name).search(
                q='something', filter_string='my_list:tag-1'
            )
            assert res['hits'][0]['_id'] == '1234'

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                                  q='something', filter_string='my_list:tag-non-existent'
                                  )

            bad_res = self.client.index(test_index_name).search(
                q='something', filter_string='my_list:tag-non-existent'
            )
            assert len(bad_res['hits']) == 0

    # TODO: Fix when use_existing_tensors bug is fixed
    # def test_use_existing_fields(self):
    #     for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
    #         test_index_name = self.get_test_index_name(
    #             cloud_test_index_to_use=cloud_test_index_to_use,
    #             open_source_test_index_name=open_source_test_index_name
    #         )
    #         self.client.index(index_name=test_index_name).add_documents(
    #             documents=[
    #                 {
    #                     "_id": "123",
    #                     "title_1": "content 1",
    #                     "desc_2": "content 2. blah blah blah",
    #                     "old": "some other cool thing"
    #                 }],
    #             tensor_fields=["title_1", "old"]
    #         )
    #
    #         assert {"title_1", "_embedding", "old"} == functools.reduce(
    #             lambda x, y: x.union(y),
    #             [set(facet.keys()) for facet in
    #              self.client.index(index_name=test_index_name).get_document(
    #                  document_id="123", expose_facets=True)["_tensor_facets"]]
    #         )
    #         self.client.index(index_name=test_index_name).add_documents(
    #             documents=[
    #                 {
    #                     "_id": "123",
    #                     "title_1": "content 1",
    #                     "desc_2": "content 2. blah blah blah",
    #                     "new_f": "12345 "
    #                 }], use_existing_tensors=True, tensor_fields=["desc 2", "new f", "title 1"]
    #         )
    #         # we don't get desc 2 facets, because it was already a non_tensor_field
    #         assert {"title_1", "_embedding", "new_f"} == functools.reduce(
    #             lambda x, y: x.union(y),
    #             [set(facet.keys()) for facet in
    #              self.client.index(index_name=test_index_name).get_document(
    #                  document_id="123", expose_facets=True)["_tensor_facets"]]
    #         )

    def test_multimodal_combination_doc(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            open_source_test_index_name = self.unstructured_image_index_name
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=test_index_name).add_documents(
                documents=[
                    {
                        "text_0": "jumping barrier_0.",
                        "text_1": "jumping barrier_1.",
                        "text_2": "jumping barrier_2.",
                        "text_3": "jumping barrier_3.",
                        "text_4": "jumping barrier_4.",
                        "image_0": "https://marqo-assets.s3.amazonaws.com/tests/images/image0.jpg",
                        "image_1": "https://marqo-assets.s3.amazonaws.com/tests/images/image1.jpg",
                        "image_2": "https://marqo-assets.s3.amazonaws.com/tests/images/image2.jpg",
                        "image_3": "https://marqo-assets.s3.amazonaws.com/tests/images/image3.jpg",
                        "image_4": "https://marqo-assets.s3.amazonaws.com/tests/images/image4.jpg",
                        "space_child_1": "search with space",
                        "space_child_2": "test space",
                        "_id": "111",
                    },

                ], mappings={"combo_text_image": {"type": "multimodal_combination", "weights": {
                    "text_0": 0.1, "text_1": 0.1, "text_2": 0.1, "text_3": 0.1, "text_4": 0.1,
                    "image_0": 0.1, "image_1": 0.1, "image_2": 0.1, "image_3": 0.1, "image_4": 0.1,
                }}, "space_field": {
                    "type": "multimodal_combination", "weights": {
                        "space_child_1": 0.5,
                        "space_child_2": 0.5,
                    }}}, tensor_fields=["combo_text_image", "space_field"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                                  "jumping barrier_0", search_method="lexical")

            lexical_res = self.client.index(test_index_name).search(
                "jumping barrier_0", search_method="lexical")
            assert lexical_res["hits"][0]["_id"] == "111"

            # a space at the end
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                                  "",
                                  filter_string="text_0:(jumping barrier_0.)")

            filtering_res = self.client.index(test_index_name).search(
                "", filter_string="text_0:(jumping barrier_0.)")
            assert filtering_res["hits"][0]["_id"] == "111"

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search, "")

            tensor_res = self.client.index(test_index_name).search("")
            assert tensor_res["hits"][0]["_id"] == "111"

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                                  "search with space", search_method="lexical")

            space_lexical_res = self.client.index(test_index_name).search(
                "search with space", search_method="lexical")
            assert space_lexical_res["hits"][0]["_id"] == "111"

            # A space in the middle
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                                  "", filter_string="space_child_1:(search this with space)")

            space_filtering_res = self.client.index(test_index_name).search(
                "", filter_string="space_child_1:(search with space)")
            assert space_filtering_res["hits"][0]["_id"] == "111"

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search, "")

            space_tensor_res = self.client.index(test_index_name).search("")
            assert space_tensor_res["hits"][0]["_id"] == "111"

    # TODO: Fix test when custom_vector is fixed
    # def test_custom_vector_doc(self):
    #     """
    #     Tests the custom_vector field type.
    #     Ensures the following features work on this field:
    #     1. lexical search
    #     2. filter string search
    #     3. tensor search
    #     4. get document
    #     """
    #
    #     for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
    #         test_index_name = self.get_test_index_name(
    #             cloud_test_index_to_use=cloud_test_index_to_use,
    #             open_source_test_index_name=open_source_test_index_name
    #         )
    #         self.client.index(index_name=test_index_name).add_documents(
    #             documents=[
    #                 {
    #                     "my_custom_vector": {
    #                         "content": "custom vector text",
    #                         "vector": [1.0 for _ in range(512)],
    #                     },
    #                     "my_normal_text_field": "normal text",
    #                     "_id": "doc1",
    #                 },
    #                 {
    #                     "my_normal_text_field": "second doc",
    #                     "_id": "doc2"
    #                 }
    #             ], mappings={
    #                 "my_custom_vector": {
    #                     "type": "custom_vector",
    #                 }
    #             },
    #             tensor_fields=["my_custom_vector"])
    #
    #         # lexical search test
    #         if self.IS_MULTI_INSTANCE:
    #             self.warm_request(self.client.index(test_index_name).search,
    #                               "custom vector text", search_method="lexical")
    #
    #         lexical_res = self.client.index(test_index_name).search(
    #             "custom vector text", search_method="lexical")
    #         assert lexical_res["hits"][0]["_id"] == "doc1"
    #
    #         # filter string test
    #         if self.IS_MULTI_INSTANCE:
    #             self.warm_request(self.client.index(test_index_name).search,
    #                               "",
    #                               filter_string="my_custom_vector:(custom vector text)")
    #
    #         filtering_res = self.client.index(test_index_name).search(
    #             "", filter_string="my_custom_vector:(custom vector text)")
    #         assert filtering_res["hits"][0]["_id"] == "doc1"
    #
    #         # tensor search test
    #         if self.IS_MULTI_INSTANCE:
    #             self.warm_request(self.client.index(test_index_name).search, q={"dummy text": 0},
    #                               context={"tensor": [{"vector": [1.0 for _ in range(512)], "weight": 1}]})
    #
    #         tensor_res = self.client.index(test_index_name).search(q={"dummy text": 0}, context={
    #             "tensor": [{"vector": [1.0 for _ in range(512)], "weight": 1}]})
    #         assert tensor_res["hits"][0]["_id"] == "doc1"
    #
    #         # get document test
    #         doc_res = self.client.index(test_index_name).get_document(
    #             document_id="doc1",
    #             expose_facets=True
    #         )
    #         assert doc_res["my_custom_vector"] == "custom vector text"
    #         assert doc_res['_tensor_facets'][0]["my_custom_vector"] == "custom vector text"
    #         assert doc_res['_tensor_facets'][0]['_embedding'] == [1.0 for _ in range(512)]

    # TODO: Fix test when custom_vector is fixed
    # @mark.ignore_during_cloud_tests
    # def test_no_model_custom_vector_doc(self):
    #     """
    #     Tests the `no_model` index model and searching with no `q` parameter.
    #     Executed on documents with custom_vector field type.
    #
    #     Ensures the following features work on this index:
    #     1. lexical search
    #     2. filter string search
    #     3. tensor search
    #     4. bulk search
    #     5. get document
    #
    #     Note: `no_model` is not yet supported on Cloud.
    #     """
    #     settings = {
    #         "index_defaults": {
    #             "model": "no_model",
    #             "model_properties": {
    #                 "dimensions": 123
    #             }
    #         }
    #     }
    #
    #     for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
    #         test_index_name = self.get_test_index_name(
    #             cloud_test_index_to_use=cloud_test_index_to_use,
    #             open_source_test_index_name=open_source_test_index_name
    #         )
    #
    #         custom_vector_1 = [1.0 for _ in range(123)]
    #         custom_vector_2 = [i for i in range(123)]
    #         custom_vector_3 = [1 / (i + 1) for i in range(123)]
    #
    #         self.client.index(index_name=test_index_name).add_documents(
    #             documents=[
    #                 {
    #                     "my_custom_vector": {
    #                         "content": "custom vector text",
    #                         "vector": custom_vector_1,
    #                     },
    #                     "_id": "doc1",
    #                 },
    #                 {
    #                     "my_custom_vector": {
    #                         "content": "second text",
    #                         "vector": custom_vector_2,
    #                     },
    #                     "_id": "doc2",
    #                 },
    #                 {
    #                     "my_custom_vector": {
    #                         "content": "third text",
    #                         "vector": custom_vector_3,
    #                     },
    #                     "_id": "doc3",
    #                 },
    #             ], mappings={
    #                 "my_custom_vector": {
    #                     "type": "custom_vector"
    #                 }
    #             },
    #             tensor_fields=["my_custom_vector"])
    #
    #         # lexical search test
    #         if self.IS_MULTI_INSTANCE:
    #             self.warm_request(self.client.index(test_index_name).search,
    #                               "custom vector text", search_method="lexical")
    #
    #         lexical_res = self.client.index(test_index_name).search(
    #             "custom vector text", search_method="lexical")
    #         assert lexical_res["hits"][0]["_id"] == "doc1"
    #
    #         # filter string test
    #         if self.IS_MULTI_INSTANCE:
    #             self.warm_request(self.client.index(test_index_name).search,
    #                               context={"tensor": [{"vector": custom_vector_2, "weight": 1}]},
    #                               filter_string="my_custom_vector:(second text)")
    #
    #         filtering_res = self.client.index(test_index_name).search(
    #             context={"tensor": [{"vector": custom_vector_2, "weight": 1}]},  # no text query
    #             filter_string="my_custom_vector:(second text)")
    #         assert filtering_res["hits"][0]["_id"] == "doc2"
    #
    #         # tensor search test
    #         if self.IS_MULTI_INSTANCE:
    #             self.warm_request(self.client.index(test_index_name).search,
    #                               context={"tensor": [{"vector": custom_vector_3, "weight": 1}]})
    #
    #         tensor_res = self.client.index(test_index_name).search(
    #             context={"tensor": [{"vector": custom_vector_3, "weight": 1}]}  # no text query
    #         )
    #         assert tensor_res["hits"][0]["_id"] == "doc3"
    #
    #         # bulk search test
    #         resp = self.client.bulk_search([{
    #             "index": test_index_name,
    #             "context": {"tensor": [{"vector": custom_vector_1, "weight": 1}]},  # no text query
    #         }])
    #         assert len(resp['result']) == 1
    #         search_res = resp['result'][0]
    #         assert search_res["hits"][0]["_id"] == "doc1"
    #
    #         # get document test
    #         doc_res = self.client.index(test_index_name).get_document(
    #             document_id="doc1",
    #             expose_facets=True
    #         )
    #         assert doc_res["my_custom_vector"] == "custom vector text"
    #         assert doc_res['_tensor_facets'][0]["my_custom_vector"] == "custom vector text"
    #         assert doc_res['_tensor_facets'][0]['_embedding'] == custom_vector_1

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

    def test_add_empty_docs(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            try:
                res = self.client.index(test_index_name).add_documents(documents=[], tensor_fields=["field a"])
                raise AssertionError
            except MarqoWebError as e:
                assert e.code == "bad_request"
                assert "empty add documents request" in e.message["message"]

    def test_add_empty_docs_batched(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            res = self.client.index(test_index_name).add_documents(documents=[], client_batch_size=5,
                                                                   tensor_fields="field a")
            assert res == []