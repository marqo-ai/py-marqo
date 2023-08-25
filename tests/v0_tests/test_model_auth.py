import json
from tests.marqo_test import MarqoTestCase
from tests.cloud_test_logic.cloud_instance_mappings import GetIndexesIndexResponseObject, mock_get_indexes_response
from marqo.utils import convert_dict_to_url_params
from unittest import mock


class TestAddDocumentsModelAuth(MarqoTestCase):
    def test_add_docs_model_auth(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            mock_s3_model_auth = {'s3': {'aws_access_key_id': 'some_acc_key',
                                         'aws_secret_access_key': 'some_sec_acc_key'}}
            self.client.index(index_name=self.generic_test_index_name).add_documents(
                documents=[{"some": "data"}], model_auth=mock_s3_model_auth, tensor_fields=["some"])
            args, kwargs = mock__post.call_args
            assert "modelAuth" in kwargs['body']
            assert kwargs['body']['modelAuth'] == mock_s3_model_auth

            return True

        assert run()

    def test_add_docs_model_client_batching(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            mock_s3_model_auth = {'s3': {'aws_access_key_id': 'some_acc_key',
                                         'aws_secret_access_key': 'some_sec_acc_key'}}
            expected_str = f"&model_auth={convert_dict_to_url_params(mock_s3_model_auth)}"
            self.client.index(index_name=self.generic_test_index_name).add_documents(
                documents=[{"some": f"data {i}"} for i in range(20)], model_auth=mock_s3_model_auth,
                client_batch_size=10, tensor_fields=["some"]
            )
            for call in mock__post.call_args_list:
                _, kwargs = call
                assert expected_str in kwargs['path'] or ('refresh' in kwargs['path'])

            assert len(mock__post.call_args_list) == 3

            return True

        assert run()

    def test_search_model_auth(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            mock_s3_model_auth = {'s3': {'aws_access_key_id': 'some_acc_key',
                                         'aws_secret_access_key': 'some_sec_acc_key'}}
            self.client.index(index_name=self.generic_test_index_name).search(
                q='something', model_auth=mock_s3_model_auth)
            args, kwargs = mock__post.call_args
            assert kwargs['body']['modelAuth'] == mock_s3_model_auth

            return True

        assert run()

    @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    def test_bulk_search_model_auth(self):
        mock__post = mock.MagicMock()

        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            mock_s3_model_auth = {'s3': {'aws_access_key_id': 'some_acc_key',
                                         'aws_secret_access_key': 'some_sec_acc_key'}}

            self.client.bulk_search([{
                "index": self.generic_test_index_name,
                "q": "a",
                "modelAuth": mock_s3_model_auth
            }])

            args, kwargs = mock__post.call_args
            assert json.loads( kwargs['body'])['queries'][0]['modelAuth'] == mock_s3_model_auth

            return True

        assert run()
