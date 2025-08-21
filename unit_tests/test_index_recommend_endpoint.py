from unittest.mock import patch, MagicMock

from marqo.config import Config
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.index import Index
from unit_tests.marqo_unit_tests import MarqoUnitTests


class TestIndexRecommendEndpoint(MarqoUnitTests):
    """
    Test class for the Index recommend endpoint parameters.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the class by creating a test index.
        """
        config = Config(
            instance_mappings=DefaultInstanceMappings(url="http://unit-tests-url:8882"),
        )
        cls.index = Index(config=config, index_name="test_index")

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_documents_parameter_true(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        allow_missing_documents = True

        self.index.recommend(documents=["doc1", "doc2"], allow_missing_documents=allow_missing_documents)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(allow_missing_documents, body["allowMissingDocuments"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_documents_parameter_false(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        allow_missing_documents = False

        self.index.recommend(documents=["doc1", "doc2"], allow_missing_documents=allow_missing_documents)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(allow_missing_documents, body["allowMissingDocuments"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_documents_parameter_none(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        allow_missing_documents = None

        self.index.recommend(documents=["doc1", "doc2"], allow_missing_documents=allow_missing_documents)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("allowMissingDocuments", body)

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_documents_parameter_not_exists(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        self.index.recommend(documents=["doc1", "doc2"])

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("allowMissingDocuments", body)

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_embeddings_parameter_true(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        allow_missing_embeddings = True

        self.index.recommend(documents=["doc1", "doc2"], allow_missing_embeddings=allow_missing_embeddings)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(allow_missing_embeddings, body["allowMissingEmbeddings"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_embeddings_parameter_false(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        allow_missing_embeddings = False

        self.index.recommend(documents=["doc1", "doc2"], allow_missing_embeddings=allow_missing_embeddings)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(allow_missing_embeddings, body["allowMissingEmbeddings"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_embeddings_parameter_none(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        allow_missing_embeddings = None

        self.index.recommend(documents=["doc1", "doc2"], allow_missing_embeddings=allow_missing_embeddings)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("allowMissingEmbeddings", body)

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_allow_missing_embeddings_parameter_not_exists(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        self.index.recommend(documents=["doc1", "doc2"])

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("allowMissingEmbeddings", body)