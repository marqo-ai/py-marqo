from unittest.mock import patch, MagicMock

from marqo.config import Config
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.index import Index
from unit_tests.marqo_unit_tests import MarqoUnitTests


class TestIndexSearchEndpointSortBy(MarqoUnitTests):
    """
    Test class for the Index search endpoint sort_by parameter.
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
    def test_search_by_parameters_one_fields(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response
        sort_by = {"fields":[{"fieldName": "price", "order": "asc"}]}
        self.index.search(q= "test", sort_by= sort_by)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual({"fields": [{"fieldName": "price", "order": "asc"}]}, body["sortBy"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_search_by_parameters_three_fields(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        sort_by = {
            "fields": [
                {"fieldName": "price", "order": "asc"},
                {"fieldName": "rating", "order": "desc"},
                {"fieldName": "date", "order": "asc"}
            ]
        }

        self.index.search(q= "test", sort_by=sort_by)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(sort_by, body["sortBy"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_search_by_parameters_none(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        sort_by = None

        self.index.search(q= "test", sort_by=sort_by)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("sortBy", body)

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_search_by_parameters_not_exists(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        self.index.search(q= "test")

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("sortBy", body)


class TestIndexSearchEndpointRelevanceCutoff(MarqoUnitTests):
    """
    Test class for the Index search endpoint relevance_cutoff parameter.
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
    def test_relevance_cutoff_parameter(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        relevance_cutoff = {
            "method": "mean_std_dev",
            "parameters": {
                "stdDevFactor": 1.0,
            },
            "probeDepth": 1000
        }

        self.index.search(q= "test", relevance_cutoff=relevance_cutoff)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(relevance_cutoff, body["relevanceCutoff"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_relevance_cutoff_parameter_none(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        relevance_cutoff = None

        self.index.search(q= "test", relevance_cutoff=relevance_cutoff)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("relevanceCutoff", body)

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_relevance_cutoff_parameter_not_exists(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        self.index.search(q= "test")

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("relevanceCutoff", body)

class TestIndexSearchEndpointInterpolationMethod(MarqoUnitTests):
    """
    Test class for the Index search endpoint interpolation_method parameter.
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
    def test_interpolation_method_parameter(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        interpolation_method = "SLERP"

        self.index.search(q= "test", interpolation_method=interpolation_method)

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertEqual(interpolation_method, body["interpolationMethod"])

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_interpolation_method_parameter_none(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        interpolation_method = None

        self.index.search(q= "test", interpolation_method=interpolation_method)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("interpolationMethod", body)

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_interpolation_method_parameter_not_exists(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        self.index.search(q= "test")

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertNotIn("interpolationMethod", body)
