import time
from unittest.mock import patch

from marqo.marqo_url_resolver import MarqoUrlResolver
from tests.marqo_test import MarqoTestCase


class TestMarqoUrlResolver(MarqoTestCase):
    @patch("requests.get")
    def test_refresh_urls_if_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)
        initial_timestamp = resolver.timestamp

        # Wait for more than the expiration time
        time.sleep(0.1)

        resolver.refresh_urls_if_needed("index1")

        # Check that the timestamp has been updated
        print(resolver.timestamp, initial_timestamp)
        assert resolver.timestamp > initial_timestamp

        # Check that the URLs mapping has been refreshed
        assert resolver._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_urls_if_not_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)

        # Call refresh_urls_if_needed without waiting
        resolver.refresh_urls_if_needed("index1")
        initial_timestamp = resolver.timestamp
        time.sleep(0.1)
        resolver.refresh_urls_if_needed("index2")

        # Check that the timestamp has not been updated
        assert resolver.timestamp == initial_timestamp

        # Check that the URLs mapping has been initially populated
        assert resolver._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_includes_only_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "NOT READY"}
        ]}
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)

        # Access the urls_mapping property
        resolver.refresh_urls_if_needed("index1")
        urls_mapping = resolver._urls_mapping

        # Check that the URLs mapping has been initially populated
        assert urls_mapping["READY"] == {
            "index1": "example.com",
        }

    def test_refresh_urls_graceful_timeout_handling(self):
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)
        # use ridiculously low timeout
        with self.assertLogs('marqo', level='WARNING') as cm:
            resolver._refresh_urls(timeout=0.0000000001)
            assert "timeout" in cm.output[0].lower()
            assert "marqo cloud indexes" in cm.output[0].lower()

    @patch("requests.get")
    def test_refresh_urls_graceful_timeout_handling_http_timeout(self, mock_get):
        from requests.exceptions import Timeout
        mock_get.side_effect = Timeout
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)
        with self.assertLogs('marqo', level='WARNING') as cm:
            resolver._refresh_urls(timeout=5)
            assert "timeout" in cm.output[0].lower()
            assert "marqo cloud indexes" in cm.output[0].lower()
