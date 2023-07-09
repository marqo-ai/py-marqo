import time
from unittest.mock import patch

from marqo.marqo_url_resolver import MarqoUrlResolver
from tests.marqo_test import MarqoTestCase


class TestMarqoUrlResolver(MarqoTestCase):
    @patch("requests.get")
    def test_refresh_urls_if_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"indices": [
            {"index_name": "index1", "load_balancer_dns_name": "example.com", "index_status": "READY"},
            {"index_name": "index2", "load_balancer_dns_name": "example2.com", "index_status": "READY"}
        ]}
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)
        initial_timestamp = resolver.timestamp

        # Wait for more than the expiration time
        time.sleep(0.1)

        resolver.refresh_urls_if_needed()

        # Check that the timestamp has been updated
        print(resolver.timestamp, initial_timestamp)
        assert resolver.timestamp > initial_timestamp

        # Check that the URLs mapping has been refreshed
        assert resolver.urls_mapping == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_urls_if_not_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"indices": [
            {"index_name": "index1", "load_balancer_dns_name": "example.com", "index_status": "READY"},
            {"index_name": "index2", "load_balancer_dns_name": "example2.com", "index_status": "READY"}
        ]}
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)

        # Call refresh_urls_if_needed without waiting
        resolver.refresh_urls_if_needed()
        initial_timestamp = resolver.timestamp
        time.sleep(0.1)
        resolver.refresh_urls_if_needed()

        # Check that the timestamp has not been updated
        assert resolver.timestamp == initial_timestamp

        # Check that the URLs mapping has been initially populated
        assert resolver.urls_mapping == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_includes_only_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"indices": [
            {"index_name": "index1", "load_balancer_dns_name": "example.com", "index_status": "READY"},
            {"index_name": "index2", "load_balancer_dns_name": "example2.com", "index_status": "NOT READY"}
        ]}
        resolver = MarqoUrlResolver(api_key="your-api-key", expiration_time=60)

        # Access the urls_mapping property
        urls_mapping = resolver.urls_mapping

        # Check that the URLs mapping has been initially populated
        assert urls_mapping == {
            "index1": "example.com",
        }
