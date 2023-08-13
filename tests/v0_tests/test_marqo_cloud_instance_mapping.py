import time
from unittest.mock import patch

from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from tests.marqo_test import MarqoTestCase
from marqo.errors import MarqoCloudIndexNotFoundError,MarqoCloudIndexNotReadyError


class TestMarqoCloudInstanceMappings(MarqoTestCase):
    @patch("requests.get")
    def test_refresh_urls_if_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai",api_key="your-api-key", url_cache_duration=60
        )
        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp

        # Wait some time to see that timestamp is updated and it is higher than initial one after refresh
        time.sleep(0.1)

        mapping._refresh_urls_if_needed("index1")

        # Check that the timestamp has been updated
        assert mapping.latest_index_mappings_refresh_timestamp > initial_timestamp

        # Check that the URLs mapping has been refreshed
        assert mapping._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_urls_if_not_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        # Call refresh_urls_if_needed without waiting
        mapping._refresh_urls_if_needed("index1")
        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp
        time.sleep(0.1)
        # Since index is loaded in cache, it should not be refreshed and timestamp should not be updated
        mapping._refresh_urls_if_needed("index2")

        # Check that the timestamp has not been updated
        assert mapping.latest_index_mappings_refresh_timestamp == initial_timestamp
        assert mock_get.called_once()

        # Check that the URLs mapping has been initially populated
        assert mapping._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_includes_only_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "NOT READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        # Access the urls_mapping property
        mapping._refresh_urls_if_needed("index1")
        urls_mapping = mapping._urls_mapping

        # Check that the URLs mapping has been initially populated
        assert urls_mapping["READY"] == {
            "index1": "example.com",
        }

    def test_refresh_urls_graceful_timeout_handling(self):
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        # use ridiculously low timeout
        with self.assertLogs('marqo', level='WARNING') as cm:
            mapping._refresh_urls(timeout=0.0000000001)
            assert "timeout" in cm.output[0].lower()
            assert "marqo cloud indexes" in cm.output[0].lower()

    @patch("requests.get")
    def test_refresh_urls_graceful_timeout_handling_http_timeout(self, mock_get):
        from requests.exceptions import Timeout
        mock_get.side_effect = Timeout
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertLogs('marqo', level='WARNING') as cm:
            mapping._refresh_urls(timeout=5)
            assert "timeout" in cm.output[0].lower()
            assert "marqo cloud indexes" in cm.output[0].lower()

    @patch("requests.get")
    def test_request_of_creating_index_raises_error(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "CREATING"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertRaises(MarqoCloudIndexNotReadyError):
            mapping.get_index_base_url("index2")

    @patch("requests.get")
    def test_modifying_state_returns_as_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "MODIFYING"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        assert mapping.get_index_base_url("index2") == "example2.com"

    @patch("requests.get")
    def test_request_of_not_existing_index_raises_error(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index2")

    def test_second_index_instantiation_does_not_refresh_urls_when_not_needed(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")
        test_index_name = self.create_test_index(self.generic_test_index_name)

        time_now = time.time()
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp -= 361
        time.sleep(0.1)
        idx = self.client.index(test_index_name)
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp > time_now

        time.sleep(0.1)
        time_now = time.time()
        idx = self.client.index(test_index_name)
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp < time_now

    def test_search_call_does_not_refresh_urls_when_not_needed(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")
        test_index_name = self.create_test_index(self.generic_test_index_name)

        time_now = time.time()
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp -= 361
        time.sleep(0.1)
        idx = self.client.index(test_index_name)
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp > time_now

        time_now = time.time()
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp -= 361
        time.sleep(0.1)
        idx.search("test")
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp > time_now

        time_now = time.time()
        time.sleep(0.1)
        idx.search("test")
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp < time_now



