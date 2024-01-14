from tests.marqo_test import MarqoTestCase, CloudTestIndex
from unittest.mock import patch
from marqo.index import marqo_url_and_version_cache
from marqo import Client
from pytest import mark


@mark.fixed
class TestInitLogging(MarqoTestCase):

    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_logging_non_existent_index(self, mock_warning):
        """Test no logging on index instantiation when index does not exist"""

        self.client.index("this-index-will-never-exist")
        assert mock_warning.call_count == 0

    @patch("requests.get")
    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_creating(self, mock_warning, mock_get):
        """Test no logging on index instantiation when a cloud index is in a CREATING state"""
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")

        test_index_name = self.generic_test_index_name
        mock_get.return_value.json.return_value = {"results": [
            {"indexName": test_index_name, "marqoEndpoint": "example2.com", "indexStatus": "CREATING"}
        ]}

        ix = self.client.index(test_index_name)

        marqo_url_and_version_cache.clear()
        ix.config.instance_mapping.latest_index_mappings_refresh_timestamp = 0
        ix.config.instance_mapping.index_http_error_handler(test_index_name)

        assert ix.config.instance_mapping.get_index_base_url(test_index_name) == 'example2.com'
        assert mock_warning.call_count == 0

    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_exists(self, mock_warning,):
        """Test no logging on index instantiation when index exists"""
        self.client.config.instance_mapping._refresh_urls()
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            ix = self.client.index(test_index_name)
            assert mock_warning.call_count == 0

            # check that index truly exists:
            ix.get_stats()

    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_exists_version_mismatch(self, mock_warning):
        """This should trigger a warning """
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            with patch("marqo._httprequests.HttpRequests.get") as mock_get:
                mock_get.return_value = {"message": "Welcome to Marqo", "version": "0.0.21"}
                assert mock_warning.call_count == 0
                #  creating a client shouldn't trigger a warning
                temp_client = Client(**self.client_settings)

                assert mock_warning.call_count == 0

                marqo_url_and_version_cache.clear()

                # instantiating an index should trigger a warning, as the version is
                # not supported
                ix = temp_client.index(test_index_name)
                mock_get.assert_called_once()
                assert mock_warning.call_count == 1
                # A get request to get Marqo's version:
                assert mock_get.call_count == 1

                index_url = ix.config.instance_mapping.get_index_base_url(test_index_name)
                assert marqo_url_and_version_cache[index_url] == "0.0.21"

                temp_client_2 = Client(**self.client_settings)
                ix_2 = temp_client.index(test_index_name)

                # We should not get a warning, as we already have the version cached:
                assert mock_warning.call_count == 1
                # but we don't make another request to get the version:
                assert mock_get.call_count == 1

