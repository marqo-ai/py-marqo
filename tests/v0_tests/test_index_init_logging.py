from tests.marqo_test import MarqoTestCase
from unittest.mock import patch

class TestInitLogging(MarqoTestCase):

    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_logging_non_existent_index(self, mock_warning):
        """Test no logging on index instantiation when index does not exist"""

        self.client.index("this-index-will-never-exist")
        assert mock_warning.call_count == 0

    @patch("requests.get")
    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_creating(self, mock_warning, mock_get):
        """Test no logging on index instantiation when index is creating"""
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "test_index_name", "endpoint": "example2.com", "index_status": "CREATING"}
        ]}
        test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        ix = self.client.index(test_index_name)
        assert ix.config.instance_mapping.get_index_base_url(test_index_name) == 'example2.com'
        assert mock_warning.call_count == 0

    @patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning")
    def test_index_init_exists(self, mock_warning,):
        """Test no logging on index instantiation when index exists"""

        test_index_name = self.create_test_index(index_name=self.generic_test_index_name)
        ix = self.client.index(test_index_name)
        assert mock_warning.call_count == 0

        # check that index truly exists:
        ix.get_stats()



