import os
from unittest import mock

from tests.marqo_test import MarqoTestCase, CloudTestIndex


class TestCloudIntegrationTests(MarqoTestCase):
    def test_add_documents_mock_overwrites_mock_for_cloud_tests(self):
        """Test that if mock for add_documents is specified it overwrites the mock that
        create_test_index creates for tracking added documents."""
        with mock.patch("marqo.index.Index.add_documents") as mock_documents, \
                mock.patch("tests.marqo_test.MarqoTestCase.cleanup_documents_from_index") as mock_cleanup_documents:
            marqo_url_before_test = self.client_settings["url"]
            marqo_cloud_url_before_test = os.environ.get("MARQO_CLOUD_URL")
            self.client_settings["url"] = "cloud.url.marqo.ai"
            os.environ["MARQO_CLOUD_URL"] = "cloud.url.marqo.ai"
            mock_documents.return_value = {"success": True}

            test_index_name = self.create_test_index(
                cloud_test_index_to_use=CloudTestIndex.basic_index,
                open_source_test_index_name=self.generic_test_index_name,
            )
            res = self.client.index(test_index_name).add_documents(documents=[{"some": "data"}], tensor_fields=["some"])
            assert res == {"success": True}
            mock_documents.assert_called_once()

        if marqo_url_before_test:
            self.client_settings["url"] = marqo_url_before_test
        if marqo_cloud_url_before_test:
            os.environ["MARQO_CLOUD_URL"] = marqo_cloud_url_before_test