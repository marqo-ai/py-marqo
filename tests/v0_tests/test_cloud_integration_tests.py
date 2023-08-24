import os
from unittest import mock

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo.index import Index


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

    def test_index_cleanup_works_with_add_documents_mocked(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("This test is only for cloud tests")
        add_documents_patch = mock.patch.object(Index, "add_documents")
        add_documents_patch.start()
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=None,
        )
        add_documents_patch.stop()
        a = self.client.index(test_index_name).add_documents(
            documents=[{"some": "data", "_id": "lost"}], tensor_fields=["some"]
        )
        add_documents_patch.start()
        assert self.client.index(test_index_name).get_stats()["numberOfDocuments"] == 1
        assert not self.index_to_documents_cleanup_mapping.get(test_index_name)  # None or empty set
        self.cleanup_documents_from_index(test_index_name)
        assert self.client.index(test_index_name).get_stats()["numberOfDocuments"] == 0
        with self.assertRaises(MarqoWebError):
            self.client.index(test_index_name).get_document("lost")
        add_documents_patch.stop()

