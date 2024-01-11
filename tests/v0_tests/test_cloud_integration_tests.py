import os
from unittest import mock

from marqo1.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo1.index import Index


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
            res = self.client.index(test_index_name).add_documents(documents=[{"some": "data"}], tensor_fields=["some"], auto_refresh=True)
            assert res == {"success": True}
            mock_documents.assert_called_once()

        if marqo_url_before_test:
            self.client_settings["url"] = marqo_url_before_test
        if marqo_cloud_url_before_test:
            os.environ["MARQO_CLOUD_URL"] = marqo_cloud_url_before_test

    def test_index_cleanup_works_with_add_documents_mocked(self):
        """Test that index cleanup works even if add_documents is mocked.

        This test ensures that the cleanup process for an index functions correctly,
        even when the 'add_documents' method is mocked. It verifies that documents are properly added
        to the cleanup mapping and deleted from the index during cleanup.

        Additionally, it ensures that the MarqoTest 'add_documents' mocking functionality
        does not interfere with the mocking of 'add_documents' used during actual testing.
        """
        if not self.client.config.is_marqo_cloud:
            self.skipTest("This test is only for cloud tests")

        def wrap_for_add_docs(original):
            def wrapped(*args, **kwargs):
                return {**original(*args, **kwargs), "specific-test-key": 123}
            return wrapped

        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=None,
        )
        with mock.patch(
                "marqo.index.Index.add_documents",
                wraps=wrap_for_add_docs(Index(self.client.config, test_index_name).add_documents)
        ):
            # this check is for isolation case
            if self.index_to_documents_cleanup_mapping.get(test_index_name) is not None:
                assert "lost" not in self.index_to_documents_cleanup_mapping.get(test_index_name)

            add_documents_response = self.client.index(test_index_name).add_documents(
                documents=[{"some": "data", "_id": "lost"}], tensor_fields=["some"], auto_refresh=True
            )

            assert add_documents_response["specific-test-key"] == 123
            assert "lost" in self.index_to_documents_cleanup_mapping.get(test_index_name)
            assert self.client.index(test_index_name).get_stats()["numberOfDocuments"] == 1

            self.cleanup_documents_from_index(test_index_name)

            assert self.client.index(test_index_name).get_stats()["numberOfDocuments"] == 0
            with self.assertRaises(MarqoWebError):
                self.client.index(test_index_name).get_document("lost")
