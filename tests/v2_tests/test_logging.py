import marqo
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark

marqo.set_log_level("INFO")


@mark.fixed
class TestLogging(MarqoTestCase):
    @staticmethod
    def _get_docs_to_index():
        return [
            # 5 OK docs:
            {"_id": "123", "Title": "Moon person"},
            {"_id": "124", "Title": "Shark island"},
            {"_id": "125", "Title": "3 people in a bar"},
            {"_id": "125", "Title": "4 people in a bar"},
            {"_id": "101", "Title": "blach"},

            {"_id": "102", "Title": "blach"},
            {"_id": "126", "Title": "https://marqo.ai/this_image_doesnt_exist.png"},  # bad
            {"_id": "103", "Title": "ooditty the second"},
            {"_id": "104", "Title": "wolol the second"},
            {"_id": "105", "Title": "strange the second"},

            {"_id": "127", "Title": "https://marqo.ai/this_image_doesnt_exist.png"},  # bad as well
            {"_id": "106", "Title": "blach"},
            {"_id": "108", "Title": "blach the third"},
            {"_id": "131", "Title": "blach the forf"},
            {"_id": "189", "Title": "https://marqo.ai/this_image_doesnt_too_exist.png"},  # bad as well

            {"_id": "109", "Title": "Sun person"},
            {"_id": "110", "Title": "Shark Shar"},
            {"_id": "111", "Title": "3 people in a car"},
            {"_id": "112", "Title": "4 people in a limo"},
            {"_id": "113", "Title": "Wow"},
        ]

    def _get_img_index(self):
        return self.get_test_index_name(
            cloud_test_index_to_use=CloudTestIndex.unstructured_image,
            open_source_test_index_name=self.unstructured_image_index_name
        )

    def test_add_document_warnings_no_batching(self):
        test_index_name = self._get_img_index()
        with self.assertLogs('marqo', level='INFO') as cm:
            self.client.index(index_name=test_index_name).add_documents(
                self._get_docs_to_index(), device="cpu", tensor_fields=["Title"]
            )
            assert len(cm.output) == 1
            assert "errors detected" in cm.output[0].lower()
            assert "info" in cm.output[0].lower()

    def test_add_document_warnings_client_batching(self):
        test_index_name = self._get_img_index()
        params_expected = [
            # so no client batching, that means no batch info output,  and therefore only 1 warning
            ({}, {"num_log_msgs": 1, "num_errors_msgs": 1}),

            # one error message, one regular info message per client batch
            ({"client_batch_size": 5}, {"num_log_msgs": 6, "num_errors_msgs": 2}),
        ]
        for params, expected in params_expected:

            with self.assertLogs('marqo', level='INFO') as cm:
                self.client.index(index_name=test_index_name).add_documents(
                    documents=self._get_docs_to_index(), device="cpu", **params, tensor_fields=["Title"]
                )
                assert len(cm.output) == expected['num_log_msgs']
                error_messages = [msg.lower() for msg in cm.output if "errors detected" in msg.lower()]
                assert len(error_messages) == expected['num_errors_msgs']
                assert all(["info" in msg for msg in error_messages])
                assert len(["info" in msg for msg in error_messages]) == expected['num_errors_msgs']

