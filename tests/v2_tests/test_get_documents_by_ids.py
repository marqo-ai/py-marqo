import requests
from pytest import mark

from tests.marqo_test import MarqoTestCase


@mark.fixed
class TestGetDocumentsByIds(MarqoTestCase):
    def get_documents_by_ids_via_post(self, index_name, document_ids, expose_facets=False):
        body = {
            "documentIds": document_ids,
        }

        url = self.authorized_url
        api_key = self.client_settings.get("api_key", None)

        if api_key:
            headers = {"x-api-key": self.client_settings["api_key"]}
        else:
            headers = {}

        url = f"{url}/indexes/{index_name}/documents/get-batch"

        if expose_facets:
            url += f"?expose_facets={expose_facets}"

        return requests.post(url, json=body, headers=headers)

    def test_add_documents_by_ids(self):
        """Ensure get_documents_by_ids works for both GET and POST requests."""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with (self.subTest(test_index_name)):
                tensor_fields = ["text_field_1", "text_field_2", "text_field_3"] if "unstr" \
                                                                                    in test_index_name else None
                d1 = {
                    "text_field_1": "Cool Document 1",
                    "text_field_2": "some extra info",
                    "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
                }
                d2 = {
                    "text_field_1": "Just Your Average Doc",
                    "text_field_2": "this is a solid doc",
                    "_id": "123456"
                }

                res = self.client.index(test_index_name).add_documents(
                    [
                        d1, d2
                    ], tensor_fields=tensor_fields
                )

                get_response = self.client.index(test_index_name).get_documents(
                    document_ids=["e197e580-0393-4f4e-90e9-8cdf4b17e339", "123456", "not_exist"], expose_facets=True
                )

                post_response = self.get_documents_by_ids_via_post(
                    index_name=test_index_name,
                    document_ids=["e197e580-0393-4f4e-90e9-8cdf4b17e339", "123456", "not_exist"], expose_facets=True
                ).json()

                self.assertEqual(
                    get_response,
                    post_response
                )