from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase


class TestScoreModifierSearch(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass
        self.client.create_index(index_name=self.index_name_1, model="ViT-B/32")
        self.client.index(index_name=self.index_name_1).add_documents(
            documents=[
                {"my_text_field": "A rider is riding a horse jumping over the barrier.",
                 "my_image_field": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg",
                 # 4 fields
                 "multiply_1": 1,
                 "multiply_2": 20.0,
                 "add_1": 1.0,
                 "add_2": 30.0,
                 "_id": "1"
                 },
                {"my_text_field": "A rider is riding a horse jumping over the barrier.",
                 "my_image_field": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg",
                 "_id": "0",
                 "filter": "original"
                 },
            ], non_tensor_fields=["multiply_1", "multiply_2", "add_1", "add_2",
                                                       "filter"]
        )

        self.query = "what is the rider doing?"

    def tearDown(self) -> None:
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def test_score_modifier_search_results(self):
        score_modifiers = {
                # miss one weight
                "multiply_score_by":
                    [{"field_name": "multiply_1",
                      "weight": 1,},
                     {"field_name": "multiply_2",}],
                "add_to_score": [
                    {"field_name": "add_1", "weight" : -3,
                     },
                    {"field_name": "add_2", "weight": 1,
                     }]
            }

        original_res = self.client.index(self.index_name_1).search(q = self.query, score_modifiers=None,
                                                                     filter_string="filter:original")
        original_score = original_res["hits"][0]["_score"]

        modifiers_res = self.client.index(self.index_name_1).search(q=self.query, score_modifiers=score_modifiers)

        modifiers_score = modifiers_res["hits"][0]["_score"]
        expected_sore = original_score * 20 * 1 + 1 * -3 + 30 * 1
        assert abs(expected_sore -modifiers_score) < 1e-5

    def test_invalid_score_modifiers_format(self):
        invalid_score_modifiers = {
                # typo in multiply score by
                "multiply_score_bys":
                    [{"field_name": "multiply_1",
                      "weight": 1,},
                     {"field_name": "multiply_2",}],
                "add_to_score": [
                    {"field_name": "add_1", "weight" : 4,
                     },
                    {"field_name": "add_2", "weight": 1,
                     }]
            }

        try:
            modifiers_res = self.client.index(self.index_name_1).search(q=self.query,
                                                                        score_modifiers = invalid_score_modifiers)
            raise AssertionError
        except MarqoWebError:
            pass

    def test_valid_score_modifiers_format(self):
        valid_score_modifiers_list = [
            {
                # missing add to score
                "add_to_score": [
                    {"field_name": "add_1", "weight" : -3,
                     },
                    {"field_name": "add_2", "weight": 1,
                     }]
            },
            {
                # missing multiply score by
                "multiply_score_by":
                    [{"field_name": "multiply_1",
                      "weight": 1,},
                     {"field_name": "multiply_2"}],
            }]

        for valid_score_modifiers in valid_score_modifiers_list:
            modifiers_res = self.client.index(self.index_name_1).search(q=self.query, score_modifiers=valid_score_modifiers)

    def test_bulk_search_error(self):
        try:
            resp = self.client.bulk_search([{
                "index": self.index_name_1,
                "q": "title about some doc",
                "scoreModifiers" : {
                    # typo in multiply score by
                    "multiply_score_bys":
                        [{"field_name": "multiply_1",
                          "weight": 1,},
                         {"field_name": "multiply_2",}],
                    "add_to_score": [
                        {"field_name": "add_1", "weight" : 4,
                         },
                        {"field_name": "add_2", "weight": 1,
                         }]
                }
            }])
            raise AssertionError
        except MarqoWebError:
            pass