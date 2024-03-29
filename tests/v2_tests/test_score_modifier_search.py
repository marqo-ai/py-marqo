from typing import Any, Dict, List, Optional

from marqo.errors import MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


@mark.fixed
class TestScoreModifierSearch(MarqoTestCase):

    def setUp(self) -> None:
        super().setUp()
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            self.test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=self.test_index_name).add_documents(
                documents=[
                    {"my_text_field": "A rider is riding a horse jumping over the barrier.",
                     "my_image_field": "https://marqo-assets.s3.amazonaws.com/tests/images/image2.jpg",
                     # 4 fields
                     "multiply_1": 1,
                     "multiply_2": 20.0,
                     "add_1": 1.0,
                     "add_2": 30.0,
                     "_id": "1"
                     },
                    {"my_text_field": "A rider is riding a horse jumping over the barrier.",
                     "my_image_field": "https://marqo-assets.s3.amazonaws.com/tests/images/image2.jpg",
                     "_id": "0",
                     "filter": "original"
                     },
                ],
                tensor_fields=["my_image_field", "my_text_field"]
            )
            self.query = "what is the rider doing?"
    
    def search_with_score_modifier(self, score_modifiers: Optional[Dict[str, List[Dict[str, Any]]]] = None, **kwargs) -> Dict[str, Any]:
        return self.client.index(self.test_index_name).search(
            q = self.query,
            score_modifiers = score_modifiers,
            **kwargs
        )

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

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_score_modifier(score_modifiers=None, filter_string="filter:original"))
        
        original_res = self.search_with_score_modifier(score_modifiers=None, filter_string="filter:original")
        original_score = original_res["hits"][0]["_score"]

        if self.IS_MULTI_INSTANCE:
            self.warm_request(lambda: self.search_with_score_modifier(score_modifiers=score_modifiers))
        modifiers_res = self.search_with_score_modifier(score_modifiers=score_modifiers)

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
            self.search_with_score_modifier(score_modifiers=invalid_score_modifiers)
            raise AssertionError
        except MarqoWebError:
            pass

    def test_valid_score_modifiers_format(self):
        valid_score_modifiers = {
                # missing one part
                "add_to_score": [
                    {"field_name": "add_1", "weight" : -3,
                     },
                    {"field_name": "add_2", "weight": 1,
                     }]
            }
        self.search_with_score_modifier(score_modifiers=valid_score_modifiers)