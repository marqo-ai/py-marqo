import uuid

from pytest import mark

from marqo.client import Client
from marqo.errors import MarqoWebError
from tests.cloud_test_logic.cloud_test_index import CloudTestIndex
from tests.marqo_test import MarqoTestCase

COLOR_OPTIONS = ["yellow", "red", "coral", "charcoal"]

PRODUCTS = [
  {
    "parentProductId": "1",
    "title": "Slim Fit Denim Jacket",
    "brand": "SnugNest",
    "description": "A timeless piece with a modern slim-fit design, perfect for casual layering.",
    "size": "S",
    "price": 10.0
  },
  {
    "parentProductId": "2",
    "title": "Classic Cotton Shirt",
    "brand": "SnugNest",
    "description": "Comfortable and breathable cotton shirt suitable for everyday wear. modern",
    "size": "M",
    "price": 20.0
  },
  {
    "parentProductId": "3",
    "title": "High-Waisted Skirt",
    "brand": "PulseWear",
    "description": "Elegant skirt with a high waistline and flattering silhouette. modern",
    "size": "L",
    "price": 30.0
  },
  {
    "parentProductId": "4",
    "title": "Knitted Winter Sweater",
    "brand": "SprintX",
    "description": "Chunky knit sweater designed for warmth and comfort in cold seasons. modern",
    "size": "Free",
    "price": 40.0
  },
  {
    "parentProductId": "5",
    "title": "Casual Linen Trousers",
    "brand": "PulseWear",
    "description": "Relaxed-fit trousers crafted from lightweight linen for maximum comfort. modern",
    "size": "M",
    "price": 50.0
  },
  {
    "parentProductId": "6",
    "title": "Embroidered Kurta",
    "brand": "RetroHue",
    "description": "Traditional kurta with intricate embroidery for festive occasions. modern",
    "size": "S",
    "price": 60.0
  },
]

ALL_VARIANTS = [{**product, **{"color": color, "_id": f"{product['parentProductId']}_{color}"}}
                for product in PRODUCTS for color in COLOR_OPTIONS]

@mark.fixed
class TestCollapseFields(MarqoTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_cases = [
            (CloudTestIndex.unstructured_collapse_fields, cls.unstructured_collapse_fields_index_name),
        ]

    def test_collapse_fields_is_in_index_settings(self):
        """Test that collapse field in the index creation request is persisted"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            with self.subTest(index_name=index_name):
                index_settings = self.client.index(index_name).get_settings()
                self.assertTrue("collapseFields" in index_settings)
                self.assertEqual(len(index_settings["collapseFields"]), 1)
                self.assertEqual(index_settings["collapseFields"][0], {"name": "parentProductId", "minGroups": 20})

    def test_search_with_valid_collapse_field_succeeds(self):
        """Test that search with valid collapse field name succeeds"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            with self.subTest(index_name=index_name):
                self.client.index(index_name).add_documents(
                    ALL_VARIANTS, tensor_fields=["combo"],
                    mappings={
                        "combo": {
                            "type": "multimodal_combination",
                            "weights": {"title": 0.5, "description": 0.5}
                        }
                    }
                )

                test_cases = [
                    ("disjunction", "rrf"),
                    ("tensor", "tensor"),
                    ("tensor", "lexical"),
                    ("lexical", "lexical"),
                    ("lexical", "tensor"),
                ]

                for retrieval_method, ranking_method in test_cases:
                    with self.subTest(retrieval_method=retrieval_method, ranking_method=ranking_method):
                        res = self.client.index(index_name).search(
                            q="modern clothes",
                            search_method="HYBRID",
                            hybrid_parameters={
                                "retrievalMethod": retrieval_method,
                                "rankingMethod": ranking_method,
                                "rerankDepthTensor": 30,
                            },
                            collapse_fields=[{"name": "parentProductId"}],
                            filter_string="price:[15.0 TO *] AND (color:yellow OR color:red)",
                            facets={"fields": {
                                "price": {"type": "number", "ranges": [
                                  {"from": 15.0, "to": 40.0},
                                  {"from": 40.0}
                                ]},
                                "color": {"type": "string"},
                                "brand": {"type": "string"},
                            }},
                            limit=6
                        )

                        # Verify the search executed successfully and only contain 1 variant from each parent product
                        # the first product with price 10.0 is excluded
                        self.assertEqual(5, len(res["hits"]))
                        # Verify each product only has one variant returned
                        self.assertEqual(set([str(p) for p in range(2, 7)]), set([hit['parentProductId'] for hit in res["hits"]]))
                        # Verify returned variants matches the color filter
                        for hit in res["hits"]:
                            self.assertIn(hit["color"], ["yellow", "red"])

                        # Verify the facets returns correct count
                        if retrieval_method != "lexical":
                            self.assertIn("facets", res)
                            self.assertDictEqual({'15.0:40.0': {'count': 2}, '40.0:Inf': {'count': 3}}, res["facets"]["price"])
                            self.assertDictEqual({'red': {'count': 5}, 'yellow': {'count': 5}}, res["facets"]["color"])
                            self.assertDictEqual({'PulseWear': {'count': 2}, 'RetroHue': {'count': 1}, 'SnugNest': {'count': 1},
                                                  'SprintX': {'count': 1}}, res["facets"]["brand"])
                        else:
                            # TODO there's but with lexical retrieval_method in Marqo that results in wrong count of
                            #   facets, will remove this branch when that bug is fixed.
                            pass
