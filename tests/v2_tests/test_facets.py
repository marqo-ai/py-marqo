import uuid

from pytest import mark

from marqo.client import Client
from marqo.errors import MarqoWebError
from tests.cloud_test_logic.cloud_test_index import CloudTestIndex
from tests.marqo_test import MarqoTestCase

EXAMPLE_FASHION_DOCUMENTS = [
  {
    "_id": "1",
    "title": "Slim Fit Denim Jacket",
    "brand": "SnugNest",
    "description": "A timeless piece with a modern slim-fit design, perfect for casual layering.",
    "color": "yellow",
    "size": "S",
    "style": "casual",
    "price": 83.42
  },
  {
    "_id": "2",
    "title": "Classic Cotton Shirt",
    "brand": "SnugNest",
    "description": "Comfortable and breathable cotton shirt suitable for everyday wear.",
    "color": "red",
    "size": "M",
    "style": "partywear",
    "price": 49.03
  },
  {
    "_id": "3",
    "title": "High-Waisted Skirt",
    "brand": "PulseWear",
    "description": "Elegant skirt with a high waistline and flattering silhouette.",
    "color": "coral",
    "size": "L",
    "style": "streetwear",
    "price": 1.2
  },
  {
    "_id": "4",
    "title": "Knitted Winter Sweater",
    "brand": "SprintX",
    "description": "Chunky knit sweater designed for warmth and comfort in cold seasons.",
    "color": "red",
    "size": "Free",
    "style": "loungewear",
    "price": 92.99
  },
  {
    "_id": "5",
    "title": "Casual Linen Trousers",
    "brand": "PulseWear",
    "description": "Relaxed-fit trousers crafted from lightweight linen for maximum comfort.",
    "color": "charcoal",
    "size": "M",
    "style": "partywear",
    "price": 88.14
  },
  {
    "_id": "6",
    "title": "Embroidered Kurta",
    "brand": "RetroHue",
    "description": "Traditional kurta with intricate embroidery for festive occasions.",
    "color": "green",
    "size": "S",
    "style": "streetwear",
    "price": 81.33
  },
  {
    "_id": "7",
    "title": "Floral Summer Dress",
    "brand": "SnugNest",
    "description": "Breezy and lightweight dress ideal for sunny summer days.",
    "color": "green",
    "size": "XS",
    "style": "streetwear",
    "price": 28.71
  },
  {
    "_id": "8",
    "title": "Athletic Running Shorts",
    "brand": "PulseWear",
    "description": "Performance shorts made from moisture-wicking fabric for workouts.",
    "color": "green",
    "size": "Free",
    "style": "biker",
    "price": 73.88
  },
  {
    "_id": "9",
    "title": "Hooded Windbreaker",
    "brand": "CozyCore",
    "description": "Windproof and waterproof jacket with adjustable hood.",
    "color": "charcoal",
    "size": "S",
    "style": "streetwear",
    "price": 55.54
  },
  {
    "_id": "10",
    "title": "Fleece Zip-Up Hoodie",
    "brand": "SnugNest",
    "description": "Super soft fleece hoodie for a relaxed and cozy look.",
    "color": "gray",
    "size": "M",
    "style": "loungewear",
    "price": 49.3
  }
]

@mark.fixed
class TestFacets(MarqoTestCase):
    def setUp(self):
        """ Overwrite setUp to add documents after deletion between tests"""
        super().setUp()
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(test_index_name).add_documents(
                EXAMPLE_FASHION_DOCUMENTS,
                tensor_fields=["title", "description"]
            )
            print("Added documents to index:", test_index_name)

    def test_facets_structured_index_fails(self):
        """Verify facets fail on structured indexes"""
        for cloud_test_index_to_use, open_source_test_index_name in [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                with self.assertRaises(MarqoWebError) as e:
                    self.client.index(test_index_name).search(
                        "shirt",
                        search_method="HYBRID",
                        facets={"fields": {"color": {"type": "string"}}}
                    )
                self.assertIn("Facets are only supported for unstructured indexes", str(e.exception))

    def test_facets_non_hybrid_search_fails(self):
        """Verify facets only work with HYBRID search method"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                for search_method in ["lexical", "tensor"]:
                    with self.assertRaises(MarqoWebError) as e:
                        self.client.index(test_index_name).search(
                            "shirt",
                            search_method=search_method,
                            facets={"fields": {"color": {"type": "string"}}}
                        )
                    self.assertIn("Facets can only be provided for 'HYBRID' search", str(e.exception))

    def test_single_string_facet(self):
        """Test getting a single string facet"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    facets={"fields": {"color": {"type": "string"}}}
                )
                self.assertIn("facets", res)
                self.assertIn("color", res["facets"])
                self.assertGreater(len(res["facets"]["color"]), 0)

    def test_multiple_facets(self):
        """Test getting multiple facets simultaneously"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    facets={
                        "fields": {
                            "color": {"type": "string"},
                            "brand": {"type": "string"},
                            "style": {"type": "string"}
                        }
                    }
                )
                self.assertIn("facets", res)
                self.assertEqual(len(res["facets"]), 3)

    def test_number_facet(self):
        """Test getting numeric facet with metrics"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    facets={"fields": {"price": {"type": "number"}}}
                )
                self.assertIn("facets", res)
                self.assertIn("price", res["facets"])
                for metric in ["min", "max", "avg", "count", "sum"]:
                    self.assertIn(metric, res["facets"]["price"])

    def test_number_facet_with_ranges(self):
        """Test getting numeric facet with custom ranges"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    facets={
                        "fields": {
                            "price": {
                                "type": "number",
                                "ranges": [
                                    {"from": 0, "to": 50, "name": "cheap"},
                                    {"from": 50, "name": "expensive"}
                                ]
                            }
                        }
                    }
                )
                self.assertIn("facets", res)
                self.assertIn("price", res["facets"])
                self.assertEqual(len(res["facets"]["price"]), 2)

    def test_overlapping_ranges_fails(self):
        """Test that overlapping ranges raise an error"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                with self.assertRaises(MarqoWebError) as e:
                    self.client.index(test_index_name).search(
                        "shirt",
                        search_method="HYBRID",
                        facets={
                            "fields": {
                                "price": {
                                    "type": "number",
                                    "ranges": [
                                        {"from": 0, "to": 60},
                                        {"from": 50, "to": 100}
                                    ]
                                }
                            }
                        }
                    )
                self.assertIn("Range configurations must not overlap", str(e.exception))

    def test_non_number_facet_with_ranges_fails(self):
        """Test that ranges are only allowed for number facets"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                for facet_type in ["string", "array"]:
                    with self.assertRaises(MarqoWebError) as e:
                        self.client.index(test_index_name).search(
                            "shirt",
                            search_method="HYBRID",
                            facets={
                                "fields": {
                                    "color": {
                                        "type": facet_type,
                                        "ranges": [
                                            {"from": 0, "to": 50}
                                        ]
                                    }
                                }
                            }
                        )
                    self.assertIn("Ranges can only be used for 'number' facets", str(e.exception))

    def test_facets_with_filter_exclusions(self):
        """Test facets with filter term exclusions"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    facets={
                        "fields": {
                            "color": {
                                "type": "string",
                                "excludeTerms": ["price:[100 TO 200]"]
                            }
                        }
                    },
                    filter_string="price:[100 TO 200]"
                )
                self.assertIn("facets", res)
                self.assertIn("color", res["facets"])

    def test_filter_exclusions_without_filter_fails(self):
        """Test that exclude terms require a filter string"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                with self.assertRaises(MarqoWebError) as e:
                    self.client.index(test_index_name).search(
                        "shirt",
                        search_method="HYBRID",
                        facets={
                            "fields": {
                                "color": {
                                    "type": "string",
                                    "excludeTerms": ["nonexistent:value"]
                                }
                            }
                        }
                    )
                self.assertIn("Exclude terms can only be used when a filter string is provided", str(e.exception))

    def test_invalid_filter_exclusions_fails(self):
        """Test that exclude terms must be present in filter string"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                with self.assertRaises(MarqoWebError) as e:
                    self.client.index(test_index_name).search(
                        "shirt",
                        search_method="HYBRID",
                        filter_string="existent:notvalue",
                        facets={
                            "fields": {
                                "color": {
                                    "type": "string",
                                    "excludeTerms": ["nonexistent:value"]
                                }
                            }
                        }
                    )
                self.assertIn("that do not appear in the filter string", str(e.exception))

    def test_invalid_facet_parameters_fail(self):
        """Test that invalid facet parameters raise errors"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                invalid_params = ["fields.to_", "fields.from_", "max_results", "max_depth", "fields.max_results"]
                for param in invalid_params:
                    with self.assertRaises(MarqoWebError) as e:
                        facets = {"fields": {"color": {"type": "string"}}}
                        if param.startswith("fields."):
                            facets["fields"]["color"][param[6:]] = 100
                        else:
                            facets[param] = 100
                        self.client.index(test_index_name).search(
                            "shirt",
                            search_method="HYBRID",
                            facets=facets
                        )
                    self.assertIn("extra fields not permitted", str(e.exception).lower())

    def test_track_total_hits_default(self):
        """Test track_total_hits parameter defaults to False"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID"
                )
                self.assertNotIn("totalHits", res)

    def test_track_total_hits_enabled(self):
        """Test track_total_hits parameter returns total hits count"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    track_total_hits=True
                )
                self.assertIn("totalHits", res)
                self.assertIsInstance(res["totalHits"], int)
                self.assertGreater(res["totalHits"], 0)

    def test_track_total_hits_with_filter(self):
        """Test track_total_hits with filter returns correct count"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    track_total_hits=True,
                    filter_string="price:[100 TO 200]"
                )
                self.assertIn("totalHits", res)
                filtered_res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    filter_string="price:[100 TO 200]"
                )
                self.assertEqual(res["totalHits"], len(filtered_res["hits"]))

    def test_track_total_hits_no_results(self):
        """Test track_total_hits when there are no matching documents"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "nonexistentquery123456",
                    search_method="HYBRID",
                    hybrid_parameters={
                        "retrievalMethod": "lexical",
                        "rankingMethod": "lexical"
                    },
                    track_total_hits=True
                )
                self.assertIn("totalHits", res)
                self.assertEqual(res["totalHits"], 0)
                self.assertEqual(len(res["hits"]), 0)

    def test_facets_filter_string_parsing_edge_cases(self):
        """Tests handling of filter strings with different parentheses placements in facet queries via API"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                test_cases = [
                    {
                        "name": "Filter string with parentheses around the whole string",
                        "filter": "(price:[49 TO 49.1] AND NOT color:red)",
                        "exclude_terms": ["color:red"]
                    },
                    {
                        "name": "Filter string with parentheses around term",
                        "filter": "price:[49 TO 49.1] AND (NOT color:red)",
                        "exclude_terms": ["color:red"]
                    },
                    {
                        "name": "Filter string with parentheses around value",
                        "filter": "price:[49 TO 49.1] AND NOT color:(red)",
                        "exclude_terms": ["color:(red)"]
                    }
                ]

                for test_case in test_cases:
                    with self.subTest(test_case["name"]):
                        res = self.client.index(test_index_name).search(
                            "shirt",
                            search_method="HYBRID",
                            filter_string=test_case["filter"],
                            facets={
                                "fields": {
                                    "color": {
                                        "type": "string",
                                        "excludeTerms": test_case["exclude_terms"]
                                    }
                                }
                            }
                        )
                        self.assertIn("facets", res)
                        self.assertIn("color", res["facets"])

    def test_non_existing_array_field_returns_empty_value(self):
        """Test that searching a non-existing array field raises an error"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            with self.subTest(test_index_name=test_index_name):
                res = self.client.index(test_index_name).search(
                    "shirt",
                    search_method="HYBRID",
                    facets={
                        "fields": {
                            "non_existing_field": {
                                "type": "array"
                            }
                        }
                    }
                )
                self.assertEqual(res["facets"]["non_existing_field"], {})
