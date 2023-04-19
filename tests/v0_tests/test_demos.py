import pprint

from marqo.client import Client
from marqo.errors import MarqoApiError

from tests.marqo_test import MarqoTestCase


class TestDemo(MarqoTestCase):
    """Tests for demos.
    """
    def setUp(self) -> None:
        client_0 = Client(**self.client_settings)
        for ix_name in ["cool-index-1", "my-first-index", "my-weighted-query-index", "my-first-multimodal-index"]:
            try:
                client_0.delete_index(ix_name)
            except MarqoApiError:
                pass

    def test_demo(self):
        client = Client(**self.client_settings)
        client.index("cool-index-1").add_documents([
            {
                "Title": "The Legend of the River",
                "Description": "Once upon a time there was a cat who wore a hat. "
                               "What happens next is ridiculous. "
             },
            {
                "Title": "Top Places to Visit in Melbourne ",
                "Key Points": """ - Collingwood 
                - Toorak 
                - Brunswick
                - Cremorne 
                
                Some of these places are great living areas, and some are great places to eat.
                S2Search is based in Melbourne. Melbourne has beautiful waterways running through it.
                """
            },
        ])
        print("\nSearching the phrase 'River' across all fields")
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index("cool-index-1").search,"River")

        pprint.pprint(client.index("cool-index-1").search("River"))
        # then we search specific searchable attributes. We can see how powerful semantic search is
        print("\nThen we search specific 'River over' searchable attributes. We can see how powerful semantic search is")

        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index("cool-index-1").search,"River", searchable_attributes=["Key Points"])

        pprint.pprint(client.index("cool-index-1").search("River", searchable_attributes=["Key Points"]))

    def test_readme_example(self):

        import marqo

        mq = marqo.Client(**self.client_settings)

        mq.index("my-first-index").add_documents([
            {
                "Title": "The Travels of Marco Polo",
                "Description": "A 13th-century travelogue describing Polo's travels"},
            {
                "Title": "Extravehicular Mobility Unit (EMU)",
                "Description": "The EMU is a spacesuit that provides environmental protection, "
                               "mobility, life support, and communications for astronauts",
                "_id": "article_591"
            }
        ])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-first-index").search,
                q="What is the best outfit to wear on the moon?"
            )

        results = mq.index("my-first-index").search(
            q="What is the best outfit to wear on the moon?"
        )

        pprint.pprint(results)

        assert results["hits"][0]["_id"] == "article_591"

        r2 = mq.index("my-first-index").get_document(document_id="article_591")
        assert {
                "Title": "Extravehicular Mobility Unit (EMU)",
                "Description": "The EMU is a spacesuit that provides environmental protection, "
                               "mobility, life support, and communications for astronauts",
                "_id": "article_591"
            } == r2

        r3 = mq.index("my-first-index").get_stats()

        assert r3["numberOfDocuments"] == 2

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-first-index").search,'marco polo', search_method=marqo.SearchMethods.LEXICAL)

        r4 = mq.index("my-first-index").search('marco polo', search_method=marqo.SearchMethods.LEXICAL)
        assert r4["hits"][0]["Title"] == "The Travels of Marco Polo"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-first-index").search,'adventure', searchable_attributes=['Title'])

        r5 = mq.index("my-first-index").search('adventure', searchable_attributes=['Title'])
        assert len(r5["hits"]) == 2

        r6 = mq.index("my-first-index").delete_documents(ids=["article_591", "article_602"])
        assert r6['details']['deletedDocuments'] == 1

        rneg1 = mq.index("my-first-index").delete()
        pprint.pprint(rneg1)
        assert (rneg1["acknowledged"] is True) or (rneg1["acknowledged"] == 'true')

    def test_readme_example_weighted_query(self):
        import marqo
        mq = marqo.Client(**self.client_settings)
        mq.index("my-weighted-query-index").add_documents([
            {
                "Title": "Smartphone",
                "Description": "A smartphone is a portable computer device that combines mobile telephone "
                "functions and computing functions into one unit.",
            },
            {
                "Title": "Telephone",
                "Description": "A telephone is a telecommunications device that permits two or more users to"
                "conduct a conversation when they are too far apart to be easily heard directly.",
            },
            {
                "Title": "Thylacine",
                "Description": "The thylacine, also commonly known as the Tasmanian tiger or Tasmanian wolf, "
                "is an extinct carnivorous marsupial."
                "The last known of its species died in 1936.",
            },
        ])

        r1 = mq.index("my-weighted-query-index").get_stats()
        assert r1["numberOfDocuments"] == 3

        query = {
            "I need to buy a communications device, what should I get?": 1.1,
            "Technology that became prevelant in the 21st century": 1.0,
        }

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-weighted-query-index").search,
                q=query, searchable_attributes=["Title", "Description"]
            )

        r2 = mq.index("my-weighted-query-index").search(
            q=query, searchable_attributes=["Title", "Description"]
        )

        assert r2["hits"][0]["Title"] == "Smartphone"

        print("Query 1:")
        pprint.pprint(r2)
        query = {
            "I need to buy a communications device, what should I get?": 1.0,
            "Technology that became prevelant in the 21st century": -1.0,
        }

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-weighted-query-index").search,
                q=query, searchable_attributes=["Title", "Description"]
            )

        r3 = mq.index("my-weighted-query-index").search(
            q=query, searchable_attributes=["Title", "Description"]
        )
        print("\nQuery 2:")
        pprint.pprint(r3)

        assert r3["hits"][0]["Title"] == "Telephone"


        assert r2["hits"][-1]["Title"] == "Thylacine"
        assert r3["hits"][-1]["Title"] == "Thylacine"

        assert len(r2["hits"]) == 3
        assert len(r3["hits"]) == 3

        rneg1 = mq.index("my-weighted-query-index").delete()
        pprint.pprint(rneg1)
        assert (rneg1["acknowledged"] is True) or (rneg1["acknowledged"] == 'true')

    def test_readme_example_multimodal_combination_query(self):
        import marqo
        mq = marqo.Client(**self.client_settings)
        settings = {"treat_urls_and_pointers_as_images": True, "model": "ViT-L/14"}
        mq.create_index("my-first-multimodal-index", **settings)
        mq.index("my-first-multimodal-index").add_documents(
            [
                {
                    "Title": "Flying Plane",
                    "captioned_image": {
                        "caption": "An image of a passenger plane flying in front of the moon.",
                        "image": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image2.jpg",
                    },
                },
                {
                    "Title": "Red Bus",
                    "captioned_image": {
                        "caption": "A red double decker London bus traveling to Aldwych",
                        "image": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image4.jpg",
                    },
                },
                {
                    "Title": "Horse Jumping",
                    "captioned_image": {
                        "caption": "A person riding a horse over a jump in a competition.",
                        "image": "https://raw.githubusercontent.com/marqo-ai/marqo/mainline/examples/ImageSearchGuide/data/image1.jpg",
                    },
                },
            ],
            mappings={
                "captioned_image": {
                    "type": "multimodal_combination",
                    "weights": {
                        "caption": 0.3,
                        "image": 0.7,
                    },
                }
            },
        )

        r1 = mq.index("my-first-multimodal-index").get_stats()
        assert r1["numberOfDocuments"] == 3

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-first-multimodal-index").search,
                q="Give me some images of vehicles and modes of transport. I am especially interested in air travel and commercial aeroplanes.",
                searchable_attributes=["captioned_image"]
            )

        r2 = mq.index("my-first-multimodal-index").search(
            q="Give me some images of vehicles and modes of transport. I am especially interested in air travel and commercial aeroplanes.",
            searchable_attributes=["captioned_image"],
        )
        print("Query 1:")
        pprint.pprint(r2)

        assert r2["hits"][0]["Title"] == "Flying Plane"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-first-multimodal-index").search,
                q={
                    "What are some vehicles and modes of transport?": 1.0,
                    "Aeroplanes and other things that fly": -1.0,
                },
                searchable_attributes=["captioned_image"]
            )
        r3 = mq.index("my-first-multimodal-index").search(
            q={
                "What are some vehicles and modes of transport?": 1.0,
                "Aeroplanes and other things that fly": -1.0,
            },
            searchable_attributes=["captioned_image"],
        )

        print("\nQuery 2:")
        pprint.pprint(r3)

        assert r3["hits"][0]["Title"] == "Red Bus"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index("my-first-multimodal-index").search,
                q={"Animals of the Perissodactyla order": -1.0},
                searchable_attributes=["captioned_image"],
            )
        r4 = mq.index("my-first-multimodal-index").search(
            q={"Animals of the Perissodactyla order": -1.0},
            searchable_attributes=["captioned_image"],
        )
        print("\nQuery 3:")
        pprint.pprint(r4)

        assert r4["hits"][-1]["Title"] == "Horse Jumping"

        assert len(r2["hits"]) == 3
        assert len(r3["hits"]) == 3
        assert len(r4["hits"]) == 3

        rneg1 = mq.index("my-first-multimodal-index").delete()
        pprint.pprint(rneg1)
        assert (rneg1["acknowledged"] is True) or (rneg1["acknowledged"] == 'true')