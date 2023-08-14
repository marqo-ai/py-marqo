from marqo.client import Client
import pprint
from tests.marqo_test import MarqoTestCase


class TestDemo(MarqoTestCase):
    """Tests for demos.
    """

    def test_demo(self):
        client = Client(**self.client_settings)
        test_index_name = self.create_test_index(self.generic_test_index_name)
        client.index(test_index_name).add_documents([
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
        ], tensor_fields=["Title", "Description", "Key Points"])
        print("\nSearching the phrase 'River' across all fields")
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,"River")

        pprint.pprint(client.index(test_index_name).search("River"))
        # then we search specific searchable attributes. We can see how powerful semantic search is
        print("\nThen we search specific 'River over' searchable attributes. We can see how powerful semantic search is")

        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,"River", searchable_attributes=["Key Points"])

        pprint.pprint(client.index(test_index_name).search("River", searchable_attributes=["Key Points"]))

        if not self.client.config.is_marqo_cloud:
            self.client.delete_index(test_index_name, wait_for_readiness=False)

    def test_readme_example(self):

        import marqo

        mq = marqo.Client(**self.client_settings)

        test_index_name = self.create_test_index(self.generic_test_index_name)
        mq.index(test_index_name).add_documents(
            [
                {
                    "Title": "The Travels of Marco Polo",
                    "Description": "A 13th-century travelogue describing Polo's travels"},
                {
                    "Title": "Extravehicular Mobility Unit (EMU)",
                    "Description": "The EMU is a spacesuit that provides environmental protection, "
                                   "mobility, life support, and communications for astronauts",
                    "_id": "article_591"
                }
            ],
            tensor_fields=["Title", "Description"]
        )

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index(test_index_name).search,
                q="What is the best outfit to wear on the moon?"
            )

        results = mq.index(test_index_name).search(
            q="What is the best outfit to wear on the moon?"
        )

        pprint.pprint(results)

        assert results["hits"][0]["_id"] == "article_591"

        r2 = mq.index(test_index_name).get_document(document_id="article_591")
        assert {
                "Title": "Extravehicular Mobility Unit (EMU)",
                "Description": "The EMU is a spacesuit that provides environmental protection, "
                               "mobility, life support, and communications for astronauts",
                "_id": "article_591"
            } == r2

        r3 = mq.index(test_index_name).get_stats()

        assert r3["numberOfDocuments"] == 2

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index(test_index_name).search,'marco polo', search_method=marqo.SearchMethods.LEXICAL)

        r4 = mq.index(test_index_name).search('marco polo', search_method=marqo.SearchMethods.LEXICAL)
        assert r4["hits"][0]["Title"] == "The Travels of Marco Polo"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index(test_index_name).search,'adventure', searchable_attributes=['Title'])

        r5 = mq.index(test_index_name).search('adventure', searchable_attributes=['Title'])
        assert len(r5["hits"]) == 2

        r6 = mq.index(test_index_name).delete_documents(ids=["article_591", "article_602"])
        assert r6['details']['deletedDocuments'] == 1

        if not self.client.config.is_marqo_cloud:
            rneg1 = mq.index(test_index_name).delete(wait_for_readiness=False)
            pprint.pprint(rneg1)
            assert (rneg1["acknowledged"] is True) or (rneg1["acknowledged"] == 'true')

    def test_readme_example_weighted_query(self):
        import marqo
        mq = marqo.Client(**self.client_settings)
        test_index_name = self.create_test_index(self.generic_test_index_name)
        mq.index(test_index_name).add_documents([
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
            ],
            tensor_fields=["Title", "Description"]
        )

        r1 = mq.index(test_index_name).get_stats()
        assert r1["numberOfDocuments"] == 3

        query = {
            "I need to buy a communications device, what should I get?": 1.1,
            "Technology that became prevelant in the 21st century": 1.0,
        }

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index(test_index_name).search,
                q=query, searchable_attributes=["Title", "Description"]
            )

        r2 = mq.index(test_index_name).search(
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
            self.warm_request(mq.index(test_index_name).search,
                q=query, searchable_attributes=["Title", "Description"]
            )

        r3 = mq.index(test_index_name).search(
            q=query, searchable_attributes=["Title", "Description"]
        )
        print("\nQuery 2:")
        pprint.pprint(r3)

        assert r3["hits"][0]["Title"] == "Telephone"


        assert r2["hits"][-1]["Title"] == "Thylacine"
        assert r3["hits"][-1]["Title"] == "Thylacine"

        assert len(r2["hits"]) == 3
        assert len(r3["hits"]) == 3

        if not self.client.config.is_marqo_cloud:
            rneg1 = mq.index(test_index_name).delete(wait_for_readiness=False)
            pprint.pprint(rneg1)
            assert (rneg1["acknowledged"] is True) or (rneg1["acknowledged"] == 'true')

    def test_readme_example_multimodal_combination_query(self):
        import marqo
        mq = marqo.Client(**self.client_settings)
        settings = {"treat_urls_and_pointers_as_images": True, "model": "ViT-B/32"}
        test_index_name = self.create_test_index(self.generic_test_index_name, **settings)
        mq.index(test_index_name).add_documents(
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
            tensor_fields=["captioned_image"],
        )

        r1 = mq.index(test_index_name).get_stats()
        assert r1["numberOfDocuments"] == 3

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index(test_index_name).search,
                q="Give me some images of vehicles and modes of transport. I am especially interested in air travel and commercial aeroplanes.",
                searchable_attributes=["captioned_image"]
            )

        r2 = mq.index(test_index_name).search(
            q="Give me some images of vehicles and modes of transport. I am especially interested in air travel and commercial aeroplanes.",
            searchable_attributes=["captioned_image"],
        )
        print("Query 1:")
        pprint.pprint(r2)

        assert r2["hits"][0]["Title"] == "Flying Plane"

        if self.IS_MULTI_INSTANCE:
            self.warm_request(mq.index(test_index_name).search,
                q={
                    "What are some vehicles and modes of transport?": 1.0,
                    "Aeroplanes and other things that fly": -1.0,
                },
                searchable_attributes=["captioned_image"]
            )
        r3 = mq.index(test_index_name).search(
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
            self.warm_request(mq.index(test_index_name).search,
                q={"Animals of the Perissodactyla order": -1.0},
                searchable_attributes=["captioned_image"],
            )
        r4 = mq.index(test_index_name).search(
            q={"Animals of the Perissodactyla order": -1.0},
            searchable_attributes=["captioned_image"],
        )
        print("\nQuery 3:")
        pprint.pprint(r4)

        assert r4["hits"][-1]["Title"] == "Horse Jumping"

        assert len(r2["hits"]) == 3
        assert len(r3["hits"]) == 3
        assert len(r4["hits"]) == 3

        if not self.client.config.is_marqo_cloud:
            rneg1 = mq.index(test_index_name).delete(wait_for_readiness=False)
            pprint.pprint(rneg1)
            assert (rneg1["acknowledged"] is True) or (rneg1["acknowledged"] == 'true')
