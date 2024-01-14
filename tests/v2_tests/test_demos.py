from marqo.client import Client
import pprint
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


class TestDemo(MarqoTestCase):
    """Tests for demos.
    """
    @mark.fixed
    def test_demo(self):
        client = Client(**self.client_settings)
        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            client.index(test_index_name).add_documents([
                {
                    "text_field_1": "The Legend of the River",
                    "text_field_2": "Once upon a time there was a cat who wore a hat. "
                                   "What happens next is ridiculous. "
                 },
                {
                    "text_field_1": "Top Places to Visit in Melbourne ",
                    "text_field_3": """ - Collingwood 
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
                self.warm_request(client.index(test_index_name).search,"River")

            pprint.pprint(client.index(test_index_name).search("River"))
            # then we search specific searchable attributes. We can see how powerful semantic search is
            print("\nThen we search specific 'River over' searchable attributes. We can see how powerful semantic search is")

            if self.IS_MULTI_INSTANCE:
                self.warm_request(client.index(test_index_name).search,"River", searchable_attributes=["text_field_3"])

            pprint.pprint(client.index(test_index_name).search("River", searchable_attributes=["text_field_3"]))

    def test_readme_example(self):

        import marqo

        mq = marqo.Client(**self.client_settings)

        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]

        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            mq.index(test_index_name).add_documents(
                [
                    {
                        "text_field_1": "The Travels of Marco Polo",
                        "text_field_2": "A 13th-century travelogue describing Polo's travels"},
                    {
                        "text_field_1": "Extravehicular Mobility Unit (EMU)",
                        "text_field_2": "The EMU is a spacesuit that provides environmental protection, "
                                       "mobility, life support, and communications for astronauts",
                        "_id": "article_591"
                    }
                ]
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
                    "text_field_1": "Extravehicular Mobility Unit (EMU)",
                    "text_field_2": "The EMU is a spacesuit that provides environmental protection, "
                                   "mobility, life support, and communications for astronauts",
                    "_id": "article_591"
                } == r2

            r3 = mq.index(test_index_name).get_stats()

            assert r3["numberOfDocuments"] == 2

            if self.IS_MULTI_INSTANCE:
                self.warm_request(mq.index(test_index_name).search,'marco polo', search_method=marqo.SearchMethods.LEXICAL)

            r4 = mq.index(test_index_name).search('marco polo', search_method=marqo.SearchMethods.LEXICAL)
            assert r4["hits"][0]["text_field_1"] == "The Travels of Marco Polo"

            if self.IS_MULTI_INSTANCE:
                self.warm_request(mq.index(test_index_name).search,'adventure', searchable_attributes=['text_field_1'])

            r5 = mq.index(test_index_name).search('adventure', searchable_attributes=['text_field_1'])
            assert len(r5["hits"]) == 2

            r6 = mq.index(test_index_name).delete_documents(ids=["article_591", "article_602"])
            assert r6['details']['deletedDocuments'] == 1

    @mark.fixed
    def test_readme_example_weighted_query(self):
        import marqo
        mq = marqo.Client(**self.client_settings)
        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            mq.index(test_index_name).add_documents([
                    {
                        "text_field_1": "Smartphone",
                        "text_field_2": "A smartphone is a portable computer device that combines mobile telephone "
                        "functions and computing functions into one unit.",
                    },
                    {
                        "text_field_1": "Telephone",
                        "text_field_2": "A telephone is a telecommunications device that permits two or more users to"
                        "conduct a conversation when they are too far apart to be easily heard directly.",
                    },
                    {
                        "text_field_1": "Thylacine",
                        "text_field_2": "The thylacine, also commonly known as the Tasmanian tiger or Tasmanian wolf, "
                        "is an extinct carnivorous marsupial."
                        "The last known of its species died in 1936.",
                    },
                ]
            )

            r1 = mq.index(test_index_name).get_stats()
            assert r1["numberOfDocuments"] == 3

            query = {
                "I need to buy a communications device, what should I get?": 1.1,
                "Technology that became prevelant in the 21st century": 1.0,
            }

            if self.IS_MULTI_INSTANCE:
                self.warm_request(mq.index(test_index_name).search,
                    q=query, searchable_attributes=["text_field_1", "text_field_2"]
                )

            r2 = mq.index(test_index_name).search(
                q=query, searchable_attributes=["text_field_1", "text_field_2"]
            )

            assert r2["hits"][0]["text_field_1"] == "Smartphone"

            print("Query 1:")
            pprint.pprint(r2)
            query = {
                "I need to buy a communications device, what should I get?": 1.0,
                "Technology that became prevelant in the 21st century": -1.0,
            }

            if self.IS_MULTI_INSTANCE:
                self.warm_request(mq.index(test_index_name).search,
                    q=query, searchable_attributes=["text_field_1", "text_field_2"]
                )

            r3 = mq.index(test_index_name).search(
                q=query, searchable_attributes=["text_field_1", "text_field_2"]
            )
            print("\nQuery 2:")
            pprint.pprint(r3)

            assert r3["hits"][0]["text_field_1"] == "Telephone"


            assert r2["hits"][-1]["text_field_1"] == "Thylacine"
            assert r3["hits"][-1]["text_field_1"] == "Thylacine"

            assert len(r2["hits"]) == 3
            assert len(r3["hits"]) == 3


    def test_readme_example_multimodal_combination_query(self):
        import marqo
        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]
        mq = marqo.Client(**self.client_settings)
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            a = mq.index(test_index_name).add_documents(
                [
                    {
                        "text_field_1": "Flying Plane",
                        "caption": "An image of a passenger plane flying in front of the moon.",
                        "image": "https://marqo-assets.s3.amazonaws.com/tests/images/image2.jpg",
                    },
                    {
                        "text_field_1": "Red Bus",
                        "caption": "A red double decker London bus traveling to Aldwych",
                        "image": "https://marqo-assets.s3.amazonaws.com/tests/images/image4.jpg",
                    },
                    {
                        "text_field_1": "Horse Jumping",
                        "caption": "A person riding a horse over a jump in a competition.",
                        "image": "https://marqo-assets.s3.amazonaws.com/tests/images/image1.jpg",
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

            assert r2["hits"][0]["text_field_1"] == "Flying Plane"

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

            assert r3["hits"][0]["text_field_1"] == "Red Bus"

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

            assert r4["hits"][-1]["text_field_1"] == "Horse Jumping"

            assert len(r2["hits"]) == 3
            assert len(r3["hits"]) == 3
            assert len(r4["hits"]) == 3
