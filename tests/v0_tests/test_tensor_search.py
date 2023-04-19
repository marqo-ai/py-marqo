import copy
import math
import random
from unittest import mock

import marqo
import requests
from marqo import enums
from marqo.client import Client
from marqo.errors import MarqoApiError

from tests.marqo_test import MarqoTestCase


class TestAddDocuments(MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError:
            pass

    @staticmethod
    def strip_marqo_fields(doc, strip_id=True):
        """Strips Marqo fields from a returned doc to get the original doc"""
        copied = copy.deepcopy(doc)

        strip_fields = ["_highlights", "_score"]
        if strip_id:
            strip_fields += ["_id"]

        for to_strip in strip_fields:
            del copied[to_strip]

        return copied

    def test_search_single(self):
        """Searches an index of a single doc.
        Checks the basic functionality and response structure"""
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "Title": "This is a title about some doc. ",
            "Description": """The Guardian is a British daily newspaper. It was founded in 1821 as The Manchester Guardian, and changed its name in 1959.[5] Along with its sister papers The Observer and The Guardian Weekly, The Guardian is part of the Guardian Media Group, owned by the Scott Trust.[6] The trust was created in 1936 to "secure the financial and editorial independence of The Guardian in perpetuity and to safeguard the journalistic freedom and liberal values of The Guardian free from commercial or political interference".[7] The trust was converted into a limited company in 2008, with a constitution written so as to maintain for The Guardian the same protections as were built into the structure of the Scott Trust by its creators. Profits are reinvested in journalism rather than distributed to owners or shareholders.[7] It is considered a newspaper of record in the UK.[8][9]
            The editor-in-chief Katharine Viner succeeded Alan Rusbridger in 2015.[10][11] Since 2018, the paper's main newsprint sections have been published in tabloid format. As of July 2021, its print edition had a daily circulation of 105,134.[4] The newspaper has an online edition, TheGuardian.com, as well as two international websites, Guardian Australia (founded in 2013) and Guardian US (founded in 2011). The paper's readership is generally on the mainstream left of British political opinion,[12][13][14][15] and the term "Guardian reader" is used to imply a stereotype of liberal, left-wing or "politically correct" views.[3] Frequent typographical errors during the age of manual typesetting led Private Eye magazine to dub the paper the "Grauniad" in the 1960s, a nickname still used occasionally by the editors for self-mockery.[16]
            """
        }
        self.client.index(self.index_name_1).add_documents([d1])
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,
            "title about some doc")

        search_res = self.client.index(self.index_name_1).search(
            "title about some doc")
        assert len(search_res["hits"]) == 1
        assert self.strip_marqo_fields(search_res["hits"][0]) == d1
        assert len(search_res["hits"][0]["_highlights"]) > 0
        assert ("Title" in search_res["hits"][0]["_highlights"]) or ("Description" in search_res["hits"][0]["_highlights"])

    def test_search_empty_index(self):
        self.client.create_index(index_name=self.index_name_1)

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,
            "title about some doc")

        search_res = self.client.index(self.index_name_1).search(
            "title about some doc")
        assert len(search_res["hits"]) == 0

    def test_search_highlights(self):
        """Tests if show_highlights works and if the deprecation behaviour is expected"""
        self.client.create_index(index_name=self.index_name_1)
        self.client.index(index_name=self.index_name_1).add_documents([{"f1": "some doc"}])
        for params, expected_highlights_presence in [
                ({"highlights": True, "show_highlights": False}, False),
                ({"highlights": False, "show_highlights": True}, False),
                ({"highlights": True, "show_highlights": True}, True),
                ({"highlights": True}, True),
                ({"highlights": False}, False),
                ({}, True),
                ({"show_highlights": False}, False),
                ({"show_highlights": True}, True)
            ]:

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.index_name_1).search,
                "title about some doc", **params)

            search_res = self.client.index(self.index_name_1).search(
                "title about some doc", **params)
            assert ("_highlights" in search_res["hits"][0]) is expected_highlights_presence

    def test_search_multi(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
                "doc title": "Cool Document 1",
                "field 1": "some extra info",
                "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
            }
        d2 = {
                "doc title": "Just Your Average Doc",
                "field X": "this is a solid doc",
                "_id": "123456"
        }
        self.client.index(self.index_name_1).add_documents([
            d1, d2
        ])

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,
            "this is a solid doc")

        search_res = self.client.index(self.index_name_1).search(
            "this is a solid doc")
        assert d2 == self.strip_marqo_fields(search_res['hits'][0], strip_id=False)
        assert search_res['hits'][0]['_highlights']["field X"] == "this is a solid doc"

    def test_select_lexical(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "field 1": "some extra info",
            "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc",
            "_id": "123456"
        }
        self.client.index(self.index_name_1).add_documents([
            d1, d2
        ])

        # Ensure that vector search works
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,
            "Examples of leadership", search_method=enums.SearchMethods.TENSOR)

        search_res = self.client.index(self.index_name_1).search(
            "Examples of leadership", search_method=enums.SearchMethods.TENSOR)
        assert d2 == self.strip_marqo_fields(search_res["hits"][0], strip_id=False)
        assert search_res["hits"][0]['_highlights']["doc title"].startswith("The captain bravely lead her followers")

        # try it with lexical search:
        #    can't find the above with synonym
        if self.IS_MULTI_INSTANCE:
            self.client.index(self.index_name_1).search(
            "Examples of leadership", search_method=marqo.SearchMethods.LEXICAL)

        assert len(self.client.index(self.index_name_1).search(
            "Examples of leadership", search_method=marqo.SearchMethods.LEXICAL)["hits"]) == 0
        #    but can look for a word

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,
            '"captain"')

        assert self.client.index(self.index_name_1).search(
            '"captain"')["hits"][0]["_id"] == "123456"

    def test_search_with_device(self):
        """use default as defined in config unless overridden"""
        temp_client = copy.deepcopy(self.client)
        temp_client.config.search_device = "cpu:4"
        temp_client.config.indexing_device = enums.Devices.cpu

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.index_name_1).search(q="my search term")
            temp_client.index(self.index_name_1).search(q="my search term", device="cuda:2")
            return True
        assert run()
        # did we use the defined default device?
        args, kwargs0 = mock__post.call_args_list[0]
        assert "device=cpu4" in kwargs0["path"]
        # did we overrride the default device?
        args, kwargs1 = mock__post.call_args_list[1]
        assert "device=cuda2" in kwargs1["path"]

    def test_prefiltering(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "abc-123": "some text",
            "an_int": 2,
            "_id": "my-cool-doc"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc",
            "field1": "other things",
            "_id": "123456"
        }
        self.client.index(self.index_name_1).add_documents([
            d1, d2
        ],auto_refresh=True)

        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search,
                "blah blah",
                filter_string="(an_int:[0 TO 30] and an_int:2) AND abc-123:(some text)")

        search_res = self.client.index(self.index_name_1).search(
            "blah blah",
            filter_string="(an_int:[0 TO 30] and an_int:2) AND abc-123:(some text)")
        assert len(search_res["hits"]) == 1
        assert search_res["hits"][0]["_id"] == "my-cool-doc"

    def test_attributes_to_retrieve(self):
        self.client.create_index(index_name=self.index_name_1)
        d1 = {
            "doc title": "Very heavy, dense metallic lead.",
            "abc-123": "some text blah",
            "an_int": 2,
            "_id": "my-cool-doc"
        }
        d2 = {
            "doc title": "The captain bravely lead her followers into battle."
                         " She directed her soldiers to and fro.",
            "field X": "this is a solid doc blah",
            "field1": "other things",
            "an_int": 2345678,
            "_id": "123456"
        }
        self.client.index(self.index_name_1).add_documents([
            d1, d2
        ], auto_refresh=True)
        atts = ["doc title", "an_int"]
        for search_method in [enums.SearchMethods.TENSOR,
                              enums.SearchMethods.LEXICAL]:
            
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.index_name_1).search,
                    q="blah blah", attributes_to_retrieve=atts,
                    search_method=search_method
                )

            search_res = self.client.index(self.index_name_1).search(
                q="blah blah", attributes_to_retrieve=atts,
                search_method=search_method
            )
            assert len(search_res['hits']) == 2
            for hit in search_res['hits']:
                assert {k for k in hit.keys() if not k.startswith('_')} == set(atts)

        
    def test_pagination_single_field(self):
        self.client.create_index(index_name=self.index_name_1)
        
        # 100 random words
        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
        vocab = requests.get(vocab_source).text.splitlines()
        num_docs = 100
        docs = [{"Title": "a " + (" ".join(random.choices(population=vocab, k=25))),
                            "_id": str(i)
                            }
                        for i in range(num_docs)]
        
        self.client.index(index_name=self.index_name_1).add_documents(
            docs, auto_refresh=False, client_batch_size=50
        )
        self.client.index(index_name=self.index_name_1).refresh()

        for search_method in (enums.SearchMethods.TENSOR, enums.SearchMethods.LEXICAL):
            for doc_count in [100]:
                # Query full results
                full_search_results = self.client.index(self.index_name_1).search(
                                        search_method=search_method,
                                        q='a', 
                                        limit=doc_count)

                for page_size in [1, 5, 10, 100]:
                    paginated_search_results = {"hits": []}

                    for page_num in range(math.ceil(num_docs / page_size)):
                        lim = page_size
                        off = page_num * page_size

                        if self.IS_MULTI_INSTANCE:
                            self.warm_request(self.client.index(self.index_name_1).search,
                                        search_method=search_method,
                                        q='a', 
                                        limit=lim, offset=off)

                        page_res = self.client.index(self.index_name_1).search(
                                        search_method=search_method,
                                        q='a', 
                                        limit=lim, offset=off)
                        
                        paginated_search_results["hits"].extend(page_res["hits"])

                    # Compare paginated to full results (length only for now)
                    assert len(full_search_results["hits"]) == len(paginated_search_results["hits"])

                    # TODO: re-add this assert when KNN incosistency bug is fixed
                    # assert full_search_results["hits"] == paginated_search_results["hits"]

    def test_multi_queries(self):
        docs = [
            {
                "loc a": "https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_realistic.png",
                "_id": 'realistic_hippo'},
            {"loc b": "https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png",
             "_id": 'artefact_hippo'}
        ]
        image_index_config = {
            'index_defaults': {
                'model': "ViT-B/16",
                'treat_urls_and_pointers_as_images': True
            }
        }
        self.client.create_index(index_name=self.index_name_1, settings_dict=image_index_config)
        self.client.index(index_name=self.index_name_1).add_documents(
            documents=docs, auto_refresh=True
        )
        queries_expected_ordering = [
            ({"Nature photography": 2.0, "Artefact": -2}, ['realistic_hippo', 'artefact_hippo']),
            ({"Nature photography": -1.0, "Artefact": 1.0}, ['artefact_hippo', 'realistic_hippo']),
            ({"https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png": -1.0,
              "blah": 1.0}, ['realistic_hippo', 'artefact_hippo']),
            ({"https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png": 2.0,
              "https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_realistic.png": -1.0},
             ['artefact_hippo', 'realistic_hippo']),
        ]
        for query, expected_ordering in queries_expected_ordering:
            res = self.client.index(index_name=self.index_name_1).search(
                q=query,
                search_method="TENSOR")
            print(res)
            # the poodle doc should be lower ranked than the irrelevant doc
            for hit_position, _ in enumerate(res['hits']):
                assert res['hits'][hit_position]['_id'] == expected_ordering[hit_position]

    def test_escaped_non_tensor_field(self):
        """We need to make sure non tensor field escaping works properly.

        We test to ensure Marqo doesn't match to the non tensor field
        """
        docs = [{
            "dont#tensorise Me": "Dog",
            "tensorise_me": "quarterly earnings report"
        }]
        self.client.index(index_name=self.index_name_1).add_documents(
            docs, auto_refresh=True, non_tensor_fields=["dont#tensorise Me"]
        )
        if self.IS_MULTI_INSTANCE:
            self.warm_request(self.client.index(self.index_name_1).search, q='Blah')
        search_res = self.client.index(index_name=self.index_name_1).search("Dog")
        assert list(search_res['hits'][0]['_highlights'].keys()) == ['tensorise_me']

    def test_special_characters(self):
        """TODO: add more special characters"""
        for special_char, filter_char in [ ("|", "|"), ('#', '#'), (' ', '\ '), ('_', '_')]:
            self.setUp()
            field_to_search = f"tensorise{special_char}me"
            field_to_not_search = f"dont{special_char}tensorise me"
            filter_field = f"filter{special_char}me"
            docs = [{
                field_to_not_search: "Dog",
                field_to_search: "quarterly earnings report",
                filter_field: "Walrus",
                "red herring": "Dog",
                "_id": f"id_{special_char}"
            }, {
                field_to_search: "Dog",
                filter_field: "Alpaca"
            }
            ]
            self.client.index(index_name=self.index_name_1).add_documents(
                docs, auto_refresh=True, non_tensor_fields=[field_to_not_search]
            )
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(self.index_name_1).search, q='Blah')
            search_filter_field = f"filter{filter_char}me"
            search1_res = self.client.index(index_name=self.index_name_1).search(
                "Dog", searchable_attributes=[field_to_search, field_to_not_search],
                attributes_to_retrieve=[field_to_not_search],
                filter_string=f'{search_filter_field}:Walrus'
            )
            assert len(search1_res['hits']) == 1
            assert search1_res['hits'][0]['_id'] == f"id_{special_char}"
            assert list(search1_res['hits'][0]['_highlights'].keys()) == [field_to_search, ]
            assert set(k for k in search1_res['hits'][0].keys() if not k.startswith('_')) == {field_to_not_search}

