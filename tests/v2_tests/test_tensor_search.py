import copy
import marqo
from marqo import enums
from unittest import mock
import requests
import random
import math
import time
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo.errors import MarqoWebError
from pytest import mark


class TestSearch(MarqoTestCase):
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

    @mark.fixed
    def test_search_single(self):
        """Searches an index of a single doc.
        Checks the basic functionality and response structure"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            d1 = {
                "Title": "This is a title about some doc. ",
                "Description": """The Guardian is a British daily newspaper. It was founded in 1821 as The Manchester Guardian, and changed its name in 1959.[5] Along with its sister papers The Observer and The Guardian Weekly, The Guardian is part of the Guardian Media Group, owned by the Scott Trust.[6] The trust was created in 1936 to "secure the financial and editorial independence of The Guardian in perpetuity and to safeguard the journalistic freedom and liberal values of The Guardian free from commercial or political interference".[7] The trust was converted into a limited company in 2008, with a constitution written so as to maintain for The Guardian the same protections as were built into the structure of the Scott Trust by its creators. Profits are reinvested in journalism rather than distributed to owners or shareholders.[7] It is considered a newspaper of record in the UK.[8][9]
                The editor-in-chief Katharine Viner succeeded Alan Rusbridger in 2015.[10][11] Since 2018, the paper's main newsprint sections have been published in tabloid format. As of July 2021, its print edition had a daily circulation of 105,134.[4] The newspaper has an online edition, TheGuardian.com, as well as two international websites, Guardian Australia (founded in 2013) and Guardian US (founded in 2011). The paper's readership is generally on the mainstream left of British political opinion,[12][13][14][15] and the term "Guardian reader" is used to imply a stereotype of liberal, left-wing or "politically correct" views.[3] Frequent typographical errors during the age of manual typesetting led Private Eye magazine to dub the paper the "Grauniad" in the 1960s, a nickname still used occasionally by the editors for self-mockery.[16]
                """
            }
            add_doc_res = self.client.index(test_index_name).add_documents([d1], tensor_fields=["Title", "Description"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                "title about some doc")

            search_res = self.client.index(test_index_name).search(
                "title about some doc")
            assert len(search_res["hits"]) == 1
            assert self.strip_marqo_fields(search_res["hits"][0]) == d1
            assert len(search_res["hits"][0]["_highlights"]) > 0
            assert ("Title" in search_res["hits"][0]["_highlights"][0]) or ("Description" in search_res["hits"][0]["_highlights"][0])

    @mark.fixed
    def test_search_empty_index(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                "title about some doc")

            search_res = self.client.index(test_index_name).search(
                "title about some doc")
            assert len(search_res["hits"]) == 0

    @mark.fixed
    def test_search_highlights(self):
        """Tests if show_highlights works and if the deprecation behaviour is expected"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=test_index_name).add_documents([{"f1": "some doc"}], tensor_fields=["f1"])
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
                    self.warm_request(self.client.index(test_index_name).search,
                    "title about some doc", **params)

                search_res = self.client.index(test_index_name).search(
                    "title about some doc", **params)
                assert ("_highlights" in search_res["hits"][0]) is expected_highlights_presence

    @mark.fixed
    def test_search_multi(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            d1 = {
                    "doc_title": "Cool Document 1",
                    "field_1": "some extra info",
                    "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
                }
            d2 = {
                    "doc_title": "Just Your Average Doc",
                    "field_X": "this is a solid doc",
                    "_id": "123456"
            }
            res = self.client.index(test_index_name).add_documents([
                d1, d2
            ], tensor_fields=["doc_title", "field_X"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                "this is a solid doc")

            search_res = self.client.index(test_index_name).search(
                "this is a solid doc")
            assert d2 == self.strip_marqo_fields(search_res['hits'][0], strip_id=False)
            assert search_res['hits'][0]['_highlights'][0]["field_X"] == "this is a solid doc"

    @mark.fixed
    def test_select_lexical(self):
        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name)
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            d1 = {
                "text_field_1": "Very heavy, dense metallic lead.",
                "text_field_2": "some extra info",
                "_id": "e197e580-0393-4f4e-90e9-8cdf4b17e339"
            }
            d2 = {
                "text_field_1": "The captain bravely lead her followers into battle."
                             " She directed her soldiers to and fro.",
                "text_field_3": "this is a solid doc",
                "_id": "123456"
            }
            res = self.client.index(test_index_name).add_documents([
                d1, d2
            ])

            # Ensure that vector search works
            if self.IS_MULTI_INSTANCE:
                time.sleep(5)
                self.warm_request(self.client.index(test_index_name).search,
                "Examples of leadership", search_method=enums.SearchMethods.TENSOR)

            search_res = self.client.index(test_index_name).search(
                "Examples of leadership", search_method=enums.SearchMethods.TENSOR)
            assert d2 == self.strip_marqo_fields(search_res["hits"][0], strip_id=False)
            assert search_res["hits"][0]['_highlights'][0]["text_field_1"].startswith("The captain bravely lead her followers")

            # try it with lexical search:
            #    can't find the above with synonym
            if self.IS_MULTI_INSTANCE:
                self.client.index(test_index_name).search(
                "Examples of leadership", search_method=marqo.SearchMethods.LEXICAL)

            assert len(self.client.index(test_index_name).search(
                "Examples of leadership", search_method=marqo.SearchMethods.LEXICAL)["hits"]) == 0
            #    but can look for a word

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                '"captain"')

            assert self.client.index(test_index_name).search(
                '"captain"')["hits"][0]["_id"] == "123456"

    @mark.fixed
    def test_search_with_device(self):
        temp_client = copy.deepcopy(self.client)
        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).search(q="my search term", device="cuda:2")
            return True
        assert run()
        # did we set the device properly?
        args, kwargs = mock__post.call_args_list[0]
        assert "device=cuda2" in kwargs["path"]

    @mark.fixed
    def test_search_with_no_device(self):
        """If device not set, do not add it to path"""
        temp_client = copy.deepcopy(self.client)

        mock__post = mock.MagicMock()
        @mock.patch("marqo._httprequests.HttpRequests.post", mock__post)
        def run():
            temp_client.index(self.generic_test_index_name).search(q="my search term")
            return True
        assert run()
        # did we use the defined default device?
        args, kwargs0 = mock__post.call_args_list[0]
        assert "device" not in kwargs0["path"]

    @mark.fixed
    def test_filter_string_and_searchable_attributes(self):
        self.test_cases = [
            (CloudTestIndex.structured_text, self.structured_index_name),
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            docs = [
                {
                    "_id": "0",                     # content in field_a
                    "text_field_1": "random content",
                    "text_field_2": "apple",
                    "int_filter_field_1": 0,
                },
                {
                    "_id": "1",                     # content in field_b
                    "text_field_3": "random content",
                    "text_field_2": "banana",
                    "int_filter_field_1": 0,
                },
                {
                    "_id": "2",                     # content in both
                    "text_field_1": "random content",
                    "text_field_3": "random content",
                    "text_field_2": "apple",
                    "int_filter_field_1": 1,
                },
                {
                    "_id": "3",                     # content in both
                    "text_field_1": "random content",
                    "text_field_3": "random content",
                    "text_field_2": "banana",
                    "int_filter_field_1": 1,
                }
            ]
            res = self.client.index(test_index_name).add_documents(docs)
            print(res)

            test_cases = (
                {   # filter string only (str)
                    "query": "random content",
                    "filter_string": "text_field_2:(apple)",
                    "searchable_attributes": None,
                    "expected": ["0", "2"]
                },
                {   # filter string only (int)
                    "query": "random content",
                    "filter_string": "int_filter_field_1:(0)",
                    "searchable_attributes": None,
                    "expected": ["0", "1"]
                },
                {   # filter string only (str and int)
                    "query": "random content",
                    "filter_string": "text_field_2:(banana) AND int_filter_field_1:(1)",
                    "searchable_attributes": None,
                    "expected": ["3"]
                },
                {  # filter string only (IN with AND)
                    "query": "random content",
                    "filter_string": "text_field_2 in (banana, orange) AND int_filter_field_1 in (0, 1)",
                    "searchable_attributes": None,
                    "expected": ["1", "3"]
                },
                {  # filter string (IN with OR)
                    "query": "random content",
                    "filter_string": "text_field_2 in (banana, orange) OR int_filter_field_1 in (1)",
                    "searchable_attributes": None,
                    "expected": ["1", "2", "3"]
                },
                {   # filter string (IN with _id)
                    "query": "random content",
                    "filter_string": "_id in (1, 2)",
                    "searchable_attributes": None,
                    "expected": ["1", "2"]
                },
                {   # searchable attributes only (one)
                    "query": "random content",
                    "filter_string": None,
                    "searchable_attributes": ["text_field_3"],
                    "expected": ["1", "2", "3"]
                },
                {   # searchable attributes only (both)
                    "query": "random content",
                    "filter_string": None,
                    "searchable_attributes": ["text_field_1", "text_field_3"],
                    "expected": ["0", "1", "2", "3"]
                },
                {   # filter string and searchable attributes (one)
                    "query": "random content",
                    "filter_string": "text_field_2:(apple)",
                    "searchable_attributes": ["text_field_3"],
                    "expected": ["2"]
                },
                {   # filter string and searchable attributes (both)
                    "query": "random content",
                    "filter_string": "text_field_2:(banana) AND int_filter_field_1:(0)",
                    "searchable_attributes": ["text_field_1"],
                    "expected": []
                }
            )

            for case in test_cases:
                if self.IS_MULTI_INSTANCE:
                    self.warm_request(self.client.index(test_index_name).search,
                        case["query"],
                        filter_string=case.get("filter_string", ""),
                        searchable_attributes=case.get("searchable_attributes", None)
                    )

                search_res = self.client.index(test_index_name).search(
                    case["query"],
                    filter_string=case.get("filter_string", ""),
                    searchable_attributes=case.get("searchable_attributes", None)
                )
                assert len(search_res["hits"]) == len(case["expected"])
                assert set([hit["_id"] for hit in search_res["hits"]]) == set(case["expected"])

    @mark.fixed
    def test_attributes_to_retrieve(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            d1 = {
                "doc_title": "Very heavy, dense metallic lead.",
                "abc_123": "some text blah",
                "an_int": "2",
                "_id": "my-cool-doc"
            }
            d2 = {
                "doc_title": "The captain bravely lead her followers into battle."
                             " She directed her soldiers to and fro.",
                "field_X": "this is a solid doc blah",
                "field1": "other things",
                "an_int": "2345678",
                "_id": "123456"
            }
            x = self.client.index(test_index_name).add_documents([
                d1, d2
            ], tensor_fields=['doc_title', 'field_X', 'field1', 'abc_123', 'an_int'])
            atts = ["doc_title", "an_int"]
            for search_method in [enums.SearchMethods.TENSOR,
                                  enums.SearchMethods.LEXICAL]:

                if self.IS_MULTI_INSTANCE:
                    self.warm_request(self.client.index(test_index_name).search,
                        q="blah blah", attributes_to_retrieve=atts,
                        search_method=search_method
                    )

                search_res = self.client.index(test_index_name).search(
                    q="blah blah", attributes_to_retrieve=atts,
                    search_method=search_method
                )
                assert len(search_res['hits']) == 2
                for hit in search_res['hits']:
                    assert {k for k in hit.keys() if not k.startswith('_')} == set(atts)

    @mark.fixed
    def test_pagination_single_field(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            # 100 random words
            vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"
            vocab = requests.get(vocab_source).text.splitlines()
            num_docs = 100
            docs = [{"Title": "a " + (" ".join(random.choices(population=vocab, k=25))),
                                "_id": str(i)
                                }
                            for i in range(num_docs)]

            self.client.index(index_name=test_index_name).add_documents(
                docs, tensor_fields=["Title"], client_batch_size=50
            )
            time.sleep(3)

            for search_method in (enums.SearchMethods.TENSOR, enums.SearchMethods.LEXICAL):
                for doc_count in [100]:
                    # Query full results
                    full_search_results = self.client.index(test_index_name).search(
                                            search_method=search_method,
                                            q='a',
                                            limit=doc_count)

                    for page_size in [1, 5, 10, 100]:
                        paginated_search_results = {"hits": []}

                        for page_num in range(math.ceil(num_docs / page_size)):
                            lim = page_size
                            off = page_num * page_size

                            if self.IS_MULTI_INSTANCE:
                                self.warm_request(self.client.index(test_index_name).search,
                                            search_method=search_method,
                                            q='a',
                                            limit=lim, offset=off)

                            page_res = self.client.index(test_index_name).search(
                                            search_method=search_method,
                                            q='a',
                                            limit=lim, offset=off)

                            paginated_search_results["hits"].extend(page_res["hits"])

                        # Compare paginated to full results (length only for now)
                        assert len(full_search_results["hits"]) == len(paginated_search_results["hits"])

                        # TODO: re-add this assert when KNN incosistency bug is fixed
                        # assert full_search_results["hits"] == paginated_search_results["hits"]

    @mark.fixed
    def test_multi_queries(self):
        docs = [
            {
                "loc_a": "https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_realistic.png",
                "_id": 'realistic_hippo'},
            {"loc_b": "https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_statue.png",
             "_id": 'artefact_hippo'}
        ]
        self.test_cases = [
            (CloudTestIndex.unstructured_image, self.unstructured_image_index_name),
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=test_index_name).add_documents(
                documents=docs, tensor_fields=['loc_a', 'loc_b']
            )
            queries_expected_ordering = [
                ({"Nature photography": 2.0, "Artefact": -2}, ['realistic_hippo', 'artefact_hippo']),
                ({"Nature photography": -1.0, "Artefact": 1.0}, ['artefact_hippo', 'realistic_hippo']),
                ({"https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_statue.png": -1.0,
                  "blah": 1.0}, ['realistic_hippo', 'artefact_hippo']),
                ({"https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_statue.png": 2.0,
                  "https://marqo-assets.s3.amazonaws.com/tests/images/ai_hippo_realistic.png": -1.0},
                 ['artefact_hippo', 'realistic_hippo']),
            ]
            for query, expected_ordering in queries_expected_ordering:
                res = self.client.index(index_name=test_index_name).search(
                    q=query,
                    search_method="TENSOR")
                # the poodle doc should be lower ranked than the irrelevant doc
                for hit_position, _ in enumerate(res['hits']):
                    assert res['hits'][hit_position]['_id'] == expected_ordering[hit_position]

    @mark.fixed
    def test_search_with_ef_search(self):
        """Tests if the ef_search parameter works"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            d1 = {
                "Title": "This is a title about some doc. ",
                "Description": """The Guardian is a British daily newspaper. It was founded in 1821 as The Manchester Guardian, and changed its name in 1959.[5] Along with its sister papers The Observer and The Guardian Weekly, The Guardian is part of the Guardian Media Group, owned by the Scott Trust.[6] The trust was created in 1936 to "secure the financial and editorial independence of The Guardian in perpetuity and to safeguard the journalistic freedom and liberal values of The Guardian free from commercial or political interference".[7] The trust was converted into a limited company in 2008, with a constitution written so as to maintain for The Guardian the same protections as were built into the structure of the Scott Trust by its creators. Profits are reinvested in journalism rather than distributed to owners or shareholders.[7] It is considered a newspaper of record in the UK.[8][9]
                The editor-in-chief Katharine Viner succeeded Alan Rusbridger in 2015.[10][11] Since 2018, the paper's main newsprint sections have been published in tabloid format. As of July 2021, its print edition had a daily circulation of 105,134.[4] The newspaper has an online edition, TheGuardian.com, as well as two international websites, Guardian Australia (founded in 2013) and Guardian US (founded in 2011). The paper's readership is generally on the mainstream left of British political opinion,[12][13][14][15] and the term "Guardian reader" is used to imply a stereotype of liberal, left-wing or "politically correct" views.[3] Frequent typographical errors during the age of manual typesetting led Private Eye magazine to dub the paper the "Grauniad" in the 1960s, a nickname still used occasionally by the editors for self-mockery.[16]
                """
            }
            _ = self.client.index(test_index_name).add_documents([d1], tensor_fields=["Title", "Description"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                "title about some doc")

            # text basic search works
            search_res = self.client.index(test_index_name).search("text", ef_search=200)
            assert len(search_res["hits"]) == 1
            assert self.strip_marqo_fields(search_res["hits"][0]) == d1
            assert len(search_res["hits"][0]["_highlights"]) > 0
            assert ("Title" in search_res["hits"][0]["_highlights"][0]) or ("Description" in search_res["hits"][0]["_highlights"][0])

            # test error on negative ef_search
            with self.assertRaises(MarqoWebError):
                self.client.index(test_index_name).search("text", ef_search=-100)
            
            # test error on ef_search and LEXICAL
            with self.assertRaises(MarqoWebError):
                self.client.index(test_index_name).search("text", search_method='LEXICAL', ef_search=1000)

    @mark.fixed
    def test_search_with_approximate(self):
        """Tests if the approximate parameter works"""
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            d1 = {
                "Title": "This is a title about some doc. ",
                "Description": """The Guardian is a British daily newspaper. It was founded in 1821 as The Manchester Guardian, and changed its name in 1959.[5] Along with its sister papers The Observer and The Guardian Weekly, The Guardian is part of the Guardian Media Group, owned by the Scott Trust.[6] The trust was created in 1936 to "secure the financial and editorial independence of The Guardian in perpetuity and to safeguard the journalistic freedom and liberal values of The Guardian free from commercial or political interference".[7] The trust was converted into a limited company in 2008, with a constitution written so as to maintain for The Guardian the same protections as were built into the structure of the Scott Trust by its creators. Profits are reinvested in journalism rather than distributed to owners or shareholders.[7] It is considered a newspaper of record in the UK.[8][9]
                The editor-in-chief Katharine Viner succeeded Alan Rusbridger in 2015.[10][11] Since 2018, the paper's main newsprint sections have been published in tabloid format. As of July 2021, its print edition had a daily circulation of 105,134.[4] The newspaper has an online edition, TheGuardian.com, as well as two international websites, Guardian Australia (founded in 2013) and Guardian US (founded in 2011). The paper's readership is generally on the mainstream left of British political opinion,[12][13][14][15] and the term "Guardian reader" is used to imply a stereotype of liberal, left-wing or "politically correct" views.[3] Frequent typographical errors during the age of manual typesetting led Private Eye magazine to dub the paper the "Grauniad" in the 1960s, a nickname still used occasionally by the editors for self-mockery.[16]
                """
            }
            _ = self.client.index(test_index_name).add_documents([d1], tensor_fields=["Title", "Description"])

            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search,
                "title about some doc")

            # text basic search works with approximate
            search_res_approx = self.client.index(test_index_name).search("text", approximate=True)
            search_res_exact = self.client.index(test_index_name).search("text", approximate=False)

            for search_res in [search_res_approx, search_res_exact]:
                assert len(search_res["hits"]) == 1
                assert self.strip_marqo_fields(search_res["hits"][0]) == d1
                assert len(search_res["hits"][0]["_highlights"]) > 0
                assert ("Title" in search_res["hits"][0]["_highlights"][0]) or ("Description" in search_res["hits"][0]["_highlights"][0])

            # test error approximate and lexical
            with self.assertRaises(MarqoWebError):
                self.client.index(test_index_name).search("text", search_method='LEXICAL', approximate=True)
            
            # test error approximate and lexical
            with self.assertRaises(MarqoWebError):
                self.client.index(test_index_name).search("text", search_method='LEXICAL', approximate=False)

    @mark.fixed
    def test_escaped_non_tensor_field(self):
        """We need to make sure non tensor field escaping works properly.

        We test to ensure Marqo doesn't match to the non tensor field
        """
        docs = [{
            "dont_tensorise_Me": "Dog",
            "tensorise_me": "quarterly earnings report"
        }]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            self.client.index(index_name=test_index_name).add_documents(
                docs, tensor_fields=["tensorise_me"]
            )
            if self.IS_MULTI_INSTANCE:
                self.warm_request(self.client.index(test_index_name).search, q='Blah')
            search_res = self.client.index(index_name=test_index_name).search("Dog")
            print(search_res)
            assert list(search_res['hits'][0]['_highlights'][0].keys()) == ['tensorise_me']

    def test_special_characters(self):
        """TODO: add more special characters"""
        for special_char, filter_char in [ ("|", "|"), ('#', '#'), ('_', '_')]:
            self.setUp()
            field_to_search = f"tensorise{special_char}me"
            field_to_not_search = f"dont{special_char}tensorise_me"

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
            for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
                test_index_name = self.get_test_index_name(
                    cloud_test_index_to_use=cloud_test_index_to_use,
                    open_source_test_index_name=open_source_test_index_name
                )
                self.client.index(index_name=test_index_name).add_documents(
                    docs, tensor_fields=[field_to_search]
                )

                search_filter_field = f"filter{filter_char}me"
                if self.IS_MULTI_INSTANCE:
                    self.warm_request(self.client.index(test_index_name).search,
                        q="Dog",
                        searchable_attributes=[field_to_search, field_to_not_search],
                        attributes_to_retrieve=[field_to_not_search],
                        filter_string=f'{search_filter_field}:Walrus'
                    )

                search1_res = self.client.index(index_name=test_index_name).search(
                    "Dog", searchable_attributes=[field_to_search, field_to_not_search],
                    attributes_to_retrieve=[field_to_not_search],
                    filter_string=f'{search_filter_field}:Walrus'
                )
                assert len(search1_res['hits']) == 1
                assert search1_res['hits'][0]['_id'] == f"id_{special_char}"
                assert list(search1_res['hits'][0]['_highlights'][0].keys()) == [field_to_search, ]
                assert set(k for k in search1_res['hits'][0].keys() if not k.startswith('_')) == {field_to_not_search}

