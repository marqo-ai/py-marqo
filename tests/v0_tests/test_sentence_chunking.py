from marqo.client import Client
from tests.marqo_test import MarqoTestCase, CloudTestIndex


class TestSentenceChunking(MarqoTestCase):
    """Test for sentence chunking

    Assumptions:
        - Local OpenSearch (not S2Search)
    """
    def test_sentence_no_chunking(self):

        client = Client(**self.client_settings)

        settings = {
            "sentences_per_chunk":int(1e3),  
            "sentence_overlap":0 
            }

        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_kwargs=settings
        )

        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello. how are you. another one.',
            'description': 'the image chunking. can (optionally) chunk. the image into sub-patches (aking to segmenting text). by using either. a learned model. or simple box generation and cropping.',
            'misc':'sasasasaifjfnonfqeno asadsdljknjdfln'}

        client.index(test_index_name).add_documents([document1], tensor_fields=['attributes', 'description', 'misc'])

        # test the search works
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,'a')

        results = client.index(test_index_name).search('a')
        print(results)
        assert results['hits'][0]['attributes'] == document1['attributes']

        assert results['hits'][0]['description'] == document1['description']

        assert results['hits'][0]['misc'] == document1['misc']

    def test_sentence_chunking_no_overlap(self):

        client = Client(**self.client_settings)

        settings = {
            "sentences_per_chunk":2,  
            "sentence_overlap":0 
            }

        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_kwargs=settings
        )

        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello. how are you. another one.',
            'description': 'the image chunking. can (optionally) chunk. the image into sub-patches (aking to segmenting text). by using either. a learned model. or simple box generation and cropping.',
            'misc':'sasasasaifjfnonfqeno asadsdljknjdfln'}

        client.index(test_index_name).add_documents([document1], tensor_fields=['attributes', 'description', 'misc'])

        # search with a term we know is an exact chunk and will then show in the highlights
        search_term = 'hello. how are you.'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)

        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['attributes'] == search_term

        # search with a term we know is an exact chunk and will then show in the highlights
        search_term = 'the image into sub-patches (aking to segmenting text). by using either.'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)

        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['description'] == search_term

        # search with a term we know is an exact chunk and will then show in the highlights
        search_term = 'sasasasaifjfnonfqeno asadsdljknjdfln'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)

        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['misc'] == search_term

        # search with a term we know is part of a sub-chunk and verify it is overlapping in the correct sentence
        search_term = 'can (optionally) chunk.'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)

        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['description'] == 'the image chunking. can (optionally) chunk.'

    def test_sentence_chunking_overlap(self):

        client = Client(**self.client_settings)

        settings = {
            "sentences_per_chunk":2,  
            "sentence_overlap":1
            }

        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.text_index_with_custom_model,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_kwargs=settings
        )


        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello. how are you. another one.',
            'description': 'the image chunking. can (optionally) chunk. the image into sub-patches (aking to segmenting text). by using either. a learned model. or simple box generation and cropping.',
            'misc':'sasasasaifjfnonfqeno asadsdljknjdfln'}

        client.index(test_index_name).add_documents([document1], tensor_fields=['attributes', 'description', 'misc'])

        # search with a term we know is an exact chunk and will then show in the highlights
        search_term = 'hello. how are you.'

        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)
        
        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['attributes'] == search_term

        # search with a term we know is an exact chunk and will then show in the highlights
        search_term = 'the image into sub-patches (aking to segmenting text). by using either.'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)
        
        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['description'] == search_term

        # search with a term we know is an exact chunk and will then show in the highlights
        search_term = 'sasasasaifjfnonfqeno asadsdljknjdfln'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)
        
        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['misc'] == search_term

        # search with a term we know is part of a sub-chunk and verify it is overlapping in the correct sentence
        search_term = 'can (optionally) chunk.'
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)
        
        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['description'] == 'the image chunking. can (optionally) chunk.'

        # check overlap
        search_term = "can (optionally) chunk. the image into sub-patches (aking to segmenting text)."
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)
        
        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['description'] == search_term

        search_term = "the image into sub-patches (aking to segmenting text). by using either."
        
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,search_term)
        
        results = client.index(test_index_name).search(search_term)
        print(results)
        assert results['hits'][0]['_highlights']['description'] == search_term
