import requests
from PIL import Image
from marqo.client import Client
from marqo.errors import MarqoApiError
import unittest
import numpy as np
import pprint
from tests.marqo_test import MarqoTestCase
import tempfile
import os


class TestImageReranking(MarqoTestCase):
    """Test for image chunking as a preprocessing step
    """
    def setUp(self) -> None:
        client_0 = Client(**self.client_settings)
        
        self.index_name = 'image-chunk-test'

        try:
            client_0.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

    def test_image_reranking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)
        
        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
             "image_preprocessing_method" : None
            }
        
        client.create_index(self.index_name, **settings)
        
        documents = [{'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': 'https://avatars.githubusercontent.com/u/13092433?v=4'},
            {'_id': '2', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the imo segmenting text) by using either a learned model or simple box generation and cropping'},
            {'_id': '3', # '_id' can be provided but is not required
            'description': 'ing either a learned model or simple box generation and cropping. brain',
            'location': 'https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png'},
        ]

        client.index(self.index_name).add_documents(documents, non_tensor_fields=[])

        ###### proper way to search over images
        # test the search works
        results = client.index(self.index_name).search('brain', searchable_attributes=['location'])
        
        assert results['hits'][0]['location'] == documents[0]['location']

        results = client.index(self.index_name).search('hippo', searchable_attributes=['location'])
      
        assert results['hits'][0]['location'] == documents[2]['location']

        ###### proper way to search over images with reranking
        results = client.index(self.index_name).search('brain', searchable_attributes=['location'], reranker='google/owlvit-base-patch32')
      
        assert results['hits'][0]['location'] == documents[0]['location']
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

        # 
        results = client.index(self.index_name).search('hippo', searchable_attributes=['location'], reranker='google/owlvit-base-patch32')
        
        assert results['hits'][0]['location'] == documents[2]['location']
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

        ###### search with lexical and no results are returned
        results = client.index(self.index_name).search('brain', searchable_attributes=['location'], reranker='google/owlvit-base-patch32', search_method='LEXICAL')        
        assert results['hits'] == []

        ###### search with multiple fields and lexical - will only use the first for reranking
        results = client.index(self.index_name).search('brain', searchable_attributes=['location', 'description'], reranker='google/owlvit-base-patch32', search_method='LEXICAL')        
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

        ###### search with multiple fields and tensor - will only use the first for reranking
        results = client.index(self.index_name).search('brain', searchable_attributes=['location', 'description'], reranker='google/owlvit-base-patch32', search_method='TENSOR')        
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

    def test_image_reranking_searchable_is_none_error(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)
        
        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
             "image_preprocessing_method" : None
            }
        
        client.create_index(self.index_name, **settings)
        
        documents = [{'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': '/local/u/1309243.jpg'},
            {'_id': '2', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the imo segmenting text) by using either a learned model or simple box generation and cropping'},
            {'_id': '3', # '_id' can be provided but is not required
            'description': 'ing either a learned model or simple box generation and cropping. brain',
            'location': 'https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png'},
        ]

        client.index(self.index_name).add_documents(documents, non_tensor_fields=[])

        # test Errors
        # # test the search works with the reranking and no searchable attributes
        try:
            results = client.index(self.index_name).search('brain', searchable_attributes=None, reranker='google/owlvit-base-patch32')
        except Exception as e:
            assert 'searchable_attributes cannot be None' in str(e)

    def test_image_reranking_model_name_error(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)
        
        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
             "image_preprocessing_method" : None
            }
        
        client.create_index(self.index_name, **settings)
        
        documents = [{'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': '/local/u/1309243.jpg'},
            {'_id': '2', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the imo segmenting text) by using either a learned model or simple box generation and cropping'},
            {'_id': '3', # '_id' can be provided but is not required
            'description': 'ing either a learned model or simple box generation and cropping. brain',
            'location': 'https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png'},
        ]

        res = client.index(self.index_name).add_documents(documents, non_tensor_fields=[])
        print(res)

        # test Errors
        # # test the search works with the reranking and no searchable attributes
        try:
            results = client.index(self.index_name).search('brain', searchable_attributes=['location'], reranker='google/owlvi-base-patch32')
            assert False
        except Exception as e:
            assert "could not find model_name=" in str(e)
         

    def test_image_reranking_with_chunking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)
        
        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
             "image_preprocessing_method" : 'marqo-yolo'
            }
        
        client.create_index(self.index_name, **settings)
        
        documents = [{'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': 'https://avatars.githubusercontent.com/u/13092433?v=4'},
            {'_id': '2', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the imo segmenting text) by using either a learned model or simple box generation and cropping'},
            {'_id': '3', # '_id' can be provided but is not required
            'description': 'ing either a learned model or simple box generation and cropping. brain',
            'location': 'https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png'},
        ]

        client.index(self.index_name).add_documents(documents, non_tensor_fields=[])

        ###### proper way to search over images
        # test the search works
        results = client.index(self.index_name).search('brain', searchable_attributes=['location'])
        
        assert results['hits'][0]['location'] == documents[0]['location']

        results = client.index(self.index_name).search('hippo', searchable_attributes=['location'])
      
        assert results['hits'][0]['location'] == documents[2]['location']

        ###### proper way to search over images with reranking
        results = client.index(self.index_name).search('brain', searchable_attributes=['location'], reranker='google/owlvit-base-patch32')
      
        assert results['hits'][0]['location'] == documents[0]['location']
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

        # 
        results = client.index(self.index_name).search('hippo', searchable_attributes=['location'], reranker='google/owlvit-base-patch32')
        
        assert results['hits'][0]['location'] == documents[2]['location']
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

        ###### search with lexical and no results are returned
        results = client.index(self.index_name).search('brain', searchable_attributes=['location'], reranker='google/owlvit-base-patch32', search_method='LEXICAL')        
        assert results['hits'] == []

        ###### search with multiple fields and lexical - will only use the first for reranking
        results = client.index(self.index_name).search('brain', searchable_attributes=['location', 'description'], reranker='google/owlvit-base-patch32', search_method='LEXICAL')        
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4

        ###### search with multiple fields and tensor - will only use the first for reranking
        results = client.index(self.index_name).search('brain', searchable_attributes=['location', 'description'], reranker='google/owlvit-base-patch32', search_method='TENSOR')        
        assert 'location' in results['hits'][0]['_highlights']
        assert len(results['hits'][0]['_highlights']['location']) == 4