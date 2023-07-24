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


class TestImageChunking(MarqoTestCase):
    """Test for image chunking as a preprocessing step
    """
    def setUp(self) -> None:
        client_0 = Client(**self.client_settings)
        
        self.index_name = 'image-chunk-test'

        try:
            client_0.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

    def test_image_no_chunking(self):

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

        temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'
        
        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name}

        client.index(self.index_name).add_documents([document1], non_tensor_fields=[])

        # test the search works
        results = client.index(self.index_name).search('a')
        print(results)
        assert results['hits'][0]['location'] == temp_file_name

        # search only the image location
        results = client.index(self.index_name).search('a', searchable_attributes=['location'])
        print(results)
        assert results['hits'][0]['location'] == temp_file_name
        # the highlight should be the location
        assert results['hits'][0]['_highlights']['location'] == temp_file_name

    def test_image_simple_chunking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)

        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
            "image_preprocessing_method":"simple"
            }
        
        client.create_index(self.index_name, **settings)

        
        temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'
        
        img = Image.open(requests.get(temp_file_name, stream=True).raw)

        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (akin to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name}

        client.index(self.index_name).add_documents([document1], non_tensor_fields=[])

        # test the search works
        results = client.index(self.index_name).search('a')
        print(results)
        assert results['hits'][0]['location'] == temp_file_name

        # search only the image location
        results = client.index(self.index_name).search('a', searchable_attributes=['location'])
        print(results)
        assert results['hits'][0]['location'] == temp_file_name
        # the highlight should be the location
        assert results['hits'][0]['_highlights']['location'] != temp_file_name
        assert len(results['hits'][0]['_highlights']['location']) == 4
        assert all(isinstance(_n, (float, int)) for _n in results['hits'][0]['_highlights']['location'])

        # search using the image itself, should return a full sized image as highlight
        results = client.index(self.index_name).search(temp_file_name)
        print(results)
        assert abs(np.array(results['hits'][0]['_highlights']['location']) - np.array([0, 0, img.size[0], img.size[1]])).sum() < 1e-6

    def test_image_simple_chunking_multifield(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)

        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
            "image_preprocessing_method":"simple"
            }
        
        client.create_index(self.index_name, **settings)
        
        temp_file_name_1 = 'https://avatars.githubusercontent.com/u/13092433?v=4' #brain
        temp_file_name_2 = 'https://raw.githubusercontent.com/marqo-ai/marqo-api-tests/mainline/assets/ai_hippo_statue.png' #hippo

        img = Image.open(requests.get(temp_file_name_1, stream=True).raw)
        

        documents = [{'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name_1},
            {'_id': '11', # '_id' can be provided but is not required
            'attributes': 'hello sdsd ' ,
            'description': 'the im sds age csdsdssdsddhunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name_1,
            'location_1': temp_file_name_2},
            {'_id': '2', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the imo segmenting text) by using either a learned model or simple box generation and cropping'},
            {'_id': '3', # '_id' can be provided but is not required
            'description': 'sds s ds',
            'location_1': temp_file_name_2},
        ]

        client.index(self.index_name).add_documents(documents, non_tensor_fields=[])

        # test the search works
        results = client.index(self.index_name).search('brain', searchable_attributes=['location', 'location_1'])
        print(results)
        assert results['hits'][0]['location'] == temp_file_name_1, results

        # search only the image location
        results = client.index(self.index_name).search('hippo', searchable_attributes=['location', 'location_1'])
        print(results)
        assert results['hits'][0]['location_1'] == temp_file_name_2
        # the highlight should be the location
        assert results['hits'][0]['_highlights']['location_1'] != temp_file_name_2
        assert len(results['hits'][0]['_highlights']['location_1']) == 4
        assert all(isinstance(_n, (float, int)) for _n in results['hits'][0]['_highlights']['location_1'])

        # search using the image itself, should return a full sized image as highlight
        results = client.index(self.index_name).search(temp_file_name_1)
        print(results)
        assert abs(np.array(results['hits'][0]['_highlights']['location']) - np.array([0, 0, img.size[0], img.size[1]])).sum() < 1e-6

    def test_image_yolox_chunking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)

        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
            "image_preprocessing_method":"marqo-yolo"
            }
        
        client.create_index(self.index_name, **settings)

        
        temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'
        
        img = Image.open(requests.get(temp_file_name, stream=True).raw)

        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (akin to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name}

        client.index(self.index_name).add_documents([document1], non_tensor_fields=[])

        # test the search works
        results = client.index(self.index_name).search('a')
        print(results)
        assert results['hits'][0]['location'] == temp_file_name

        # search only the image location
        results = client.index(self.index_name).search('a', searchable_attributes=['location'])
        print(results)
        assert results['hits'][0]['location'] == temp_file_name
        # the highlight should be the location
        assert results['hits'][0]['_highlights']['location'] != temp_file_name
        assert len(results['hits'][0]['_highlights']['location']) == 4
        assert all(isinstance(_n, (float, int)) for _n in results['hits'][0]['_highlights']['location'])

        # search using the image itself, should return a full sized image as highlight
        results = client.index(self.index_name).search(temp_file_name)
        print(results)
        assert abs(np.array(results['hits'][0]['_highlights']['location']) - np.array([0, 0, img.size[0], img.size[1]])).sum() < 1e-6

    def test_image_dino_chunking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)

        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
            "image_preprocessing_method":"dino-v1"
            }
        
        client.create_index(self.index_name, **settings)

        
        temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'
        
        img = Image.open(requests.get(temp_file_name, stream=True).raw)

        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (akin to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name}

        client.index(self.index_name).add_documents([document1], non_tensor_fields=[])

        # test the search works
        results = client.index(self.index_name).search('a')
        print(results)
        assert results['hits'][0]['location'] == temp_file_name

        # search only the image location
        results = client.index(self.index_name).search('a', searchable_attributes=['location'])
        print(results)
        assert results['hits'][0]['location'] == temp_file_name
        # the highlight should be the location
        assert results['hits'][0]['_highlights']['location'] != temp_file_name
        assert len(results['hits'][0]['_highlights']['location']) == 4
        assert all(isinstance(_n, (float, int)) for _n in results['hits'][0]['_highlights']['location'])

        # search using the image itself, should return a full sized image as highlight
        results = client.index(self.index_name).search(temp_file_name)
        print(results)
        assert abs(np.array(results['hits'][0]['_highlights']['location']) - np.array([0, 0, img.size[0], img.size[1]])).sum() < 1e-6

        try:
            client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass

        settings = {
            "treat_urls_and_pointers_as_images":True,   # allows us to find an image file and index it 
            "model":"ViT-B/16",
            "image_preprocessing_method":"dino-v2"
            }
        
        client.create_index(self.index_name, **settings)

        
        temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'
        
        img = Image.open(requests.get(temp_file_name, stream=True).raw)

        document1 = {'_id': '1', # '_id' can be provided but is not required
            'attributes': 'hello',
            'description': 'the image chunking can (optionally) chunk the image into sub-patches (akin to segmenting text) by using either a learned model or simple box generation and cropping',
            'location': temp_file_name}

        client.index(self.index_name).add_documents([document1], non_tensor_fields=[])

        # test the search works
        results = client.index(self.index_name).search('a')
        print(results)
        assert results['hits'][0]['location'] == temp_file_name

        # search only the image location
        results = client.index(self.index_name).search('a', searchable_attributes=['location'])
        print(results)
        assert results['hits'][0]['location'] == temp_file_name
        # the highlight should be the location
        assert results['hits'][0]['_highlights']['location'] != temp_file_name
        assert len(results['hits'][0]['_highlights']['location']) == 4
        assert all(isinstance(_n, (float, int)) for _n in results['hits'][0]['_highlights']['location'])

        # search using the image itself, should return a full sized image as highlight
        results = client.index(self.index_name).search(temp_file_name)
        print(results)
        assert abs(np.array(results['hits'][0]['_highlights']['location']) - np.array([0, 0, img.size[0], img.size[1]])).sum() < 1e-6

        