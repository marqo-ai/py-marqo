import json

import requests
from PIL import Image
from marqo.client import Client
from marqo.errors import MarqoApiError
import numpy as np
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from pytest import mark


class TestImageChunking(MarqoTestCase):
    """Test for image chunking as a preprocessing step
    """

    def test_image_no_chunking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)
        if not client.config.is_marqo_cloud:
            try:
                client.delete_index(self.generic_test_index_name)
            except MarqoApiError as s:
                pass

        self.test_cases = [
            (CloudTestIndex.structured_image, self.structured_image_index_name),
        ]
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )

            temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'

            document1 = {'_id': '1', # '_id' can be provided but is not required
                'text_field_1': 'hello',
                'text_field_2': 'the image chunking can (optionally) chunk the image into sub-patches (aking to segmenting text) by using either a learned model or simple box generation and cropping',
                'image_field_1': temp_file_name,
                         }

            client.index(test_index_name).add_documents([document1])

            # test the search works
            if self.IS_MULTI_INSTANCE:
                self.warm_request(client.index(test_index_name).search,'a')

            results = client.index(test_index_name).search('a')
            assert results['hits'][0]['image_field_1'] == temp_file_name

            # search only the image location
            if self.IS_MULTI_INSTANCE:
                self.warm_request(client.index(test_index_name).search,'a', searchable_attributes=['image_field_1'])

            results = client.index(test_index_name).search('a', searchable_attributes=['image_field_1'])
            assert results['hits'][0]['image_field_1'] == temp_file_name
            # the highlight should be the location
            assert results['hits'][0]['_highlights'][0]['image_field_1'] == temp_file_name

    @mark.fixed
    def test_image_simple_chunking(self):

        image_size = (256, 384)

        client = Client(**self.client_settings)
        if not client.config.is_marqo_cloud:
            try:
                client.delete_index(self.generic_test_index_name)
            except MarqoApiError as s:
                pass

        settings = {
            "type": "structured",
        "model": "open_clip/ViT-B-32/laion2b_s34b_b79k",
        "allFields": [
            {"name": "text_field_1", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "text_field_2", "type": "text", "features": ["lexical_search", "filter"]},
            {"name": "image_field_1", "type": "image_pointer"},
            ],
            "tensorFields": ["text_field_1", "text_field_2", "image_field_1"],
            "imagePreprocessing": {
                "patchMethod": "simple",
            },
            }

        test_index_name = self.get_test_index_name(
            cloud_test_index_to_use=CloudTestIndex.structured_image,
            open_source_test_index_name=None
        )
        if not self.client.config.is_marqo_cloud:
            self.client.create_index(self.generic_test_index_name, settings_dict=settings)
            test_index_name = self.generic_test_index_name
        temp_file_name = 'https://avatars.githubusercontent.com/u/13092433?v=4'
        
        img = Image.open(requests.get(temp_file_name, stream=True).raw)

        document1 = {'_id': '1',  # '_id' can be provided but is not required
            'text_field_1': 'hello',
            'text_field_2': 'the image chunking can (optionally) chunk the image into sub-patches (akin to segmenting text) by using either a learned model or simple box generation and cropping',
            'image_field_1': temp_file_name}

        client.index(test_index_name).add_documents([document1])

        # test the search works
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,'a')

        results = client.index(test_index_name).search('a')
        assert results['hits'][0]['image_field_1'] == temp_file_name

        # search only the image location
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,'a', searchable_attributes=['image_field_1'])

        results = client.index(test_index_name).search('a', searchable_attributes=['image_field_1'])
        assert results['hits'][0]['image_field_1'] == temp_file_name
        # the highlight should be the location
        assert json.loads(results['hits'][0]['_highlights'][0]['image_field_1']) != temp_file_name
        assert len(json.loads(results['hits'][0]['_highlights'][0]['image_field_1'])) == 4
        assert all(
            isinstance(_n, (float, int)) for _n in json.loads(results['hits'][0]['_highlights'][0]['image_field_1'])
        )

        # search using the image itself, should return a full sized image as highlight
        if self.IS_MULTI_INSTANCE:
            self.warm_request(client.index(test_index_name).search,temp_file_name)

        results = client.index(test_index_name).search(temp_file_name)
        assert abs(np.array(json.loads(results['hits'][0]['_highlights'][0]['image_field_1'])) - np.array([0, 0, img.size[0], img.size[1]])).sum() < 1e-6

        if not self.client.config.is_marqo_cloud:
            self.client.delete_index(test_index_name)
