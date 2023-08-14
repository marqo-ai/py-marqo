from tests.marqo_test import MarqoTestCase
from pytest import mark


class TestGetSettings(MarqoTestCase):
    def test_default_settings(self):
        """default fields should be returned if index is created with default settings
            sample structure of output: {'index_defaults': {'treat_urls_and_pointers_as_images': False,
                                          'text_preprocessing': {'split_method': 'sentence', 'split_length': 2,
                                                                 'split_overlap': 0},
                                          'model': 'hf/all_datasets_v4_MiniLM-L6', 'normalize_embeddings': True,
                                          'image_preprocessing': {'patch_method': None}}, 'number_of_shards': 5,
                                          'number_of_replicas' : 1,}
        """
        test_index_name = self.create_test_index(index_name=self.generic_test_index_name)

        ix = self.client.index(test_index_name)
        index_settings = ix.get_settings()
        fields = {'treat_urls_and_pointers_as_images', 'text_preprocessing', 'model', 'normalize_embeddings',
                  'image_preprocessing'}

        self.assertIn('index_defaults', index_settings)
        self.assertIn('number_of_shards', index_settings)
        self.assertIn("number_of_replicas", index_settings)
        self.assertTrue(fields.issubset(set(index_settings['index_defaults'])))

    @mark.ignore_during_cloud_tests
    def test_custom_settings(self):
        """adding custom settings to the index should be reflected in the returned output
        """
        model_properties = {'name': 'sentence-transformers/multi-qa-MiniLM-L6-cos-v1',
                            'dimensions': 384,
                            'tokens': 128,
                            'type': 'sbert'}

        index_settings = {
            'index_defaults': {
                'treat_urls_and_pointers_as_images': False,
                'model': 'test-model',
                'model_properties': model_properties,
                'normalize_embeddings': True,
            }
        }

        test_index_name = self.create_test_index(index_name=self.generic_test_index_name, settings_dict=index_settings)

        ix = self.client.index(test_index_name)
        index_settings = ix.get_settings()
        fields = {'treat_urls_and_pointers_as_images', 'text_preprocessing', 'model', 'normalize_embeddings',
                  'image_preprocessing', 'model_properties'}

        self.assertIn('index_defaults', index_settings)
        self.assertIn('number_of_shards', index_settings)
        self.assertIn("number_of_replicas", index_settings)
        self.assertTrue(fields.issubset(set(index_settings['index_defaults'])))

    def test_settings_should_be_type_dict(self):
        test_index_name = self.create_test_index(index_name=self.generic_test_index_name)

        ix = self.client.index(test_index_name)
        index_settings = ix.get_settings()

        self.assertIsInstance(index_settings, dict)
