import logging
import subprocess
import os
import time
from requests import HTTPError
from tests import marqo_test
from tests import utilities
from marqo import Client
from marqo.errors import MarqoApiError, BackendCommunicationError, MarqoWebError
import pprint
import json

class TestEnvVarChanges(marqo_test.MarqoTestCase):

    """
        All tests that rerun marqo with different env vars should go here
        Teardown will handle resetting marqo back to base settings
    """

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass
    
    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        # Ensures that marqo goes back to default state after these tests
        utilities.rerun_marqo_with_default_config(
            calling_class=cls.__name__
        )
        print("Marqo has been rerun with default env vars!")

    def test_max_replicas(self):
        # Default max is 1
        # Rerun marqo with new replica count
        max_replicas = 5
        print(f"Attempting to rerun marqo with max replicas: {max_replicas}")
        utilities.rerun_marqo_with_env_vars(
            env_vars = ["-e", f"MARQO_MAX_NUMBER_OF_REPLICAS={max_replicas}"],
            calling_class=self.__class__.__name__
        )

        # Attempt to create index with 4 replicas (should succeed)
        res_0 = self.client.create_index(index_name=self.index_name_1, settings_dict={
            "index_defaults": {
                "treat_urls_and_pointers_as_images": True,
                "model": "ViT-B/32",
            },
            "number_of_replicas": 4
        })

        # Make sure new index has 4 replicas
        assert self.client.get_index(self.index_name_1).get_settings() \
            ["number_of_replicas"] == 4
    

    def test_preload_models(self):
        # Default models are ["hf/all_datasets_v4_MiniLM-L6", "ViT-L/14"]
        # Rerun marqo with new custom model
        open_clip_model_object = {
            "model": "open-clip-1",
            "model_properties": {
                "name": "ViT-B-32-quickgelu",
                "dimensions": 512,
                "type": "open_clip",
                "url": "https://github.com/mlfoundations/open_clip/releases/download/v0.2-weights/vit_b_32-quickgelu-laion400m_avg-8a00ab3c.pt"
            }
        }

        print(f"Attempting to rerun marqo with custom model {open_clip_model_object['model']}")
        utilities.rerun_marqo_with_env_vars(
            env_vars = ['-e', f"MARQO_MODELS_TO_PRELOAD=[{json.dumps(open_clip_model_object)}]"],
            calling_class=self.__class__.__name__
        )

        # check preloaded models (should be custom model)
        custom_models = ["open-clip-1"]
        res = self.client.get_loaded_models()
        assert set([item["model_name"] for item in res["models"]]) == set(custom_models)


    def test_multiple_env_vars(self):
        """
            Ensures that rerun_marqo_with_env_vars can work with several different env vars
            at the same time

            3 things in the same command:
            1. Load models
            2. set max number of replicas
            3. set max EF
        """

        # Restart marqo with new max values
        max_replicas = 10
        max_ef = 6000
        new_models = ["hf/all_datasets_v4_MiniLM-L6"]
        utilities.rerun_marqo_with_env_vars(
            env_vars = [
                "-e", f"MARQO_MAX_NUMBER_OF_REPLICAS={max_replicas}",
                "-e", f"MARQO_EF_CONSTRUCTION_MAX_VALUE={max_ef}",
                "-e", f"MARQO_MODELS_TO_PRELOAD={json.dumps(new_models)}"
            ],
            calling_class=self.__class__.__name__
        )

        # Create index with same number of replicas and EF
        res_0 = self.client.create_index(index_name=self.index_name_1, settings_dict={
            "number_of_replicas": 4,         # should be fine now
            "index_defaults": {
                "ann_parameters" : {
                    "space_type": "cosinesimil",
                    "parameters": {
                        "ef_construction": 5000,    # should be fine now
                        "m": 16
                    }
                }
            }
        })

        # Assert correct replicas
        # Make sure new index has 4 replicas
        assert self.client.get_index(self.index_name_1).get_settings() \
            ["number_of_replicas"] == 4
        
        # Assert correct EF const
        assert self.client.get_index(self.index_name_1).get_settings() \
            ["index_defaults"]["ann_parameters"]["parameters"]["ef_construction"] == 5000

        # Assert correct models
        res = self.client.get_loaded_models()
        assert set([item["model_name"] for item in res["models"]]) == set(new_models)
