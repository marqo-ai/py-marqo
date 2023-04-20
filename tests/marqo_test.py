"""Please have a running Marqo instance to test against!

Pass its settings to local_marqo_settings.
"""
import unittest
from marqo.utils import construct_authorized_url
from marqo import Client
from marqo.errors import MarqoWebError


class MarqoTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        local_marqo_settings = {
            "url": 'http://localhost:8882'
        }
        cls.client_settings = local_marqo_settings
        cls.authorized_url = cls.client_settings["url"]

    @classmethod
    def tearDownClass(cls) -> None:
        # A function that will be automatically called after each test call
        # This removes all the loaded models to save memory space.
        cls.removeAllModels()

    @classmethod
    def removeAllModels(cls) -> None:
        # A function that can be called to remove loaded models in Marqo.
        # Use it whenever you think there is a risk of OOM problem.
        # E.g., add it into the `tearDown` function to remove models between test cases.
        client = Client(**cls.client_settings)
        model_list = client.get_loaded_models().get("models", [])
        for model in model_list:
            try:
                client.eject_model(model_name=model["model_name"], model_device=model["model_device"])
            except MarqoWebError:
                pass

