"""Please have a running Marqo instance to test against!


Pass its settings to local_marqo_settings.
"""
import unittest
from marqo.utils import construct_authorized_url


class MarqoTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        local_marqo_settings = {
            "url": 'http://localhost:8882'
        }
        cls.client_settings = local_marqo_settings
        cls.authorized_url = cls.client_settings["url"]
