"""Please have a running Marqo instance to test against!


Pass its settings to local_marqo_settings.
"""
import unittest
from marqo.utils import construct_authorized_url
from marqo.version import __marqo_version__ as py_marqo_support_version
from marqo.client import Client
import os
import time

class MarqoTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        local_marqo_settings = {
            "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
        }

        api_key = os.environ.get("MARQO_API_KEY", None)
        if (api_key):
            local_marqo_settings["api_key"] = api_key

        cls.client_settings = local_marqo_settings
        cls.authorized_url = cls.client_settings["url"]

        # class property to indicate if test is being run on multi
        cls.IS_MULTI_INSTANCE = (True if os.environ.get("IS_MULTI_INSTANCE", False) in ["True", "TRUE", "true", True] else False)

        marqo_server_version = Client(**cls.client_settings).get_marqo()["version"]
        if marqo_server_version != py_marqo_support_version:
            print(f"WARNING: supported py Marqo version and Marqo versions aren't the same!\n {marqo_server_version} != {py_marqo_support_version}")
            print(f"MARQO SERVER VERSION -> {marqo_server_version}")
            print(f"PY-MARQO SUPPORTED VERSION -> {py_marqo_support_version}")



    def warm_request(f) -> None:
        for i in range(5):
            f
