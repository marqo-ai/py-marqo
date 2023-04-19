"""Please have a running Marqo instance to test against!


Pass its settings to local_marqo_settings.
"""
import os
import unittest

from marqo.client import Client
from marqo.version import __marqo_version__ as py_marqo_support_version


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
            print(f"WARNING: supported Py-marqo version and Marqo versions aren't the same!\n {marqo_server_version} != {py_marqo_support_version}")
            print(f"MARQO SERVER VERSION -> {marqo_server_version}")
            print(f"PY-MARQO SUPPORTED VERSION -> {py_marqo_support_version}")



    def warm_request(self, func, *args, **kwargs):
        '''
        Takes in a function object, func, and executes the function 5 times to warm search results.
        Any arguments passed to args and kwargs are passed as arguments to the function.
        This solves the occurence of tests failing due to eventual consistency implemented in marqo cloud.
        '''
        for i in range(5):
            func(*args, **kwargs) 
