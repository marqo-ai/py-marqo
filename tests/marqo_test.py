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
    "url": 'https://cff00aeacd.api.s2search.io', 'api_key': '8lQkoEGRBW2UX2JmSI6ZK8velGqiv9767DYSUjMc'
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
