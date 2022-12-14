import copy
import math
import pprint
import random
import requests
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase
from marqo import enums
from unittest import mock


class TestModelCacheManagement(MarqoTestCase):

    # NOTE: The cuda should already have model loaded in the startup
    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name = "test_index"
        self.MODEL = "ViT-B/32"
        try:
            self.client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass


    def tearDown(self) -> None:
        try:
            self.client.delete_index(self.index_name)
        except MarqoApiError as s:
            pass


    def test_get_cuda_info(self) -> None:
        try:
            res = self.client.get_cuda_info()
            if "cuda_devices" not in res:
                raise AssertionError
        # catch error if no cuda device in marqo
        except MarqoWebError:
            pass


    def test_get_cpu_info(self) -> None:
        res = self.client.get_cpu_info()

        if "cpu_usage_percent" not in res:
            raise AssertionError

        if "memory_used_percent" not in res:
            raise AssertionError

        if "memory_used_gb" not in res:
            raise AssertionError


    def test_get_loaded_models(self) -> None:
        res = self.client.get_loaded_models()

        if "models" not in res:
            raise AssertionError


    def test_eject_no_cached_model(self) -> None:
        # test a model that is not cached
        try:
            res = self.client.eject_model("void_model", "void_device")
            raise AssertionError
        except MarqoWebError:
            pass


    def test_eject_model(self) -> None:
        settings = {"model": self.MODEL}

        self.client.create_index(index_name=self.index_name, **settings)
        d1 = {
            "doc title": "Cool Document 1",
            "field 1": "some extra info"
        }
        self.client.index(self.index_name).add_documents([d1], device="cpu")
        res = self.client.eject_model(self.MODEL, "cpu")
        assert res["result"] == "success"
        assert res["message"].startswith("successfully eject")



