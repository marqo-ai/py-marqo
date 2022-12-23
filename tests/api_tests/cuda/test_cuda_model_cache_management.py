import copy
import pprint
from marqo.client import Client
from marqo.errors import MarqoApiError, MarqoError, MarqoWebError
import unittest
from tests.marqo_test import MarqoTestCase
from marqo import enums
from unittest import mock
from tests.utilities import allow_environments
from tests.utilities import classwide_decorate

@classwide_decorate(allow_environments, allowed_configurations=["CUDA_DIND_MARQO_OS"])
class TestAddDocuments(MarqoTestCase):

    # NOTE: The cuda should already have model loaded in the startup
    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name = "test_index"
        self.MODEL = "ViT-B/32"


    def test_get_cuda_info(self) -> None:
        try:
            res = self.client.get_cuda_info()
            if "cuda_devices" not in res:
                raise AssertionError
        except MarqoWebError: # catch error if no cuda device in marqo
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
        # test eject a model that is not cached
        try:
            self.client.eject_model("void_model", "void_device")
        except MarqoWebError:
            pass

    def test_eject_model(self) -> None:
        settings = {"model" : self.MODEL}

        self.client.create_index(index_name=self.index_name_1, **settings)
        d1 = {
            "doc title": "Cool Document 1",
            "field 1": "some extra info"
        }
        self.client.index(self.index_name_1).add_documents([d1], device="cuda")
        self.client.index.eject_model(self.MODEL, "cuda")





