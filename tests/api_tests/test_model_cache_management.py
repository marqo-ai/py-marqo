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


class TestAddDocuments(MarqoTestCase):

    # NOTE: The cuda should already have model loaded in the startup
    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name = "test_index"
        self.MODEL = "ViT-L/14"


    def test_get_cuda_info(self) -> None:
        self.client.get_cuda_info()


    def test_get_cpu_info(self) -> None:
        self.client.get_cpu_info()


    def test_get_loaded_models(self) -> None:
        self.client.get_loaded_models()


    def test_eject_no_cached_model(self) -> None:
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
        self.client.index(self.index_name_1).add_documents([d1], device="cpu")
        self.client.index.eject_model(self.MODEL, "cpu")





