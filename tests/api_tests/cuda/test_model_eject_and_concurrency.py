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
import multiprocessing
import time

@classwide_decorate(allow_environments, allowed_configurations=["CUDA_DIND_MARQO_OS"])
class TestModelEjectAndConcurrency(MarqoTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.client = Client(**cls.client_settings)
        cls.index_model_object = {
            "test_0": 'open_clip/ViT-B-32/laion400m_e31',
            "test_1": 'open_clip/ViT-B-32/laion400m_e32',
            "test_2": 'open_clip/RN50x4/openai',
            "test_3": 'onnx16/open_clip/RN50-quickgelu/openai',
            "test_4": "onnx16/open_clip/ViT-L-14/laion2b_s32b_b82k",
            "test_5": "onnx32/open_clip/ViT-L-14/openai",
            "test_6": "hf/all-MiniLM-L6-v1",
            "test_7": "hf/all-MiniLM-L6-v2",
            "test_8": "hf/all_datasets_v3_MiniLM-L12",
            "test_9": 'open_clip/ViT-B-32/laion2b_e16',
            "test_10": 'ViT-B/16',
            "test_11": 'ViT-L/14@336px',
            "test_12": "onnx16/openai/ViT-L/14",
            "test_13": 'onnx32/open_clip/ViT-B-32-quickgelu/laion400m_e32',
        }

        for index_name, model in cls.index_model_object.items():
            settings = {
                "model": model
            }
            try:
                cls.client.delete_index(index_name)
            except:
                pass

            cls.client.create_index(index_name, **settings)

            cls.client.index(index_name).add_documents([
                {
                    "Title": "The Travels of Marco Polo",
                    "Description": "A 13th-century travelogue describing Polo's travels"
                },
                {
                    "Title": "Extravehicular Mobility Unit (EMU)",
                    "Description": "The EMU is a spacesuit that provides environmental protection, "
                                   "mobility, life support, and communications for astronauts",
                    "_id": "article_591"
                }], device = "cuda",
            )

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)

    def tearDown(self) -> None:
        pass

    def normal_search(self, index_name, q):
        # A function will be called in multiprocess
        res = self.client.index(index_name).search("what is best to wear on the moon?", device = "cuda")
        if len(res["hits"]) != 2:
            q.put(AssertionError)

    def racing_search(self, index_name, q):
        # A function will be called in multiprocess
        try:
            res = self.client.index(index_name).search("what is best to wear on the moon?", device = "cuda")
            q.put(AssertionError)
        except MarqoWebError as e:
            if not "another request was updating the model cache at the same time" in e.message:
                q.put(e)
            pass

    def test_sequentially_search(self):
        time.sleep(5)
        for index_name in list(self.index_model_object):
            self.client.index(index_name).search(q='What is the best outfit to wear on the moon?',device = "cuda")

    def test_concurrent_search_with_cache(self):
        # Search once to make sure the model is in cache
        test_index = "test_1"
        res = self.client.index(test_index).search("what is best to wear on the moon?",device = "cuda")

        q = multiprocessing.Queue()
        processes = []
        for i in range(5):
            p = multiprocessing.Process(target=self.normal_search, args=(test_index, q))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        assert q.empty()

    def test_concurrent_search_without_cache(self):
        # Remove all the cached models
        super().removeAllModels()

        test_index = "test_3"
        q = multiprocessing.Queue()
        processes = []
        p = multiprocessing.Process(target=self.normal_search, args=(test_index, q))
        processes.append(p)
        p.start()

        for i in range(5):
            p = multiprocessing.Process(target=self.racing_search, args=(test_index, q))
            processes.append(p)
            p.start()

        assert q.empty()