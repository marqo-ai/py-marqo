import asyncio
import pprint
import random
import time
import threading
from tests import marqo_test
from marqo import Client
from marqo.errors import MarqoApiError
import requests
import logging
import sys
sys.setswitchinterval(0.005)


class TestAsync (marqo_test.MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def test_async(self):
        num_docs = 500

        vocab_source = "https://www.mit.edu/~ecprice/wordlist.10000"

        vocab = requests.get(vocab_source).text.splitlines()

        d1 = {
            "doc title": "Just Your Average Doc",
            "field X": "this is a solid doc",
            "_id": "56"
        }
        self.client.index(self.index_name_1).add_documents([d1])
        assert self.client.index(self.index_name_1).get_stats()['numberOfDocuments'] == 1


        def significant_ingestion():
            docs = [{"Title": " ".join(random.choices(population=vocab, k=10)),
                          "Description": " ".join(random.choices(population=vocab, k=25)),
                          } for _ in range(num_docs)]
            self.client.index(self.index_name_1).add_documents(
                auto_refresh=True, documents=docs, server_batch_size=1, processes=1)

        cache_update_thread = threading.Thread(
            target=significant_ingestion)
        cache_update_thread.start()
        time.sleep(3)
        refresh_res = self.client.index(self.index_name_1).refresh()
        time.sleep(0.5)
        assert cache_update_thread.is_alive()
        assert self.client.index(self.index_name_1).get_stats()['numberOfDocuments'] > 1
        assert cache_update_thread.is_alive()
        assert self.client.index(self.index_name_1).get_stats()['numberOfDocuments'] < 251

        while cache_update_thread.is_alive():
            time.sleep(1)

        self.client.index(self.index_name_1).refresh()
        assert self.client.index(self.index_name_1).get_stats()['numberOfDocuments'] == 501


