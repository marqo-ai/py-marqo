import logging
import subprocess
import time

from tests import marqo_test
from marqo import Client
from marqo.errors import MarqoApiError, BackendCommunicationError


class TestStartStop(marqo_test.MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def test_start_stop(self):

        NUMBER_OF_RESTARTS = 6

        def run_start_stop(restart_number: int):
            # 1 retry a second...
            NUMBER_OF_TRIES = 400

            d1 = {"Title": "The colour of plants", "_id": "fact_1"}
            d2 = {"Title": "some frogs", "_id": "fact_2"}
            self.client.index(self.index_name_1).add_documents(documents=[d1, d2])
            search_res_0 = self.client.index(self.index_name_1).search(q="General nature facts")
            assert (search_res_0["hits"][0]["_id"] == "fact_1") or (search_res_0["hits"][0]["_id"] == "fact_2")
            assert len(search_res_0["hits"]) == 2

            stop_marqo_res = subprocess.run(["docker", "stop", "marqo"], check=True, capture_output=True)
            assert "marqo" in str(stop_marqo_res.stdout)

            try:
                self.client.index(self.index_name_1).search(q="General nature facts")
                raise AssertionError("Marqo is still accessible despite docker stopping!")
            except BackendCommunicationError as mqe:
                pass

            start_marqo_res = subprocess.run(["docker", "start", "marqo"], check=True, capture_output=True)
            assert "marqo" in str(start_marqo_res.stdout)

            for i in range(NUMBER_OF_TRIES):
                try:
                    self.client.index(self.index_name_1).search(q="General nature facts")
                    break
                except BackendCommunicationError as mqe:
                    if "exceeds your S2Search free tier limit" in str(mqe):
                        raise mqe
                    if i + 1 >= NUMBER_OF_TRIES:
                        raise AssertionError(f"Timeout waiting for Marqo to restart! Restart number {b}")
                    time.sleep(1)

            search_res_1 = self.client.index(self.index_name_1).search(q="General nature facts")
            assert search_res_1["hits"] == search_res_0["hits"]
            assert (search_res_1["hits"][0]["_id"] == "fact_1") or (search_res_1["hits"][0]["_id"] == "fact_2")
            return True

        counter = 0
        for b in range(NUMBER_OF_RESTARTS):
            counter += 1
            assert run_start_stop(restart_number=b)
        assert counter == NUMBER_OF_RESTARTS
