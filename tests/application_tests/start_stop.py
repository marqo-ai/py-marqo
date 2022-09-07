import subprocess

from tests import marqo_test
from marqo import Client
from marqo.errors import MarqoApiError


class TestStartStop(marqo_test.MarqoTestCase):

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        self.index_name_1 = "my-test-index-1"
        try:
            self.client.delete_index(self.index_name_1)
        except MarqoApiError as s:
            pass

    def test_start_stop(self):
        d1 = {"Title": "The colour of plants", "_id": "fact_1"}
        d2 = {"Title": "some frogs", "_id": "fact_2"}
        self.client.index(self.index_name_1).add_documents(documents=[d1, d2])
        search_res_0 = self.client.index(self.index_name_1).search(q="General nature facts")
        assert (search_res_0["hits"][0]["_id"] == "fact_1") or (search_res_0["hits"][0]["_id"] == "fact_2")
        assert len(search_res_0) == 2

        stop_marqo_res = subprocess.run(["docker", "stop", "marqo"], check=True, capture_output=True)
        assert "marqo" in str(stop_marqo_res.stdout)

        try:
            self.client.index(self.index_name_1).search(q="General nature facts")
        except MarqoApiError as mqe:
            print("ERROR CAUGHT:")
            print(mqe)
            pass
        print("PASSED THE STOP ERROR")
        start_marqo_res = subprocess.run(["docker", "start", "marqo"], check=True, capture_output=True)
        assert "marqo" in str(start_marqo_res.stdout)
