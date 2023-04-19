
from marqo import config

from tests.marqo_test import MarqoTestCase


class TestConfig(MarqoTestCase):

    def setUp(self) -> None:
        self.endpoint = self.authorized_url

    def test_init_custom_devices(self):
        c = config.Config(url=self.endpoint,indexing_device="cuda:3", search_device="cuda:4")
        assert c.indexing_device == "cuda:3"
        assert c.search_device == "cuda:4"

    def test_url_is_s2search(self):
        c = config.Config(url="https://s2search.io/abdcde:8882")
        assert c.cluster_is_s2search

    def test_url_is_not_s2search(self):
        c = config.Config(url="https://som_random_cluster/abdcde:8882")
        assert not c.cluster_is_s2search
