import unittest
from unittest import mock
from marqo import config
from marqo import enums
from marqo.client import Client
from marqo import utils
from tests.marqo_test import MarqoTestCase


class TestConfig(MarqoTestCase):

    def setUp(self) -> None:
        self.endpoint = self.authorized_url

    def test_url_is_s2search(self):
        c = config.Config(url="https://s2search.io/abdcde:8882")
        assert c.cluster_is_s2search

    def test_url_is_not_s2search(self):
        c = config.Config(url="https://som_random_cluster/abdcde:8882")
        assert not c.cluster_is_s2search

    def test_url_is_marqo(self):
        c = config.Config(url="https://api.marqo.ai")
        assert c.cluster_is_marqo

    def test_get_url_when_cluster_is_marqo_and_no_index_name_specified(self):
        c = config.Config(url="https://api.marqo.ai")
        assert c.get_url() == "https://api.marqo.ai/api"

    @mock.patch("requests.get")
    def test_get_url_when_cluster_is_marqo_and_index_name_specified(self, mock_get):
        mock_get.return_value.json.return_value = {"indices": [
            {"index_name": "index1", "load_balancer_dns_name": "example.com", "index_status": "READY"},
            {"index_name": "index2", "load_balancer_dns_name": "example2.com", "index_status": "READY"}
        ]}
        c = config.Config(url="https://api.marqo.ai")
        print(c.marqo_url_resolver._urls_mapping)
        assert c.get_url(index_name="index1") == "example.com"

    def test_get_url_when_cluster_is_not_marqo_and_index_name_specified(self):
        c = config.Config(url="https://s2search.io/abdcde:8882")
        assert c.get_url(index_name="index1") == "https://s2search.io/abdcde:8882"
