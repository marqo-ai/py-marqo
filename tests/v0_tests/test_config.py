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
