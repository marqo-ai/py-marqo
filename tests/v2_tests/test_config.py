from unittest import mock
from marqo import config
from tests.marqo_test import MarqoTestCase
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from marqo.default_instance_mappings import DefaultInstanceMappings


class TestConfig(MarqoTestCase):

    def setUp(self) -> None:
        self.endpoint = self.authorized_url

    def tearDown(self) -> None:
        pass

    def test_url_is_marqo(self):
        c = config.Config(MarqoCloudInstanceMappings("https://api.marqo.ai"), is_marqo_cloud=True)
        assert c.is_marqo_cloud

    def test_get_url_when_cluster_is_marqo_and_no_index_name_specified(self):
        c = config.Config(instance_mappings=MarqoCloudInstanceMappings("https://api.marqo.ai"))
        assert c.instance_mapping.get_control_base_url() == "https://api.marqo.ai/api"

    @mock.patch("requests.get")
    def test_get_url_when_cluster_is_marqo_and_index_name_specified(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        c = config.Config(instance_mappings=MarqoCloudInstanceMappings("https://api.marqo.ai"))
        assert c.instance_mapping.get_index_base_url(index_name="index1") == "example.com"

    def test_get_url_when_cluster_is_not_marqo_and_index_name_specified(self):
        c = config.Config(instance_mappings=DefaultInstanceMappings("https://s2search.io/abdcde:8882"))
        assert c.instance_mapping.get_index_base_url(index_name="index1") == "https://s2search.io/abdcde:8882"
