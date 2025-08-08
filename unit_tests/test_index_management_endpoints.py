from unittest.mock import patch, MagicMock

from marqo.config import Config
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.index import Index
from marqo.models.marqo_index import CollapseField
from unit_tests.marqo_unit_tests import MarqoUnitTests


class TestIndexCreationEndpoint(MarqoUnitTests):
    @classmethod
    def setUpClass(cls):
        """
        Set up the class by creating a test index.
        """
        cls.config = Config(
            instance_mappings=DefaultInstanceMappings(url="http://unit-tests-url:8882"),
        )

    @patch('marqo._httprequests.HttpRequests.send_request')
    def test_collapse_field_parameter(self, mock_send_request):
        mock_response = MagicMock()
        mock_send_request.return_value = mock_response

        Index.create(self.config, index_name='test_index',
                     collapse_fields=[
                         CollapseField(name='parent_id'),  # test default minGroups value is 500
                         CollapseField(name='color', minGroups=10),
                     ])

        self.assertEqual(1, mock_send_request.call_count)
        body = mock_send_request.call_args[0][2]
        self.assertListEqual([{'name': 'parent_id', 'minGroups': 500}, {'name': 'color', 'minGroups': 10}],
                             body["collapseFields"])
