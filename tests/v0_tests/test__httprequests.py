import unittest
from marqo._httprequests import HttpRequests
from marqo.config import Config
from marqo.default_instance_mappings import DefaultInstanceMappings

class TestConstructPath(unittest.TestCase):

    def setUp(self):
        self.base_url = "http://localhost:8882"

    def construct_path_helper(self, path, use_telemetry=None):
        r = HttpRequests(
            config=Config(use_telemetry=use_telemetry, instance_mappings=DefaultInstanceMappings(self.base_url))
        )
        return r._construct_path(path)

    def test_construct_path_with_telemetry_enabled(self):
        result = self.construct_path_helper("testpath", True)
        self.assertEqual(result, f"{self.base_url}/testpath?telemetry=True")

    def test_construct_path_with_query_string_and_telemetry_enabled(self):
        result = self.construct_path_helper("testpath?param=value", True)
        self.assertEqual(result, f"{self.base_url}/testpath?param=value&telemetry=True")

    def test_construct_path_with_telemetry_disabled(self):
        result = self.construct_path_helper("testpath", False)
        self.assertEqual(result, f"{self.base_url}/testpath")

    def test_construct_path_with_no_telemetry_parameter(self):
        result = self.construct_path_helper("testpath")
        self.assertEqual(result, f"{self.base_url}/testpath")
