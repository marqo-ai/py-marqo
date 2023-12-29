import copy
import functools
import math
import pprint
import random
import pytest
import requests
import time

from pytest import mark

from marqo.errors import MarqoError, MarqoWebError
from tests.marqo_test import MarqoTestCase, CloudTestIndex
from marqo import enums
from unittest import mock


class TestAddDocuments(MarqoTestCase):
    # Create index tests
    def test_create_index(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )

    def test_create_index_double(self):
        if not self.client.config.is_marqo_cloud:
            self.create_test_index(
                cloud_test_index_to_use=CloudTestIndex.basic_index,
                open_source_test_index_name=self.generic_test_index_name,
            )
        try:
            self.create_test_index(
                cloud_test_index_to_use=CloudTestIndex.basic_index,
                open_source_test_index_name=self.generic_test_index_name,
            )
        except MarqoError as e:
            assert e.code == "index_already_exists_cloud"
        except MarqoWebError as e:
            assert e.code == "index_already_exists"

    def test_create_index_hnsw(self):
        if not self.client.config.is_marqo_cloud:
            self.client.delete_index(self.generic_test_index_name)
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.image_index,
            open_source_test_index_name=self.generic_test_index_name,
            open_source_index_settings_dict={
                "index_defaults": {
                    "ann_parameters": {
                        "parameters": {
                            "m": 24
                        }
                    }
                }
            }
        )
        assert self.client.get_index(test_index_name).get_settings() \
                   ["index_defaults"]["ann_parameters"]["parameters"]["m"] == 24

        # Ensure non-specified values are in default
        assert self.client.get_index(test_index_name).get_settings() \
                   ["index_defaults"]["ann_parameters"]["parameters"]["ef_construction"] == 128
        assert self.client.get_index(test_index_name).get_settings() \
                   ["index_defaults"]["ann_parameters"]["space_type"] == "cosinesimil"

    # Delete index tests:

    @mark.ignore_during_cloud_tests
    def test_delete_index(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        self.client.delete_index(test_index_name)
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )

    def test_delete_index_response(self):
        mock_delete = mock.Mock()
        mock_delete.return_value = {'mock_delete_message': 'mock_delete_response'}

        @mock.patch("marqo._httprequests.HttpRequests.delete", mock_delete)
        def run():
            test_index_name = self.create_test_index(
                cloud_test_index_to_use=CloudTestIndex.basic_index,
                open_source_test_index_name=self.generic_test_index_name,
            )
            delete_response = self.client.delete_index(test_index_name, wait_for_readiness=False)
            assert delete_response == mock_delete.return_value
            return 2

        assert run() == 2

    # Get index tests:

    def test_get_index(self):
        test_index_name = self.create_test_index(
            cloud_test_index_to_use=CloudTestIndex.basic_index,
            open_source_test_index_name=self.generic_test_index_name,
        )
        index = self.client.get_index(test_index_name)
        assert index.index_name == test_index_name

    def test_get_index_non_existent(self):
        try:
            index = self.client.get_index("some-non-existent-index")
            raise AssertionError
        except MarqoError as e:
            assert e.code == "index_not_found_cloud"
        except MarqoWebError as e:
            assert e.code == "index_not_found"