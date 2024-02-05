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


class TestIndexManipulations(MarqoTestCase):
    def setUp(self):
        super().setUp()
        if not self.client.config.is_marqo_cloud:
            try:
                self.client.delete_index(self.generic_test_index_name)
            except MarqoError as e:
                pass

    # Create index tests
    @mark.fixed
    @mark.ignore_during_cloud_tests
    def test_create_index(self):
        self.client.create_index(self.generic_test_index_name)
        assert any(self.generic_test_index_name == ix["indexName"] for ix in  self.client.get_indexes()['results'])

    @mark.fixed
    # TODO: unmark when cloud responses are fixed
    @mark.ignore_during_cloud_tests
    def test_create_index_double(self):
        index_name = self.get_test_index_name(
            cloud_test_index_to_use=CloudTestIndex.unstructured_image,
            open_source_test_index_name=self.generic_test_index_name
        )
        if not self.client.config.is_marqo_cloud:
            self.client.create_index(index_name)
        assert any([index_name == ix["indexName"] for ix in  self.client.get_indexes()['results']])
        try:
            self.client.create_index(index_name)
        except MarqoWebError as e:
            assert e.code == "index_already_exists"

    @mark.fixed
    @mark.ignore_during_cloud_tests
    def test_create_index_hnsw(self):
        self.client.create_index(index_name=self.generic_test_index_name,
            settings_dict={
                    "annParameters": {
                        "spaceType": "dotproduct",
                        "parameters": {
                            "efConstruction": 128,
                            "m": 24
                        }
                    }
            }
        )
        assert self.client.get_index(self.generic_test_index_name).get_settings() \
                   ["annParameters"]["parameters"]["m"] == 24

        # Ensure non-specified values are in default
        assert self.client.get_index(self.generic_test_index_name).get_settings() \
                   ["annParameters"]["parameters"]["efConstruction"] == 128
        assert self.client.get_index(self.generic_test_index_name).get_settings() \
                   ["annParameters"]["spaceType"] == "dotproduct"

    # Delete index tests:

    @mark.ignore_during_cloud_tests
    @mark.fixed
    def test_create_delete_create_index(self):
        self.client.create_index(self.generic_test_index_name)
        self.client.delete_index(self.generic_test_index_name)
        self.client.create_index(self.generic_test_index_name)

    @mark.fixed
    def test_delete_index_response(self):
        mock_delete = mock.Mock()
        mock_delete.return_value = {'mock_delete_message': 'mock_delete_response'}

        @mock.patch("marqo._httprequests.HttpRequests.delete", mock_delete)
        def run():
            for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
                test_index_name = self.get_test_index_name(
                    cloud_test_index_to_use=cloud_test_index_to_use,
                    open_source_test_index_name=open_source_test_index_name
                )
                delete_response = self.client.delete_index(test_index_name, wait_for_readiness=False)
                assert delete_response == mock_delete.return_value
                return 2

        assert run() == 2

    # Get index tests:

    @mark.fixed
    def test_get_index(self):
        for cloud_test_index_to_use, open_source_test_index_name in self.test_cases:
            test_index_name = self.get_test_index_name(
                cloud_test_index_to_use=cloud_test_index_to_use,
                open_source_test_index_name=open_source_test_index_name
            )
            index = self.client.get_index(test_index_name)
            assert index.index_name == test_index_name

    @mark.fixed
    def test_get_index_non_existent(self):
        try:
            index = self.client.get_index("some-non-existent-index")
            raise AssertionError
        except MarqoError as e:
            assert e.code == "index_not_found_cloud"
        except MarqoWebError as e:
            assert e.code == "index_not_found"