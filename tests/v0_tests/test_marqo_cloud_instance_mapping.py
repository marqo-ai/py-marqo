import time
from unittest import mock
from unittest.mock import patch, MagicMock
from marqo.enums import IndexStatus
from marqo.index import marqo_url_and_version_cache
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from tests.marqo_test import MarqoTestCase
from marqo.errors import MarqoCloudIndexNotFoundError, MarqoCloudIndexNotReadyError, MarqoWebError, \
    BackendCommunicationError


class TestMarqoCloudInstanceMappings(MarqoTestCase):
    @patch("requests.get")
    def test_refresh_urls_if_needed(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai",api_key="your-api-key", url_cache_duration=60
        )
        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp

        # Wait some time to see that timestamp is updated and it is higher than initial one after refresh
        time.sleep(0.1)

        mapping._refresh_urls_if_needed("index1")

        # Check that the timestamp has been updated
        assert mapping.latest_index_mappings_refresh_timestamp > initial_timestamp

        # Check that the URLs mapping has been refreshed
        assert mapping._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_urls_if_needed_index_exists(self, mock_get: MagicMock):
        """
        Test that if index is already in cache, it is not refreshed.
        """
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=0
        )
        # Call refresh_urls_if_needed without waiting
        mapping._refresh_urls_if_needed("index1")
        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp
        time.sleep(0.1)
        # Since index is loaded in cache, it should not be refreshed and timestamp should not be updated
        mapping._refresh_urls_if_needed("index2")

        # Check that the timestamp has not been updated
        assert mapping.latest_index_mappings_refresh_timestamp == initial_timestamp
        mock_get.assert_called_once()

        # Check that the URLs mapping has been initially populated
        assert mapping._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_urls_if_needed_cache_duration_not_passed(self, mock_get: MagicMock):
        """
        Test that if cache duration has not passed, it is not refreshed.
        """
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        # Call refresh_urls_if_needed without waiting
        mapping._refresh_urls_if_needed("index1")
        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp
        time.sleep(0.1)
        # Since cache duration has not passed, it should not be refreshed and timestamp should not be updated
        mapping._refresh_urls_if_needed("index3")

        # Check that the timestamp has not been updated
        assert mapping.latest_index_mappings_refresh_timestamp == initial_timestamp
        mock_get.assert_called_once()

        # Check that the URLs mapping has been initially populated
        assert mapping._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_urls_if_needed_no_index(self, mock_get: MagicMock):
        """
        Test that if no index is passed, cache refresh is only time based.
        """
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=1
        )
        mapping._refresh_urls_if_needed("index1")
        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp
        time.sleep(0.1)
        # Since cache duration has not passed, it should not be refreshed and timestamp should not be updated
        mapping._refresh_urls_if_needed()

        # Check that the timestamp has not been updated
        assert mapping.latest_index_mappings_refresh_timestamp == initial_timestamp
        mock_get.assert_called_once()

        initial_timestamp = mapping.latest_index_mappings_refresh_timestamp
        time.sleep(1)
        # Since cache duration has passed, cache should refresh
        mapping._refresh_urls_if_needed()

        # Check that the timestamp has been updated
        assert mapping.latest_index_mappings_refresh_timestamp > initial_timestamp
        self.assertEqual(mock_get.call_count, 2)

        # Check that the URLs mapping has been initially populated
        assert mapping._urls_mapping["READY"] == {
            "index1": "example.com",
            "index2": "example2.com",
        }

    @patch("requests.get")
    def test_refresh_includes_only_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "NOT READY"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        # Access the urls_mapping property
        mapping._refresh_urls_if_needed("index1")
        urls_mapping = mapping._urls_mapping

        # Check that the URLs mapping has been initially populated
        assert urls_mapping["READY"] == {
            "index1": "example.com",
        }

    def test_refresh_urls_graceful_timeout_handling(self):
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        # use ridiculously low timeout
        with self.assertLogs('marqo', level='WARNING') as cm:
            mapping._refresh_urls(timeout=0.0000000001)
            assert "timeout" in cm.output[0].lower()
            assert "marqo cloud indexes" in cm.output[0].lower()

    @patch("requests.get")
    def test_refresh_urls_graceful_timeout_handling_http_timeout(self, mock_get):
        from requests.exceptions import Timeout
        mock_get.side_effect = Timeout
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertLogs('marqo', level='WARNING') as cm:
            mapping._refresh_urls(timeout=5)
            assert "timeout" in cm.output[0].lower()
            assert "marqo cloud indexes" in cm.output[0].lower()

    @patch("requests.get")
    def test_refresh_urls_non_ok_response(self, mock_get):
        """None is return and text content is logged as a warning"""
        expected_message = "some HTTP error"
        mock_get.return_value.ok = False
        mock_get.return_value.text = expected_message
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertLogs('marqo', level='WARNING') as cm:
            assert mapping._refresh_urls(timeout=5) is None
            assert expected_message in cm.output[0]

    @patch("requests.get")
    def test_ok_to_get_index_before_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "CREATING"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        assert 'example2.com' == mapping.get_index_base_url("index2")

    @patch("requests.get")
    def test_modifying_state_returns_as_ready(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "MODIFYING"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        assert mapping.get_index_base_url("index2") == "example2.com"

    @patch("requests.get")
    def test_request_of_not_existing_index_raises_error(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index2")

    @patch("requests.get")
    def test_get_indexes_fails_cache_doesnt_update(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "MODIFYING"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=0.1
        )
        assert mapping.get_index_base_url("index1") == "example.com"
        with patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning") as mock_warning:
            # simulate that the cache has expired
            mapping.latest_index_mappings_refresh_timestamp = mapping.latest_index_mappings_refresh_timestamp - 20

            # when mapping.index_http_error_handler triggers an update, we should gracefully
            # handle the error
            mock_get.return_value.ok = False
            mock_get.return_value.text = "Some Internal Server Error Message"
            mock_get.return_value.json.return_value = {"status_code": 500}
            mapping.index_http_error_handler('index2')

            assert mapping.get_index_base_url("index1") == "example.com"
            mock_warning.assert_called_once()
            assert str(mock_warning.call_args[0][0]) == mock_get.return_value.text
            # Ensure cache has maintained the old value
            assert mapping.get_index_base_url("index2") == "example2.com"

    @patch("requests.get")
    def test_get_indexes_fails_cache_updates(self, mock_get):
        mock_get.return_value.json.return_value = {"status_code": 500}
        mock_get.return_value.ok = False
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=0.1
        )
        with patch("marqo.marqo_cloud_instance_mappings.mq_logger.warning") as mock_warning:
            mapping.latest_index_mappings_refresh_timestamp = time.time() - 366
            with self.assertRaises(MarqoCloudIndexNotFoundError):
                mapping.get_index_base_url("index1")
            mock_warning.assert_called_once()
        mapping.latest_index_mappings_refresh_timestamp = time.time() - mapping.url_cache_duration - 1
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
        ]}
        assert mapping.get_index_base_url("index1") == "example.com"

    @patch("requests.get")
    def test_deleting_status_raises_error(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "DELETING"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index2")

    @patch("requests.get")
    def test_deleted_status_raises_error(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "DELETED"}
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index2")

    def test_only_1_http_request_sent_for_search(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")
        test_index_name = self.create_test_index(self.generic_test_index_name)

        # pop the index_name to force a refresh
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp = time.time() - 366
        self.client.config.instance_mapping._urls_mapping["READY"].pop(test_index_name, '')

        with patch("marqo._httprequests.HttpRequests.post") as mock_post, \
                patch("requests.get") as mock_get:
            # 1 for the initial refresh, 1 for the search
            self.client.index(test_index_name).search("test")
            assert mock_post.call_count == 1
            assert mock_get.call_count == 1

            # increased for search, didn't change for refresh
            self.client.index(test_index_name).search("test")
            assert mock_post.call_count == 2
            assert mock_get.call_count == 1

    def test_deleted_index_created_again(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")

        test_index_name = self.create_test_index(self.generic_test_index_name)
        bad_url = 'https://dummy-url-e0244394-4383-4869-b633-46e6fe4a3ac1.dp1.marqo.ai'

        # trigger the version check, if needed, to make this fair between individual runs and
        # running the entire test suite
        self.client.index(test_index_name)
        marqo_url_and_version_cache[bad_url] = '_skipped'

        # set time to now to prevent the mappings from refreshing prematurely
        mappings = self.client.config.instance_mapping
        mappings.latest_index_mappings_refresh_timestamp = time.time()
        mappings._urls_mapping[IndexStatus.READY][test_index_name] = bad_url


        with mock.patch('marqo.index.Index._marqo_minimum_supported_version_check'):
            with self.assertRaises(BackendCommunicationError):
                # attempts to use the bad_url for searching and raises a connection error
                # but does not refresh the cache yet, because we haven't yet hit the minimum
                # refresh duration
                self.client.index(test_index_name).search('test query')

        # note that the troublesome URL is NOT evicted from mappings
        assert (mappings._urls_mapping[IndexStatus.READY][test_index_name] == bad_url)

        # ... another client deletes the index here ...

        # set time to the past to force a mappings refresh on the next error
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp = 0

        # instantiating a client should not trigger a mappings refresh (as the version cache
        # still has the bad_url)
        ix = self.client.index(test_index_name)

        with self.assertRaises(BackendCommunicationError):
            # error is raised on search, which kicks off the mappings refresh
            # We can actually refresh this time as the minimum refresh duration has passed
            ix.search('test query')

        # now we can search with the correct URL
        ix.search('test query')

        assert len(mappings._urls_mapping[IndexStatus.READY][test_index_name]) > 0
        assert (mappings._urls_mapping[IndexStatus.READY][test_index_name] != bad_url)

    def test_when_needed_http_request_for_get_indexes_is_sent(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")
        test_index_name = self.create_test_index(self.generic_test_index_name)

        from marqo._httprequests import HttpRequests as HttpReq2
        # these assignments allow HttpRequests.post to used while also being mocked,
        # while preventing infinite recursion:
        h = HttpReq2(config=self.client.config)
        v = h.post

        def pass_through_post(*args, **kwargs):
            return v(*args, **kwargs)

        # pop the index_name to force a refresh
        # Ensure the mappings are ready:
        self.client.index(test_index_name).search("test")
        simulated_last_reset_time = time.time() - 366
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp =  simulated_last_reset_time
        # 1 for the initial refresh, 1 for the search
        mock_post = mock.MagicMock()
        mock_get = mock.MagicMock()
        mock_post.side_effect = pass_through_post

        @mock.patch("marqo._httprequests.HttpRequests.post", mock_post)
        @mock.patch("requests.get", mock_get)
        def run():

            self.client.index(test_index_name).search("test")
            assert mock_post.call_count == 1
            # no need to refresh, as there is no error, even if the refresh duration has passed:
            assert mock_get.call_count == 0

            assert (simulated_last_reset_time
                    == self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp)

            # trigger a communication error on the next search:
            not_real_url = 'https://dummy-url-e0244394-4383-4869-b633-46e6fe4a3ac1.dp1.marqo.ai'
            # to prevent us accidentally prematurely refreshing mappings, we
            #  need to add the dummy url to the version cache:
            marqo_url_and_version_cache[not_real_url] = '_skipped'
            self.client.config.instance_mapping._urls_mapping["READY"][test_index_name] = not_real_url

            with self.assertRaises(BackendCommunicationError):
                ix2 = self.client.index(test_index_name)
                ix2.search("test")
            assert mock_post.call_count == 2
            # mappings refresh is triggered due to the error:
            assert mock_get.call_count == 1
            return 2
        assert run() == 2

    @patch("requests.get")
    def test_transitioning_flow(self, mock_get):
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=1
        )
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index1")

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "CREATING"},
        ]}
        mapping.latest_index_mappings_refresh_timestamp = time.time() - 2
        assert 'example.com' == mapping.get_index_base_url("index1")

        # index is ready but cache is not expired
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
        ]}
        assert 'example.com' == mapping.get_index_base_url("index1")

        mapping.latest_index_mappings_refresh_timestamp = time.time() - 2
        assert mapping.get_index_base_url("index1") == "example.com"

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "MODIFYING"},
        ]}
        mapping.latest_index_mappings_refresh_timestamp = time.time() - 366

        assert mapping.get_index_base_url("index1") == "example.com"

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "DELETING"},
        ]}

        # cache has not expired, url is still returned
        assert mapping.get_index_base_url("index1") == "example.com"

        # Trigger cache eviction for this index
        mapping.index_http_error_handler("index1")
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index1")

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "DELETED"},
        ]}
        mapping.latest_index_mappings_refresh_timestamp = time.time() - 366
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index1")

        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index-unknown")

    @patch("requests.get")
    def test_transitioning_flow_without_modifying(self, mock_get):
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=1
        )
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index1")

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "CREATING"},
        ]}
        mapping.latest_index_mappings_refresh_timestamp = time.time() - 2

        assert "example.com" == mapping.get_index_base_url("index1")

        # index is ready but cache is not expired
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "READY"},
        ]}
        assert "example.com" == mapping.get_index_base_url("index1")

        mapping.latest_index_mappings_refresh_timestamp = time.time() - 2
        assert mapping.get_index_base_url("index1") == "example.com"

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "DELETING"},
        ]}

        # cache has not expired, url is still returned
        assert mapping.get_index_base_url("index1") == "example.com"

        # Trigger cache refresh, wait 1 second to ensure refresh isn't skipped
        time.sleep(1)
        mapping.index_http_error_handler("index1")
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index1")

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "DELETED"},
        ]}
        mapping.latest_index_mappings_refresh_timestamp = time.time() - 366
        with self.assertRaises(MarqoCloudIndexNotFoundError):
            mapping.get_index_base_url("index1")

    def test_index_instantiation_does_not_refresh_urls(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")
        test_index_name = self.create_test_index(self.generic_test_index_name)

        start_time = time.time()
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp -= 361
        time.sleep(0.1)
        idx = self.client.index(test_index_name)
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp < start_time

        time.sleep(0.1)
        start_time = time.time()
        idx = self.client.index(test_index_name)
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp < start_time

    def test_search_call_does_not_refresh_urls_when_not_needed(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")
        test_index_name = self.create_test_index(self.generic_test_index_name)

        time_now = time.time()
        self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp -= 361
        idx = self.client.index(test_index_name)
        self.client.config.instance_mapping.index_http_error_handler(test_index_name)
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp > time_now

        last_refresh = self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp
        idx.search("test")
        assert self.client.config.instance_mapping.latest_index_mappings_refresh_timestamp == last_refresh

    @patch("requests.get")
    def test_index_http_error_handler(self, mock_get):
        mappings = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key"
        )
        mappings._urls_mapping[IndexStatus.READY]['index1'] = "example.com"
        mappings._urls_mapping[IndexStatus.READY]['index2'] = "example.com"
        mappings._urls_mapping[IndexStatus.CREATING]['index1'] = "example.com"

        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index2", "endpoint": "example.com", "index_status": "READY"},
            {"index_name": "index3", "endpoint": "example.com", "index_status": "CREATING"},
        ]}

        mappings.index_http_error_handler('index1')

        self.assertEqual(mappings._urls_mapping,
                         {IndexStatus.READY: {'index2': 'example.com'},
                          IndexStatus.CREATING: {'index3': 'example.com'}}
                         )

    def test_is_index_usage_allowed(self):
        if not self.client.config.is_marqo_cloud:
            self.skipTest("Test is not relevant for non-Marqo Cloud instances")

        test_index_name = self.create_test_index(self.generic_test_index_name)
        assert self.client.config.instance_mapping.is_index_usage_allowed(test_index_name)

        assert not self.client.config.instance_mapping.is_index_usage_allowed("not-existing-index")

    @patch("requests.get")
    def test_is_index_usage_allowed_combinations(self, mock_get):
        mock_get.return_value.json.return_value = {"results": [
            {"index_name": "index1", "endpoint": "example.com", "index_status": "CREATING"},
            {"index_name": "index2", "endpoint": "example2.com", "index_status": "MODIFYING"},
            {"index_name": "index3", "endpoint": "example3.com", "index_status": "READY"},
            {"index_name": "index4", "endpoint": "example4.com", "index_status": "DELETING"},
            {"index_name": "index5", "endpoint": "example5.com", "index_status": "BLAH"},
        ]}
        mapping = MarqoCloudInstanceMappings(
            control_base_url="https://api.marqo.ai", api_key="your-api-key", url_cache_duration=60
        )
        mapping._refresh_urls()
        assert not mapping.is_index_usage_allowed("index1")
        assert not mapping.is_index_usage_allowed("index4")
        assert not mapping.is_index_usage_allowed("index5")
        assert mapping.is_index_usage_allowed("index3")
        assert mapping.is_index_usage_allowed("index2")


