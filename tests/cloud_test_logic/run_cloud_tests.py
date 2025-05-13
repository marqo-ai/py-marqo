""" Script for running cloud tests. It receives passed arguments and passes them
to pytest. It also handles 3 special arguments:
    --create-indexes: creates the indexes for the tests
    --delete-indexes: deletes the indexes after the tests
    --use-unique-identifier: creates a unique identifier for the indexes,
        so that multiple runs of the tests don't interfere with each other.
        Defaults to cinteg or gets one from the environment variable MQ_TEST_RUN_IDENTIFIER.
    """
import os
import signal
import sys

import concurrent.futures
import threading
import time

import pytest

from create_and_set_cloud_unique_run_identifier import set_unique_run_identifier
from delete_all_cloud_test_indexes import delete_all_test_indices
from marqo.errors import MarqoWebError
from populate_indices_for_cloud_tests import populate_indices

tests_specific_kwargs = {
    'create-indexes': False, # True if you want to create the indexes for the tests. If any index exists, it will be skipped.
    'delete-indexes': False, # True if you want to delete the indexes after the tests.
    'use-unique-identifier': False, # True if you want to create indexes with unique suffix identifier.
}


def handle_interrupt(signum, frame):
    print("\nInterrupt received. Cleaning up and deleting indices.")
    if tests_specific_kwargs['delete-indexes']:
        delete_all_test_indices(wait_for_readiness=False)
    sys.exit(signum)


def convert_string_to_boolean(string_value):
    valid_representations_of_true = ['true', '1']
    if string_value.lower() in valid_representations_of_true:
        return True

def run_pytest(pytest_args):
    """Function to run pytest suite"""
    print("running pytest integration tests with args:", pytest_args)
    return pytest.main(pytest_args)

def run_pytest_with_timeout():
    TIMEOUT_SECONDS = 45 * 60  # 45 minute timeout (Full suite takes ~10 mins now 9/20/24)
    pytest_args = ['tests/', '--cloud'] + sys.argv[1:]

    # Use ThreadPoolExecutor to run pytest in a separate thread
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(run_pytest, pytest_args)

        try:
            # Wait for the pytest to complete or timeout
            pytest_exit_code = future.result(timeout=TIMEOUT_SECONDS)
        except concurrent.futures.TimeoutError:
            print(f"Tests exceeded the {TIMEOUT_SECONDS // 60} minute timeout and were terminated.")
            pytest_exit_code = 1  # Set an exit code indicating failure due to timeout

    return pytest_exit_code

if __name__ == '__main__':
    # Set up the signal handler for KeyboardInterrupt (Cmd+C)
    signal.signal(signal.SIGINT, handle_interrupt)
    tests_args = []
    for arg in sys.argv[1:]:
        for test_specific_arg in tests_specific_kwargs.keys():
            if test_specific_arg in arg:
                tests_specific_kwargs[test_specific_arg] = convert_string_to_boolean(arg.split('=')[1])
                sys.argv.remove(arg)
    try:
        if tests_specific_kwargs['use-unique-identifier']:
            set_unique_run_identifier()
        if 'MQ_TEST_RUN_IDENTIFIER' not in os.environ:
            os.environ['MQ_TEST_RUN_IDENTIFIER'] = 'cinteg'
        print(f"Using unique identifier: {os.environ['MQ_TEST_RUN_IDENTIFIER']}")
        if tests_specific_kwargs['create-indexes']:
            try:
                populate_indices()
            except MarqoWebError as e:
                print("Detected an error while creating indices, deleting all indices and exiting the workflow.")
                delete_all_test_indices(wait_for_readiness=False)
                sys.exit(1)
        print(f"All indices have been created, proceeding to run tests with pytest. Arguments: {sys.argv[1:]}")

        pytest_exit_code = run_pytest_with_timeout()

        if pytest_exit_code != 0:
            raise RuntimeError(f"Pytest failed with exit code: {pytest_exit_code}")
        print("All tests have been executed successfully")
        if tests_specific_kwargs['delete-indexes']:
            delete_all_test_indices(wait_for_readiness=False)
    except Exception as e:
        print(f"Error: {e}")
        if tests_specific_kwargs['delete-indexes']:
            delete_all_test_indices(wait_for_readiness=False)
        sys.exit(1)
