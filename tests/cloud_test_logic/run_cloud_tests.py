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

from create_and_set_cloud_unique_run_identifier import set_unique_run_identifier
from delete_all_cloud_test_indexes import delete_all_test_indices
from populate_indices_for_cloud_tests import populate_indices

tests_specific_kwargs = {
    'create-indexes': False, 'delete-indexes': False, 'use-unique-identifier': False,
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
            populate_indices()
        print(f"All indices has been created, proceeding to run tests with pytest. Arguments: {sys.argv[1:]}")

        import pytest
        pytest_args = ['tests/', '-m', 'not ignore_during_cloud_tests'] + sys.argv[1:]
        print("running integration tests with args:", pytest_args)
        pytest_exit_code = pytest.main(pytest_args)
        if pytest_exit_code != 0:
            raise RuntimeError(f"Pytest failed with exit code: {pytest_exit_code}")
        print("All tests has been executed successfully")

        if tests_specific_kwargs['delete-indexes']:
            delete_all_test_indices(wait_for_readiness=True)
    except Exception as e:
        print(f"Error: {e}")
        if tests_specific_kwargs['delete-indexes']:
            delete_all_test_indices(wait_for_readiness=True)
        sys.exit(1)
