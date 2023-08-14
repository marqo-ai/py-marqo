import os
import signal
import sys

from create_and_set_cloud_unique_run_identifier import set_unique_run_identifier
from delete_all_cloud_test_indexes import delete_all_test_indices
from populate_indices_for_cloud_tests import populate_indices


def handle_interrupt(signum, frame):
    print("\nInterrupt received. Cleaning up and deleting indices.")
    delete_all_test_indices()
    sys.exit(1)


if __name__ == '__main__':
    # Set up the signal handler for KeyboardInterrupt (Cmd+C)
    signal.signal(signal.SIGINT, handle_interrupt)

    try:
        set_unique_run_identifier()
        print(f"Using unique identifier: {os.environ['MQ_TEST_RUN_IDENTIFIER']}")
        populate_indices()
        print(f"All indices has been created, proceeding to run tests with pytest. Arguments: {sys.argv[1:]}")

        import pytest
        pytest_args = ['tests/', '-m', 'not ignore_during_cloud_tests'] + sys.argv[1:]
        pytest.main(pytest_args)
        print("All tests has been executed, proceeding to delete indices")

        delete_all_test_indices()
    except Exception as e:
        print(f"Error: {e}")
        delete_all_test_indices()
