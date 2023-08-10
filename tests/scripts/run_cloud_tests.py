import os
import sys
from create_test_suffix import set_index_suffix
from delete_all_cloud_test_indexes import delete_all_test_indices
from populate_indices_for_cloud_tests import populate_indices

if __name__ == '__main__':
    # Generate the random suffix
    set_index_suffix()
    print(f"Using unique identifier: {os.environ.get('MQ_TEST_RUN_IDENTIFIER', '')}")
    populate_indices()
    print(f"All indices has been created, proceeding to run tests with pytest. Arguments: {sys.argv[1:]}")

    import pytest
    pytest_args = ['tests/', '-m', 'not ignore_during_cloud_tests'] + sys.argv[1:]
    pytest.main(pytest_args)
    print("All tests has been executed, proceeding to delete indices")

    delete_all_test_indices()
