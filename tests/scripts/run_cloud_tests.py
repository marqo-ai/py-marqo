import sys
from create_test_suffix import set_index_suffix
from delete_all_indexes import delete_all_test_indices

if __name__ == '__main__':
    # Generate the random suffix
    set_index_suffix()

    # Run the first command to generate the suffix (already done)
    # generate_index_suffix.py will set the TEST_INDEX_SUFFIX environment variable

    # Run the second command with the generated suffix and pass posargs to pytest
    import pytest
    pytest_args = ['tests/', '-m', 'not ignore_cloud_tests'] + sys.argv[1:]
    pytest.main(pytest_args)

    # Run the third command that uses the suffix
    delete_all_test_indices()
