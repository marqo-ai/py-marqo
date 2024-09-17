"""
Creates a unique suffix for the index name to avoid conflicts
with another testing executed in the same Marqo cloud account.
"""

import os
import uuid


def set_unique_run_identifier():
    """
    Set the unique run identifier for this test.
    Use run ID from GitHub workflow, but only if environment variable MQ_TEST_RUN_IDENTIFIER is not set. Priority:
    1. Manually set environment variable MQ_TEST_RUN_IDENTIFIER
    2. GitHub run ID
    3. Random 4-character identifier
    """
    index_suffix = os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")
    if not index_suffix:
        github_run_id = os.environ.get("MARQO_GITHUB_RUN_ID", None)
        if github_run_id:
            print(f"Found GitHub run ID: {github_run_id}. "
                  f"Using the last 4 characters: {github_run_id[-4:]} as the unique identifier.", flush=True)
            os.environ["MQ_TEST_RUN_IDENTIFIER"] = github_run_id[-4:]
        else:
            random_identifier = str(uuid.uuid4())[:4]
            print(f"No unique identifier found. Generating a random one: {random_identifier}.", flush=True)
            os.environ["MQ_TEST_RUN_IDENTIFIER"] = random_identifier
