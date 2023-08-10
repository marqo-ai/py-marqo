"""
Creates a unique suffix for the index name to avoid conflicts
with another testing executed in the same Marqo cloud account.
"""

import os
import uuid


def set_index_suffix():
    index_suffix = os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")
    if not index_suffix:
        os.environ["MQ_TEST_RUN_IDENTIFIER"] = str(uuid.uuid4())[:8]