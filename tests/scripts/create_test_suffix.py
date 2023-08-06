import os
import uuid


def set_index_suffix():
    index_suffix = os.environ.get("MARQO_INDEX_SUFFIX", None)
    if not index_suffix:
        os.environ["MARQO_INDEX_SUFFIX"] = str(uuid.uuid4())[:8]