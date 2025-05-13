# Integration Tests for py-marqo

This document explains how to run the integration tests in the `py-marqo` project. You can run tests against either:

- **Open Source Marqo**
- **Marqo Cloud**

---

## 🔧 Running Tests Against Open Source Marqo

To run integration tests against a local Open Source Marqo instance, simply use:

```bash
tox
```

This assumes Marqo is running locally (typically on `http://localhost:8882`).

### 🧹 Index and Document Cleanup

Open Source tests enforce strict isolation by:
- **Deleting indexes** between each test class.
- **Deleting all documents** between each individual test using the [`/indexes/{index_name}/documents/delete-all`](https://docs.marqo.ai/latest/API-Reference/indexes.html#delete-all-documents) API.

> **Note:** The `delete-all` API is **disabled in Marqo Cloud** for security reasons. This causes behavioral differences between Open Source and Cloud test runs.

---

## ☁️ Running Tests Against Marqo Cloud

To run integration tests against Marqo Cloud, set the following environment variables:

### Required Environment Variables

```bash
export MARQO_CLOUD_URL=https://your-cloud-instance.marqo.ai  # Should match MARQO_URL for non-default cloud environments (e.g. staging)
export MARQO_API_KEY=your_api_key_here  # Your Marqo Cloud API key
export MARQO_URL=https://your-cloud-instance.marqo.ai  # Marqo Cloud endpoint
```

- `MARQO_CLOUD_URL` is used internally to determine if the test suite is targeting Marqo Cloud. It should be **equal to `MARQO_URL`** for non-default environments such as staging.

### Running the Tests

```bash
tox -e cloud_tests
```

---

## ⚙️ Optional Arguments for Cloud Tests

The cloud test suite supports several additional arguments:

| Argument                | Default | Description |
|-------------------------|---------|-------------|
| `create-indexes`        | `False` | If `True`, creates test indexes before test execution. Skips any that already exist. |
| `delete-indexes`        | `False` | If `True`, deletes indexes after tests complete. |
| `use-unique-identifier` | `False` | If `True`, appends a unique suffix to index names to avoid collisions. |

### Example

```bash
tox -e cloud_tests -- create-indexes=True use-unique-identifier=False delete-indexes=True
```

---

## 🧹 Document Cleanup Behavior (Cloud vs Open Source)

| Aspect                | Open Source                        | Cloud                                 |
|------------------------|------------------------------------|----------------------------------------|
| **Document Cleanup**   | Uses `documents/delete-all` API between **every test** | Tracks document IDs and performs individual deletions in `tearDown` |
| **Index Cleanup**      | Deletes indexes between **test classes** | Optional: controlled via `delete-indexes` argument |
| **Isolation Level**    | High (reset state between tests)  | Moderate (controlled via parameters)   |

> ⚠️ **Important:** These differences can lead to **test suite discrepancies** between Open Source and Cloud runs, especially in tests relying on full state resets.

---

## 🆔 Custom Index Identifier

If `use-unique-identifier=True`, a random suffix is generated to avoid collisions between test runs.

To define a custom identifier:

```bash
export MQ_TEST_RUN_IDENTIFIER=my-custom-id
```

This is useful for debugging or tracking test runs in shared environments.

---

## ✅ Running Tests via GitHub Actions

You can trigger integration tests on a branch using the **Cloud Integration Tests** GitHub Action.

### Default Behavior

- Tests will run in the **default Marqo Cloud test account**.
- Indexes will be deleted automatically after the run finishes.

> ⚠️ **Do not cancel the action after it has started**, as this may leave indexes behind.

### Cleaning Up Leftover Indexes

To manually clean up leftover indexes from the Cloud test account:

1. Trigger the same **Cloud Integration Tests** action.
2. Set the input `job_to_run` to `delete_all_indexes`.

This will remove all test-created indexes safely.

---

## 📌 Summary

- Use `tox` for Open Source, and `tox -e cloud_tests` for Cloud.
- Set all required environment variables when targeting Cloud.
- Be aware of differences in document and index cleanup logic that may affect test results.
- Use GitHub Actions for automated CI tests and cleanup tasks.

---

Happy testing! 🚀