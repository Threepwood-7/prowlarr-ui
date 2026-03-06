# Integration Tests

This directory contains tests/scripts that require live external dependencies (for example a running Prowlarr instance and optional Everything integration).

- Manual run:
  - `python tests/integration/test_integrations.py`
- Default `pytest` runs target only `tests/unit` and `tests/ui`.
