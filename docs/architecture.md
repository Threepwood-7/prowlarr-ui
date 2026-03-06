# Architecture Overview

## Runtime Shape

- Entry points:
  - `python -m prowlarr_ui`
  - `prowlarr-ui`
- Main UI and composition:
  - `src/prowlarr_ui/app.py`
- Modular packages:
  - `api/` external integrations (Prowlarr, Everything)
  - `workers/` background QThread workers
  - `ui/` focused widgets/windows/helpers
  - `utils/` config, logging, parsing, formatting

## Worker Lifecycle Model

The UI remains responsive by running blocking operations in worker threads:

1. `InitWorker`
- Initializes Everything integration.
- Fetches indexers/categories from Prowlarr.
- Emits one completion signal with initialized dependencies and any startup error text.

2. `SearchWorker`
- Executes Prowlarr search requests.
- Supports interruption checks to avoid stale UI updates during shutdown/new searches.

3. `EverythingCheckWorker`
- Resolves local file matches for current result set.
- Feeds results back in batches to avoid large UI pauses.

4. `DownloadWorker`
- Serially processes queue items.
- Supports dedupe/enqueue behavior while running.

## Shutdown Model

- Shutdown is coordinated from `MainWindow.closeEvent`.
- Active workers are asked to interrupt first.
- Retry/force-stop windows are bounded by configurable timing guards.
- UI status and spinner state are synchronized with worker ownership to prevent stale "busy" states.

## State and Ownership Rules

- `MainWindow` owns:
  - Active worker references
  - Search generation counters
  - Deferred/retry timer references
  - Preferences persistence lifecycle
- Utility modules own filesystem paths for:
  - Config (`config/app.local.toml` with layered defaults/secrets)
  - App/runtime logs (`<temp>/prowlarr-ui/prowlarr_ui.log`, `<temp>/prowlarr-ui/download_history.log`)
  - Everything SDK cache (`.everything_sdk`)

## Testing Strategy

- `tests/unit/`: deterministic logic and regression tests.
- `tests/ui/`: pytest-qt GUI behavior tests with fake backends.
- `tests/integration/`: live dependency checks, executed manually or explicitly in CI lanes.
