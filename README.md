# Prowlarr Search Client

A Windows desktop application for searching [Prowlarr](https://prowlarr.com/) indexers with [Everything](https://www.voidtools.com/) integration for duplicate detection and batch downloads.

Built with PySide6 (Qt for Python).

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![PySide6](https://img.shields.io/badge/GUI-PySide6-green)
![Windows](https://img.shields.io/badge/platform-Windows-lightgrey)

## Features

- **Multi-indexer search** - query all your Prowlarr indexers at once, filter by indexer and category
- **Duplicate detection** - automatically checks results against [Everything](https://www.voidtools.com/) to find files you already have on disk
- **Batch downloads** - download individual results, selected rows, or everything visible with one click
- **Bookmarks** - save frequently used search queries for quick access
- **Quality parsing** - displays resolution, source, codec, and HDR info extracted from release titles
- **Keyboard-driven** - full keyboard navigation with single-key shortcuts for common actions
- **Custom commands** - bind F2/F3/F4 to your own scripts with `{title}` and `{video}` placeholders
- **Paginated results** - navigate through large result sets page by page or load all pages at once

## Screenshot

![Prowlarr Search Client](prowlarr_ui_screenshot.jpg)

*Left panel with search controls and filters, center results table with color-coded title grouping, and a detachable log window.*

## Requirements

- **Windows** (10 or later)
- **Python 3.10+**
- **Prowlarr** instance with API access
- **Everything** (optional) - for duplicate detection via SDK or HTTP server

## Installation

1. Clone the repository and install dependencies:

```bash
pip install -r requirements.txt
```

2. Copy the example config and add your Prowlarr credentials:

```bash
cp prowlarr_ui_config_example.toml prowlarr_ui_config.toml
```

3. Edit `prowlarr_ui_config.toml` with your Prowlarr API key (found in Prowlarr under *Settings > General*):

```toml
[prowlarr]
host = "http://localhost:9696"
api_key = "YOUR_API_KEY_HERE"
# http_basic_auth_username = ""
# http_basic_auth_password = ""
```

## Usage

```bash
python main.py
```

### Quick Start

1. Enter a search query and press **Enter**
2. Select indexers and categories from the tree views on the left
3. Results appear in the center table, color-grouped by title
4. Press **Space** to download a result and advance to the next row
5. Gray rows = already on disk (detected by Everything)

### Keyboard Shortcuts

These work when the results table is focused:

| Key | Action |
|---|---|
| **Space** | Download current row, advance to next |
| **S** | Launch Everything search for the title |
| **C** | Copy release title to clipboard |
| **G** | Open web search for the title |
| **P** | Play video file found by Everything |
| **Tab** | Jump to next title group |
| **Shift+Tab** | Jump to previous title group |
| **Ctrl+A** | Select all visible rows |
| **Ctrl+F** | Find in table |
| **F1** | Show help |
| **F2 / F3 / F4** | Run custom commands (configurable) |

## Configuration

All settings live in `prowlarr_ui_config.toml`. See [`prowlarr_ui_config_example.toml`](prowlarr_ui_config_example.toml) for the full template.

### Key Settings

| Setting | Default | Description |
|---|---|---|
| `everything_integration_method` | `"sdk"` | `"sdk"`, `"http"`, or `"none"` |
| `title_match_chars` | `42` | Characters used for title grouping and color coding |
| `everything_search_chars` | `42` | Characters used for Everything prefix search |
| `api_timeout` | `30` | API request timeout in seconds |
| `api_retries` | `2` | Retry attempts on connection errors |
| `prowlarr_page_size` | `100` | Results per page from Prowlarr API |
| `everything_recheck_delay` | `6000` | Delay in ms before rechecking Everything after download |
| `everything_max_results` | `5` | Max Everything matches shown in tooltip |
| `everything_batch_size` | `10` | Results per UI update batch during Everything check |
| `web_search_url` | `"https://...google..."` | URL template with `{query}` placeholder |

### Custom Commands

Bind scripts to F2, F3, F4 in the `[settings]` section:

```toml
custom_command_F2 = 'my_script.bat "{title}" "{video}"'
custom_command_F3 = 'explorer /select,"{video}"'
custom_command_F4 = 'notepad "{title}"'
```

Placeholders: `{title}` = release title, `{video}` = video file path from Everything (empty if not found).

### Preferences

The `[preferences]` section is auto-saved on exit and includes search history, selected indexers/categories, splitter position, column widths, and bookmarks. You generally don't need to edit this manually.

## Everything Integration

[Everything](https://www.voidtools.com/) is a Windows file search engine. This app uses it to detect which releases you already have on disk.

**SDK mode** (default): The app auto-downloads `Everything64.dll` from voidtools on first run. Requires Everything to be running.

**HTTP mode**: Uses Everything's built-in HTTP server. Enable it in Everything: *Tools > Options > HTTP Server*.

**None**: Disable Everything integration entirely if you don't need duplicate detection.

## Project Structure

```
main.py                        Main entry point and UI (MainWindow)
src/
  api/
    prowlarr_client.py         Prowlarr REST API client
    everything_search.py       Everything SDK/HTTP integration
  workers/
    search_worker.py           Background search thread
    everything_worker.py       Background Everything check thread
    download_worker.py         Download queue processor
  ui/
    widgets.py                 Custom table widget for numeric sorting
    log_window.py              Detachable log viewer window
    help_text.py               Help dialog content
  utils/
    config.py                  TOML config load/save with atomic writes
    formatters.py              Size and age formatting utilities
    logging_config.py          Rotating file log setup
    quality_parser.py          Resolution/source/codec extraction from titles
prowlarr_ui_config_example.toml            Configuration template
test_integrations.py           Prowlarr + Everything connectivity tests
```

## Testing

Run the integration tests to verify your Prowlarr and Everything connections:

```bash
python test_integrations.py
```

Run the automated headless UI tests (mocked, no live Prowlarr/Everything required):

```bash
pip install -r requirements-dev.txt
pytest -q
```

## Dependencies

| Package | Purpose |
|---|---|
| PySide6 >= 6.5.0 | Qt GUI framework |
| requests >= 2.31.0 | HTTP client for Prowlarr API |
| tomlkit >= 0.12.0 | TOML config with comment preservation |
| colorama >= 0.4.6 | Colored test output |

## License

MIT License. See [LICENSE](LICENSE) for details.

---

**Tags:** prowlarr, prowlarr-client, prowlarr-gui, prowlarr-search, usenet, torrent, nzb, indexer, newznab, torznab, pyside6, qt, desktop-app, windows, everything-search, voidtools, duplicate-detection, batch-download, nzb-download, torrent-search, usenet-search, python, search-client, prowlarr-api
