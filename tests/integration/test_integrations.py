#!/usr/bin/env python3
"""
Integration Test Suite for Prowlarr UI
Tests Prowlarr and Everything integrations with color-coded results
"""

import sys

import pytest

from prowlarr_ui.api.everything_search import EverythingSearch
from prowlarr_ui.api.prowlarr_client import ProwlarrClient
from prowlarr_ui.utils.config import load_config

# Use colorama for cross-platform color support (Windows + Linux)
try:
    from colorama import Fore, Style, init

    init(autoreset=True)  # Auto-reset colors after each print
    COLORS_AVAILABLE = True
except ImportError:
    print("WARNING: colorama not installed. Install with: pip install colorama")
    print("Colors will not work without it.\n")
    COLORS_AVAILABLE = False

    # Fallback to no colors
    class Fore:
        GREEN = RED = YELLOW = BLUE = CYAN = MAGENTA = WHITE = ""

    class Style:
        RESET_ALL = BRIGHT = ""


# Color shortcuts
class Colors:
    GREEN = Fore.GREEN
    RED = Fore.RED
    YELLOW = Fore.YELLOW
    BLUE = Fore.BLUE
    RESET = Style.RESET_ALL
    BOLD = Style.BRIGHT


@pytest.fixture
def config():
    return load_config()


def print_header(text):
    """Print a section header"""
    print(f"\n{Colors.BLUE}{Colors.BOLD}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{text}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'=' * 60}{Colors.RESET}")


def print_test(name, status, details=""):
    """Print a test result with color coding"""
    if status:
        status_text = f"{Colors.GREEN}[OK]{Colors.RESET}"
    else:
        status_text = f"{Colors.RED}[KO]{Colors.RESET}"

    print(f"{status_text} {name}")
    if details:
        print(f"    {Colors.YELLOW}{details}{Colors.RESET}")


def test_prowlarr_api(config):
    """Test Prowlarr using direct API method"""
    print_header("PROWLARR - Direct API Method")

    prowlarr_config = config.get("prowlarr", {})
    host = prowlarr_config.get("host", "http://localhost:9696")
    api_key = prowlarr_config.get("api_key", "")

    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print_test("API Key Check", False, "API key not configured in config")
        return False

    print_test("API Key Check", True, f"Key configured: {api_key[:8]}...")

    try:
        client = ProwlarrClient(
            host,
            api_key,
            http_basic_auth_username=prowlarr_config.get("http_basic_auth_username", ""),
            http_basic_auth_password=prowlarr_config.get("http_basic_auth_password", ""),
        )
        print_test("Client Creation", True, f"Host: {host}")
    except Exception as e:
        print_test("Client Creation", False, str(e))
        return False

    # Test indexers
    try:
        indexers = client.get_indexers()
        print_test("Get Indexers", True, f"Found {len(indexers)} indexers")

        # Show first 3 indexers
        for idx in indexers[:3]:
            enabled = "enabled" if idx.get("enable") else "disabled"
            print(f"    - {idx.get('name', 'Unknown')} ({enabled})")
        if len(indexers) > 3:
            print(f"    ... and {len(indexers) - 3} more")
    except Exception as e:
        print_test("Get Indexers", False, str(e))
        return False

    # Test categories
    try:
        categories = client.get_categories()
        print_test("Get Categories", True, f"Found {len(categories)} categories")
    except Exception as e:
        print_test("Get Categories", False, str(e))
        return False

    # Test search
    try:
        test_query = "test"
        results = client.search(test_query, offset=0, limit=5)
        print_test("Search Test", True, f"Query '{test_query}' returned {len(results)} results")

        # Show first 2 results
        for result in results[:2]:
            title = result.get("title", "Unknown")[:50]
            indexer = result.get("indexer", "Unknown")
            print(f"    - {title}... (from {indexer})")
    except Exception as e:
        print_test("Search Test", False, str(e))
        return False

    return True


def test_everything_sdk():
    """Test Everything using SDK method"""
    print_header("EVERYTHING - SDK Method (DLL)")

    try:
        everything = EverythingSearch(integration_method="sdk")

        if not everything.sdk_available:
            print_test("SDK Available", False, "Everything64.dll not found or failed to load")
            return False

        print_test("SDK Available", True, "Everything64.dll loaded successfully")
    except Exception as e:
        print_test("SDK Initialization", False, str(e))
        return False

    # Test search
    try:
        test_query = "*.txt"
        results = everything.search(test_query, everything_max_results=5)
        print_test("Search Test", True, f"Query '{test_query}' returned {len(results)} results")

        # Show first 3 results
        for filename, size in results[:3]:
            size_kb = size / 1024 if size > 0 else 0
            display_name = filename[:60] + "..." if len(filename) > 60 else filename
            print(f"    - {display_name} ({size_kb:.1f} KB)")
    except Exception as e:
        print_test("Search Test", False, str(e))
        return False

    return True


def test_everything_http():
    """Test Everything using HTTP method"""
    print_header("EVERYTHING - HTTP Method")

    try:
        everything = EverythingSearch(integration_method="http")

        if not everything.http_available:
            print_test("HTTP Server Available", False, "Everything HTTP server not responding at http://localhost:80")
            print_test("", False, "Enable HTTP server in Everything: Tools > Options > HTTP Server")
            return False

        print_test("HTTP Server Available", True, "Connected to http://localhost:80")
    except Exception as e:
        print_test("HTTP Initialization", False, str(e))
        return False

    # Test search
    try:
        test_query = "*.txt"
        results = everything.search(test_query, everything_max_results=5)
        print_test("Search Test", True, f"Query '{test_query}' returned {len(results)} results")

        # Show first 3 results
        for filename, size in results[:3]:
            size_kb = size / 1024 if size > 0 else 0
            display_name = filename[:60] + "..." if len(filename) > 60 else filename
            print(f"    - {display_name} ({size_kb:.1f} KB)")
    except Exception as e:
        print_test("Search Test", False, str(e))
        return False

    return True


def main():
    """Run all integration tests"""
    if not COLORS_AVAILABLE:
        print("ERROR: colorama library required for colors")
        print("Install it with: pip install colorama")
        print("Continuing without colors...\n")

    print(f"{Colors.BOLD}Prowlarr UI Integration Test Suite{Colors.RESET}")
    print("Testing all integration methods with runtime config store")

    # Load config
    print_header("Configuration")
    try:
        config = load_config()
        print_test("Load runtime config", True, "Configuration loaded successfully")
    except Exception as e:
        print_test("Load runtime config", False, str(e))
        print(f"\n{Colors.RED}Cannot proceed without valid runtime config{Colors.RESET}")
        return 1

    # Run all tests
    results = {
        "Prowlarr API": test_prowlarr_api(config),
        "Everything SDK": test_everything_sdk(),
        "Everything HTTP": test_everything_http(),
    }

    # Summary
    print_header("Test Summary")
    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, status in results.items():
        print_test(name, status)

    print(f"\n{Colors.BOLD}Results: {passed}/{total} tests passed{Colors.RESET}")

    if passed == total:
        print(f"{Colors.GREEN}{Colors.BOLD}All tests passed!{Colors.RESET}\n")
        return 0
    else:
        print(
            f"{Colors.YELLOW}{Colors.BOLD}Some tests failed. Check configuration and service availability.{Colors.RESET}\n"
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
