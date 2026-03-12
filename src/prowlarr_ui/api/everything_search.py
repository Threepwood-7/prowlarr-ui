"""Everything search engine integration"""

import io
import logging
import os
import subprocess
import threading
import zipfile
from pathlib import Path
from typing import Any

import requests
from threep_commons.executables import find_first_available_executable, program_files_candidates
from threep_commons.paths import resolve_app_data_dir

from prowlarr_ui.constants import APP_IDENTITY

logger = logging.getLogger(__name__)


def find_everything_exe() -> str | None:
    """
    Find Everything.exe in common installation paths or PATH
    Returns full path if found, None otherwise
    """
    exe_path = find_first_available_executable(
        command_names=("Everything.exe",),
        candidate_paths=program_files_candidates(Path("Everything") / "Everything.exe"),
    )
    if exe_path is not None:
        logger.info(f"Found Everything.exe at: {exe_path}")
        return str(exe_path)
    logger.warning("Everything.exe not found")
    return None


def _resolve_sdk_dir() -> Path:
    """Resolve SDK cache directory path under the app data root."""
    return resolve_app_data_dir(APP_IDENTITY) / ".everything_sdk"


SDK_DIR = str(_resolve_sdk_dir())
DEFAULT_SDK_URL = "https://www.voidtools.com/Everything-SDK.zip"
DLL_NAME = "Everything64.dll"


def _download_everything_sdk(sdk_url: str = DEFAULT_SDK_URL) -> str | None:
    """Download Everything SDK zip and extract the DLL to the SDK cache directory."""
    try:
        logger.info(f"Downloading Everything SDK from {sdk_url}...")
        response = requests.get(sdk_url, timeout=30)
        response.raise_for_status()

        os.makedirs(SDK_DIR, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Find Everything64.dll inside the zip (may be in a subdirectory)
            dll_entries = [n for n in zf.namelist() if n.endswith(DLL_NAME)]
            if not dll_entries:
                logger.error(f"{DLL_NAME} not found in SDK zip")
                return None

            # Extract the DLL to SDK_DIR
            dll_entry = dll_entries[0]
            dll_data = zf.read(dll_entry)
            dll_path = os.path.join(SDK_DIR, DLL_NAME)
            with open(dll_path, "wb") as f:
                f.write(dll_data)

            logger.info(f"Everything SDK downloaded and extracted to {dll_path}")
            return dll_path
    except Exception as e:
        logger.error(f"Failed to download Everything SDK: {e}")
        return None


def find_everything_dll(sdk_url: str = DEFAULT_SDK_URL) -> str | None:
    """
    Find Everything64.dll in common installation paths, local SDK dir, or PATH.
    If not found anywhere, attempts to download from sdk_url.
    Returns full path if found, None otherwise.
    """
    # Check local SDK directory first
    local_dll = os.path.join(SDK_DIR, DLL_NAME)
    if os.path.exists(local_dll):
        logger.info(f"Found {DLL_NAME} in local SDK dir: {local_dll}")
        return local_dll

    # Common installation paths
    common_paths = [
        r"C:\Program Files\Everything\Everything64.dll",
        r"C:\Program Files (x86)\Everything\Everything64.dll",
        os.path.expandvars(r"%PROGRAMFILES%\Everything\Everything64.dll"),
        os.path.expandvars(r"%PROGRAMFILES(X86)%\Everything\Everything64.dll"),
    ]

    for path in common_paths:
        if os.path.exists(path):
            logger.info(f"Found {DLL_NAME} at: {path}")
            return path

    # Try current directory
    if os.path.exists(DLL_NAME):
        logger.info(f"Found {DLL_NAME} in current directory")
        return os.path.abspath(DLL_NAME)

    # Not found anywhere — attempt to download
    logger.info(f"{DLL_NAME} not found locally, attempting download...")
    return _download_everything_sdk(sdk_url)


class EverythingSearch:
    """
    Integration with Everything search engine
    Supports SDK, HTTP, or disabled based on configuration
    """

    def __init__(self, integration_method: str = "sdk", sdk_url: str = DEFAULT_SDK_URL):
        self.integration_method = integration_method
        self.sdk_url = sdk_url
        self.sdk_available = False
        self.http_available = False
        self.http_url = "http://localhost:80"  # Default Everything HTTP server URL
        self.dll: Any = None
        self._lock = threading.Lock()  # Guards SDK global state for thread safety

        if integration_method == "none":
            logger.info("Everything integration disabled by config")
            return
        elif integration_method == "sdk":
            logger.info("Everything integration: trying SDK")
            self._init_sdk()
        elif integration_method == "http":
            logger.info("Everything integration: using HTTP only")
            self._init_http()
        else:
            logger.warning(f"Unknown Everything integration method '{integration_method}', trying SDK")
            self._init_sdk()

    def _init_sdk(self):
        """Initialize Everything SDK (Windows DLL) - checks common paths and PATH"""
        try:
            import ctypes

            # Find DLL
            dll_path = find_everything_dll(self.sdk_url)
            if not dll_path:
                raise Exception("Everything64.dll not found")

            # Load DLL
            self.dll = ctypes.WinDLL(dll_path)

            # Configure function signatures for search
            self.dll.Everything_SetSearchW.argtypes = [ctypes.c_wchar_p]
            self.dll.Everything_QueryW.argtypes = [ctypes.c_bool]
            self.dll.Everything_GetNumResults.restype = ctypes.c_uint

            # Functions for retrieving results
            self.dll.Everything_GetResultFileNameW.argtypes = [ctypes.c_uint]
            self.dll.Everything_GetResultFileNameW.restype = ctypes.c_wchar_p
            self.dll.Everything_GetResultPathW.argtypes = [ctypes.c_uint]
            self.dll.Everything_GetResultPathW.restype = ctypes.c_wchar_p

            # Size retrieval - use proper LARGE_INTEGER handling
            self.dll.Everything_GetResultSize.argtypes = [ctypes.c_uint, ctypes.POINTER(ctypes.c_longlong)]
            self.dll.Everything_GetResultSize.restype = ctypes.c_bool

            # Request flags - ensure size is included in results
            everything_request_file_name = 0x00000001
            everything_request_path = 0x00000002
            everything_request_size = 0x00000010

            self.dll.Everything_SetRequestFlags.argtypes = [ctypes.c_uint]
            self.dll.Everything_SetRequestFlags(
                everything_request_file_name | everything_request_path | everything_request_size
            )

            self.sdk_available = True
            logger.info("Everything SDK initialized successfully")
        except Exception as e:
            logger.warning(f"Everything SDK not available: {e}, will try HTTP")
            self._init_http()

    def _init_http(self):
        """Initialize Everything HTTP server connection"""
        try:
            response = requests.get(f"{self.http_url}/?search=test&count=1", timeout=2)
            if response.status_code == 200:
                self.http_available = True
                logger.info("Everything HTTP server available")
        except Exception as e:
            logger.warning(f"Everything HTTP server not available: {e}")

    def search(self, query: str, everything_max_results: int = 10) -> list[tuple[str, int]]:
        """
        Search for files or folders
        Returns list of tuples (filename, size_in_bytes)
        """
        # Return empty if disabled
        if self.integration_method == "none":
            return []

        # Use configured method
        if self.integration_method == "sdk" and self.sdk_available:
            return self._search_sdk(query, everything_max_results)
        elif self.integration_method == "http" and self.http_available:
            return self._search_http(query, everything_max_results)

        # Fallback: try whatever is available
        if self.sdk_available:
            return self._search_sdk(query, everything_max_results)
        elif self.http_available:
            return self._search_http(query, everything_max_results)

        return []

    def _search_sdk(self, query: str, everything_max_results: int) -> list[tuple[str, int]]:
        """Search using SDK and return list of (filename, size) tuples.
        Thread-safe: uses a lock to serialize access to the SDK's global state."""
        with self._lock:
            try:
                import ctypes

                # Set search query
                self.dll.Everything_SetSearchW(query)

                # Execute query
                self.dll.Everything_QueryW(True)

                # Get result count
                count = self.dll.Everything_GetNumResults()

                results = []
                for i in range(min(count, everything_max_results)):
                    # Get filename
                    filename = self.dll.Everything_GetResultFileNameW(i)

                    # Get path
                    path = self.dll.Everything_GetResultPathW(i)

                    # Combine to full name
                    if path and filename:
                        full_name = os.path.join(path, filename)
                    elif filename:
                        full_name = filename
                    else:
                        continue

                    # Get file size using LARGE_INTEGER
                    size_value = ctypes.c_longlong()
                    size_retrieved = self.dll.Everything_GetResultSize(i, ctypes.byref(size_value))

                    # Extract size - handle negative values (very large files)
                    if size_retrieved:
                        file_size = size_value.value
                        # If negative, it's a very large file, convert properly
                        if file_size < 0:
                            file_size = (1 << 64) + file_size
                    else:
                        file_size = 0

                    results.append((full_name, file_size))

                return results
            except Exception as e:
                logger.error(f"SDK search failed: {e}")
                return []

    def _search_http(self, query: str, everything_max_results: int) -> list[tuple[str, int]]:
        """Search using HTTP API and return list of (filename, size) tuples"""
        try:
            # Request size and path information from Everything HTTP server
            params: dict[str, str | int] = {
                "search": query,
                "count": everything_max_results,
                "json": 1,
                "path_column": 1,  # Include path in JSON results
                "size_column": 1,  # Include size in JSON results
            }
            response = requests.get(self.http_url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()

                results = []
                for item in data.get("results", []):
                    # Get name
                    name = item.get("name", "")
                    if not name:
                        continue

                    # Get path if available
                    path = item.get("path", "")

                    # Combine to full name
                    full_name = os.path.join(path, name) if path else name

                    # Get size - only for files, not folders
                    size = 0
                    item_type = item.get("type", "file")

                    if item_type != "folder":
                        # Try to get size from various possible field names
                        for size_field in ["size", "size_bytes", "filesize", "file_size"]:
                            if size_field in item:
                                size_value = item[size_field]
                                # Convert to int if it's a string
                                if isinstance(size_value, str):
                                    try:
                                        size = int(size_value)
                                        break
                                    except (ValueError, TypeError):
                                        continue
                                elif isinstance(size_value, (int, float)):
                                    size = int(size_value)
                                    break

                    results.append((full_name, size))
                return results
        except Exception as e:
            logger.error(f"HTTP search failed: {e}")
            import traceback

            logger.error(f"HTTP search traceback: {traceback.format_exc()}")
        return []

    def launch_search(self, query: str):
        """Launch Everything.exe with search query"""
        try:
            exe_path = find_everything_exe()
            if exe_path:
                subprocess.Popen([exe_path, "-search", query])
                logger.info(f"Launched Everything.exe with query: {query}")
            else:
                logger.error("Everything.exe not found, cannot launch")
        except Exception as e:
            logger.error(f"Failed to launch Everything.exe: {e}")
