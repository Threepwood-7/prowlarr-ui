"""Prowlarr API client"""
import logging
import time
import requests
from typing import List, Dict, Any, Callable, Optional

logger = logging.getLogger(__name__)


class ProwlarrClient:
    """
    Prowlarr API client using direct REST API calls
    """

    def __init__(self, host: str, api_key: str,
                 timeout: int = 30, retries: int = 2,
                 http_basic_auth_username: str = '', http_basic_auth_password: str = ''):
        self.host = host.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.retries = retries
        self.auth = (http_basic_auth_username, http_basic_auth_password) if http_basic_auth_username else None

    @staticmethod
    def _sleep_with_cancel(seconds: float, should_cancel: Optional[Callable[[], bool]]) -> bool:
        """Sleep in small increments so retries can be cancelled cooperatively."""
        deadline = time.monotonic() + max(0.0, float(seconds))
        while True:
            if should_cancel and should_cancel():
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.1, remaining))

    def _request_timeout(self, cancellable: bool = False):
        """
        Build a requests timeout value (seconds).
        Cancellable worker requests use the same timeout so long-running searches can complete.
        """
        try:
            base = float(self.timeout)
        except Exception:
            base = 120.0
        if base <= 0:
            base = 120.0
        return base

    def _api_request(self, endpoint: str, params: Dict = None, method: str = 'GET',
                     data: Dict = None, should_cancel: Optional[Callable[[], bool]] = None) -> Any:
        """
        Make a direct API request to Prowlarr with retry logic.
        Retries on connection errors and 5xx responses with exponential backoff.
        """
        url = f"{self.host}/api/v1/{endpoint}"
        headers = {'X-Api-Key': self.api_key}

        # Log request
        logger.debug(f"Prowlarr API Request: {method} {url}")
        if params:
            logger.debug(f"Request params: {params}")
        if data:
            logger.debug(f"Request data: {data}")

        http = requests

        last_error = None
        max_attempts = 1 + self.retries  # retries=2 means 3 total attempts
        for attempt in range(1, max_attempts + 1):
            if should_cancel and should_cancel():
                raise RuntimeError("Prowlarr request cancelled")
            timeout_value = self._request_timeout(cancellable=bool(should_cancel))
            try:
                if method == 'GET':
                    response = http.get(url, headers=headers, params=params or {}, timeout=timeout_value, auth=self.auth)
                elif method == 'POST':
                    headers['Content-Type'] = 'application/json'
                    response = http.post(url, headers=headers, json=data or {}, timeout=timeout_value, auth=self.auth)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                # Log response
                logger.debug(f"Prowlarr API Response: Status {response.status_code}")

                # Try to log response body (truncate if too large)
                try:
                    response_json = response.json()
                    response_str = str(response_json)
                    if len(response_str) > 10000:
                        logger.debug(f"Response body (truncated): {response_str[:10000]}... (total length: {len(response_str)})")
                    else:
                        logger.debug(f"Response body: {response_json}")
                except Exception:
                    logger.debug(f"Response body (non-JSON): {response.text[:1000]}")

                # Retry on server errors (5xx)
                if response.status_code >= 500 and attempt < max_attempts:
                    wait = 2 ** attempt
                    logger.warning(f"Server error {response.status_code}, retrying in {wait}s (attempt {attempt}/{max_attempts})")
                    if self._sleep_with_cancel(wait, should_cancel):
                        raise RuntimeError("Prowlarr request cancelled")
                    continue

                response.raise_for_status()
                return response.json()

            except (requests.ConnectionError, requests.Timeout) as e:
                last_error = e
                if attempt < max_attempts:
                    wait = 2 ** attempt
                    logger.warning(f"Connection error: {e}, retrying in {wait}s (attempt {attempt}/{max_attempts})")
                    if self._sleep_with_cancel(wait, should_cancel):
                        raise RuntimeError("Prowlarr request cancelled")
                else:
                    raise

        raise last_error

    def get_indexers(self, should_cancel: Optional[Callable[[], bool]] = None) -> List[Dict]:
        """Fetch all configured indexers"""
        try:
            return self._api_request('indexer', should_cancel=should_cancel)
        except Exception as e:
            logger.error(f"Failed to get indexers: {e}")
            raise

    def get_categories(self) -> List[Dict]:
        """
        Get predefined Prowlarr categories
        These are standard across all indexers
        """
        return [
            {"id": 1000, "name": "Console"},
            {"id": 2000, "name": "Movies"},
            {"id": 3000, "name": "Audio"},
            {"id": 4000, "name": "PC"},
            {"id": 5000, "name": "TV"},
            {"id": 6000, "name": "XXX"},
            {"id": 7000, "name": "Books"},
            {"id": 8000, "name": "Other"},
        ]

    def search(self, query: str, indexer_ids: Optional[List[int]] = None,
               categories: Optional[List[int]] = None, offset: int = 0, limit: int = 1000,
               should_cancel: Optional[Callable[[], bool]] = None) -> List[Dict]:
        """
        Search across indexers
        Returns list of release dictionaries
        """
        try:
            params = {
                'type': 'search',
                'query': query,
                'offset': offset,
                'limit': limit
            }

            # Include explicit list when provided. None means "all".
            if indexer_ids is not None:
                params['indexerIds'] = indexer_ids

            # Include explicit list when provided. None means "all".
            if categories is not None:
                params['categories'] = categories

            return self._api_request('search', params, should_cancel=should_cancel)
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise

    def download(self, guid: str, indexer_id: int,
                 should_cancel: Optional[Callable[[], bool]] = None) -> bool:
        """
        Download/grab a release
        Uses POST /api/v1/search with ReleaseResource body
        """
        try:
            if should_cancel and should_cancel():
                logger.info("Download request cancelled before API call")
                return False
            data = {
                'guid': guid,
                'indexerId': indexer_id
            }
            self._api_request('search', method='POST', data=data, should_cancel=should_cancel)
            logger.info("Download successful via direct API")
            return True
        except RuntimeError as e:
            logger.info(f"Download cancelled: {e}")
            return False
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
