"""Prowlarr API client"""
import logging
import time
import requests
from typing import List, Dict, Any

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

    def _api_request(self, endpoint: str, params: Dict = None, method: str = 'GET',
                     data: Dict = None) -> Any:
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
            try:
                if method == 'GET':
                    response = http.get(url, headers=headers, params=params or {}, timeout=self.timeout, auth=self.auth)
                elif method == 'POST':
                    headers['Content-Type'] = 'application/json'
                    response = http.post(url, headers=headers, json=data or {}, timeout=self.timeout, auth=self.auth)
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
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except (requests.ConnectionError, requests.Timeout) as e:
                last_error = e
                if attempt < max_attempts:
                    wait = 2 ** attempt
                    logger.warning(f"Connection error: {e}, retrying in {wait}s (attempt {attempt}/{max_attempts})")
                    time.sleep(wait)
                else:
                    raise

        raise last_error

    def get_indexers(self) -> List[Dict]:
        """Fetch all configured indexers"""
        try:
            return self._api_request('indexer')
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

    def search(self, query: str, indexer_ids: List[int] = None,
               categories: List[int] = None, offset: int = 0, limit: int = 1000) -> List[Dict]:
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

            # Only include indexerIds if specific indexers selected
            if indexer_ids:
                params['indexerIds'] = indexer_ids

            # Only include categories if specific categories selected
            if categories:
                params['categories'] = categories

            return self._api_request('search', params)
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise

    def download(self, guid: str, indexer_id: int) -> bool:
        """
        Download/grab a release
        Uses POST /api/v1/search with ReleaseResource body
        """
        try:
            data = {
                'guid': guid,
                'indexerId': indexer_id
            }
            self._api_request('search', method='POST', data=data)
            logger.info("Download successful via direct API")
            return True
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
