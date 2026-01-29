"""Zoho OAuth token management for email sending."""

import logging
import os

import requests

logger = logging.getLogger(__name__)


class ZohoOAuth:
    """Manages Zoho OAuth tokens with automatic refresh."""

    def __init__(self):
        """Initialize with environment variables."""
        self.client_id = os.getenv("ZOHO_CLIENT_ID")
        self.client_secret = os.getenv("ZOHO_CLIENT_SECRET")
        self.refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
        self.account_id = os.getenv("ZOHO_ACCOUNT_ID")

        # Cache for access token (in-memory, refreshed as needed)
        self._access_token = None

    def _validate_config(self) -> bool:
        """Check if all required config is present."""
        missing = []
        if not self.client_id:
            missing.append("ZOHO_CLIENT_ID")
        if not self.client_secret:
            missing.append("ZOHO_CLIENT_SECRET")
        if not self.refresh_token:
            missing.append("ZOHO_REFRESH_TOKEN")
        if not self.account_id:
            missing.append("ZOHO_ACCOUNT_ID")

        if missing:
            logger.error(f"Missing Zoho config: {', '.join(missing)}")
            return False
        return True

    def refresh_access_token(self) -> str | None:
        """Refresh the access token using refresh token."""
        if not self._validate_config():
            return None

        url = "https://accounts.zoho.com/oauth/v2/token"
        data = {
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            tokens = response.json()

            if "access_token" in tokens:
                self._access_token = tokens["access_token"]
                logger.info("Zoho access token refreshed successfully")
                return self._access_token
            else:
                logger.error(f"No access_token in response: {tokens}")
                return None

        except requests.RequestException as e:
            logger.error(f"Zoho token refresh failed: {e}")
            return None

    def _test_token(self, token: str) -> bool:
        """Test if access token is valid."""
        try:
            headers = {"Authorization": f"Zoho-oauthtoken {token}"}
            response = requests.get(
                "https://mail.zoho.com/api/accounts",
                headers=headers,
                timeout=10,
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_valid_access_token(self) -> str | None:
        """Get a valid access token, refreshing if necessary."""
        if not self._validate_config():
            return None

        # If we have a cached token, test it first
        if self._access_token and self._test_token(self._access_token):
            return self._access_token

        # Need to refresh
        logger.info("Refreshing Zoho access token...")
        return self.refresh_access_token()

    def get_account_id(self) -> str | None:
        """Get the Zoho account ID."""
        return self.account_id
