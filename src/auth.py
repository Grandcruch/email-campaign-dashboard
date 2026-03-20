"""
auth.py — Shopify OAuth2 client-credentials token manager + HubSpot header helper.
"""

import time
import requests


class ShopifyAuth:
    """
    Manages Shopify access tokens via the client_credentials grant.
    Tokens expire in ~24 hours; auto-refreshes 5 minutes before expiry.
    """

    def __init__(self, store_domain: str, client_id: str, client_secret: str):
        self.store_domain = store_domain
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        """Return a valid access token, refreshing if near expiry."""
        if self._access_token is None or time.time() > (self._expires_at - 300):
            self._refresh()
        return self._access_token  # type: ignore[return-value]

    def _refresh(self) -> None:
        resp = requests.post(
            f"https://{self.store_domain}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 86399)
        print(f"  [auth] Shopify token acquired (scopes: {data.get('scope', 'unknown')})")

    def headers(self) -> dict:
        return {"X-Shopify-Access-Token": self.get_token()}


def hubspot_headers(token: str) -> dict:
    """Return Authorization header for HubSpot API calls."""
    return {"Authorization": f"Bearer {token}"}
