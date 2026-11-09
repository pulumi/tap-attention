"""REST client for the Attention API v2."""

from __future__ import annotations

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream


class AttentionStream(RESTStream):
    """Base stream for the Attention API."""

    @property
    def url_base(self) -> str:
        """Return the API root."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Send the API key as a Bearer token."""
        return BearerTokenAuthenticator(token=self.config["api_key"])
