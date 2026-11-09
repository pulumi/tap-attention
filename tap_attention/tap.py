"""Attention tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_attention import streams


class TapAttention(Tap):
    """Singer tap for Attention call recordings."""

    name = "tap-attention"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="Organization-level API key (Attention: Settings > API Keys).",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            default="https://api.attention.tech/v2",
            title="API URL",
            description="Root URL of the Attention API v2.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description=(
                "Earliest call date pulled by the first (stateless) sync. "
                "Later syncs filter by updatedAt only."
            ),
        ),
        th.Property(
            "lookback_hours",
            th.IntegerType(nullable=False),
            required=True,
            description=(
                "Always re-pull conversations updated within this many hours. "
                "Attention does not bump updatedAt for CRM link or scorecard changes."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AttentionStream]:
        """Return the tap's streams."""
        return [
            streams.ConversationsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapAttention.cli()
