"""Streams for tap-attention."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone

from singer_sdk import typing as th
from singer_sdk.pagination import (
    BaseAPIPaginator,
    JSONPathPaginator,
    PageNumberPaginator,
    SinglePagePaginator,
)

from tap_attention.client import AttentionStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

PAGE_SIZE = 50  # API maximum for /conversations/list

PARTICIPANT = th.ObjectType(
    th.Property("id", th.StringType),
    th.Property("email", th.StringType),
    th.Property("name", th.StringType),
    th.Property("organizer", th.BooleanType),
    th.Property("status", th.StringType),
)

CRM_RECORD = th.ObjectType(
    th.Property("id", th.StringType),
    th.Property("code", th.StringType),
)

# Kept as raw JSON: the API spec does not pin down these shapes.
OPEN_OBJECT = th.ObjectType(additional_properties=True)

TEAM = th.ObjectType(
    th.Property("uuid", th.StringType),
    th.Property("name", th.StringType),
    th.Property("parentTeamUUID", th.StringType),
)


class PageCountPaginator(PageNumberPaginator):
    """Page-number paging that stops at meta.pageCount."""

    def has_more(self, response: requests.Response) -> bool:
        """Return True while the current page is below the page count."""
        meta = response.json()["meta"]
        return meta["pageNumber"] < meta["pageCount"]


class ConversationsStream(AttentionStream):
    """Recorded calls with participants, transcript and CRM links.

    The first sync (no bookmark) pages through every conversation, because the
    updatedAt filter skips conversations that never got an updatedAt. Later syncs
    request updatedAt >= min(bookmark, now - lookback_hours) and follow nextCursor.
    """

    name = "conversations"
    path = "/conversations/list"
    primary_keys = ("id",)
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Conversation id"),
        th.Property("uuid", th.StringType),
        th.Property("title", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property(
            "updatedAt",
            th.DateTimeType,
            description="Last change to the conversation itself (not CRM links or scorecards)",
        ),
        th.Property("finishedAt", th.DateTimeType),
        th.Property("liveStreamStartedAt", th.DateTimeType),
        th.Property("liveStreamEndedAt", th.DateTimeType),
        th.Property("mediaDuration", th.NumberType, description="Recording length in seconds"),
        th.Property("userUUID", th.StringType, description="Conversation owner (Attention user)"),
        th.Property("teamUUID", th.StringType),
        th.Property("participants", th.ArrayType(PARTICIPANT), description="People who joined"),
        th.Property("attendees", th.ArrayType(PARTICIPANT), description="Calendar invitees"),
        th.Property(
            "transcript",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "speaker",
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("email", th.StringType),
                            th.Property("type", th.StringType),
                        ),
                    ),
                    th.Property(
                        "words",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("text", th.StringType),
                                th.Property("startTimestamp", th.NumberType),
                                th.Property("endTimestamp", th.NumberType),
                            )
                        ),
                    ),
                )
            ),
            description="Speaker turns with word-level timings in seconds",
        ),
        th.Property("transcriptStatus", th.StringType),
        th.Property("dealUUID", th.StringType),
        th.Property(
            "externalOpportunity",
            th.ObjectType(
                th.Property("uuid", th.StringType),
                th.Property("title", th.StringType),
            ),
        ),
        th.Property("externalIntelligenceOpportunityID", th.StringType),
        th.Property("linkedCrmRecords", th.ArrayType(CRM_RECORD)),
        th.Property("externalAccounts", th.ArrayType(CRM_RECORD)),
        th.Property("externalContacts", th.ArrayType(CRM_RECORD)),
        th.Property("crmExportStatus", th.StringType),
        th.Property("labels", OPEN_OBJECT),
        th.Property(
            "scorecardResults", th.ArrayType(OPEN_OBJECT), description="AI scorecards and summaries"
        ),
        th.Property("extractedIntelligence", OPEN_OBJECT, description="AI-extracted CRM fields"),
        th.Property("confirmedExtractedIntelligence", OPEN_OBJECT),
        th.Property("archived", th.BooleanType),
        th.Property("isInternal", th.BooleanType),
        th.Property("isEmpty", th.BooleanType),
        th.Property("referenceable", th.BooleanType),
        th.Property("videoUID", th.StringType),
        th.Property("videoStatus", th.StringType),
        th.Property("mediaStorageStatus", th.StringType),
        th.Property("importStatus", th.StringType),
        th.Property(
            "bots",
            th.ArrayType(
                th.ObjectType(
                    th.Property("uuid", th.StringType),
                    th.Property("meetingUrl", th.StringType),
                    th.Property(
                        "status",
                        th.ObjectType(
                            th.Property("code", th.StringType),
                            th.Property("message", th.StringType),
                            th.Property("createdAt", th.DateTimeType),
                        ),
                    ),
                )
            ),
        ),
        th.Property("zoomMeetingJoinUrl", th.StringType),
        th.Property("googleMeetJoinUrl", th.StringType),
        th.Property("microsoftTeamsJoinUrl", th.StringType),
        th.Property("webexJoinUrl", th.StringType),
    ).to_dict()

    def _bookmark(self, context: Context | None) -> str | None:
        return self.get_context_state(context).get("replication_key_value")

    def _updated_since(self, context: Context | None) -> datetime:
        bookmark = datetime.fromisoformat(str(self._bookmark(context)).replace("Z", "+00:00"))
        lookback = datetime.now(timezone.utc) - timedelta(hours=self.config["lookback_hours"])
        return min(bookmark, lookback)

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Use the updatedAt cursor once a bookmark exists, page numbers before."""
        if self._bookmark(None):
            return JSONPathPaginator("$.nextCursor")
        return PageCountPaginator(start_value=1)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Build query parameters for the current page."""
        params: dict[str, t.Any] = {
            "size": PAGE_SIZE,
            "detailedTranscript": "true",
            "withCrmRecords": "true",
            "filter[include_internal_participants]": "true",
        }
        if self._bookmark(context):
            params["fromUpdatedAtDateTime"] = self._updated_since(context).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            if next_page_token:
                params["cursor"] = next_page_token
        else:
            params["page"] = next_page_token
            if self.config.get("start_date"):
                params["fromDateTime"] = self.config["start_date"]
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Lift the JSON:API attributes to the top level."""
        for row in response.json()["data"]:
            yield {"id": row["id"], **row["attributes"]}


class UsersStream(AttentionStream):
    """Attention users, including deactivated ones."""

    name = "users"
    path = "/users"
    primary_keys = ("uuid",)
    replication_key = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("uuid", th.StringType),
        th.Property("email", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("deletedAt", th.DateTimeType),
        th.Property("seatType", th.StringType),
        th.Property("primaryTeamUUID", th.StringType),
        th.Property("organizationRoleUUID", th.StringType),
        th.Property(
            "roles",
            th.ArrayType(
                th.ObjectType(
                    th.Property("uuid", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("type", th.StringType),
                )
            ),
        ),
        th.Property("teams", th.ArrayType(TEAM)),
    ).to_dict()

    def get_new_paginator(self) -> SinglePagePaginator:
        """Fetch every user in one request (the endpoint does not page)."""
        return SinglePagePaginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401, ARG002
    ) -> dict[str, t.Any]:
        """Include deactivated users so old call owners resolve."""
        return {"includeDeleted": "true"}
