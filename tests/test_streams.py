"""Sync-mode, pagination and record-shape tests against a mocked API."""

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest
from singer_sdk.exceptions import ConfigValidationError

from tap_attention.tap import TapAttention

API = "https://api.attention.tech/v2"
CONFIG = {"api_key": "k", "start_date": "2026-01-01T00:00:00Z", "lookback_hours": 72}


def _conversation(cid, updated):
    return {
        "type": "conversations",
        "id": cid,
        "attributes": {
            "uuid": cid,
            "title": f"Call {cid}",
            "updatedAt": updated,
            "participants": [{"email": "a@example.com", "name": "A", "organizer": True}],
            "linkedCrmRecords": [{"id": "006X", "code": "Opportunity"}],
            "labels": {"Topic": ["Pricing"]},
            "scorecardResults": [{"uuid": "s1", "summary": {"summaryText": "ok"}}],
            "extractedIntelligence": {"k": {"value": "v", "options": {"values": ["v"]}}},
            "transcript": [
                {
                    "speaker": {"name": "A", "email": "a@example.com", "type": "internal"},
                    "words": [{"text": "Hi", "startTimestamp": 0.1, "endTimestamp": 0.3}],
                }
            ],
        },
    }


def _stream(state=None):
    tap = TapAttention(config=CONFIG, state=state or {})
    return tap.streams["conversations"]


def _query(request):
    return {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}


def test_first_sync_pages_by_number(requests_mock):
    url = f"{API}/conversations/list"
    requests_mock.get(
        url,
        [
            {
                "json": {
                    "data": [_conversation("c1", "2026-02-01T00:00:00Z")],
                    "meta": {"pageNumber": 1, "pageCount": 2},
                }
            },
            {
                "json": {
                    "data": [_conversation("c2", None)],
                    "meta": {"pageNumber": 2, "pageCount": 2},
                }
            },
        ],
    )
    records = list(_stream().get_records(None))

    assert [r["id"] for r in records] == ["c1", "c2"]
    first, second = (_query(r) for r in requests_mock.request_history)
    assert first["page"] == "1"
    assert second["page"] == "2"
    assert first["fromDateTime"] == "2026-01-01T00:00:00Z"
    assert "fromUpdatedAtDateTime" not in first
    assert first["detailedTranscript"] == "true"
    assert first["filter[include_internal_participants]"] == "true"


def _state(bookmark):
    return {
        "bookmarks": {
            "conversations": {"replication_key": "updatedAt", "replication_key_value": bookmark}
        }
    }


def test_incremental_sync_follows_cursor(requests_mock):
    url = f"{API}/conversations/list"
    requests_mock.get(
        url,
        [
            {"json": {"data": [_conversation("c1", "2026-02-01T00:00:00Z")], "nextCursor": "abc"}},
            {"json": {"data": [_conversation("c2", "2026-02-02T00:00:00Z")]}},
        ],
    )
    records = list(_stream(_state("2026-01-15T00:00:00Z")).get_records(None))

    assert [r["id"] for r in records] == ["c1", "c2"]
    first, second = (_query(r) for r in requests_mock.request_history)
    assert first["fromUpdatedAtDateTime"] == "2026-01-15T00:00:00Z"
    assert "cursor" not in first
    assert second["cursor"] == "abc"
    assert "page" not in first
    assert "fromDateTime" not in first


def test_recent_bookmark_rewinds_to_lookback():
    now = datetime.now(timezone.utc)
    stream = _stream(_state(now.strftime("%Y-%m-%dT%H:%M:%SZ")))
    since = stream._updated_since(None)  # noqa: SLF001
    assert abs(since - (now - timedelta(hours=72))) < timedelta(minutes=1)


def test_record_is_flattened(requests_mock):
    requests_mock.get(
        f"{API}/conversations/list",
        json={
            "data": [_conversation("c1", "2026-02-01T00:00:00Z")],
            "meta": {"pageNumber": 1, "pageCount": 1},
        },
    )
    record = next(iter(_stream().get_records(None)))

    assert record["id"] == "c1"
    assert record["title"] == "Call c1"
    assert record["linkedCrmRecords"] == [{"id": "006X", "code": "Opportunity"}]
    assert record["transcript"][0]["words"][0]["text"] == "Hi"
    assert "attributes" not in record


def test_full_sync_keeps_raw_json_fields(requests_mock, capsys):
    requests_mock.get(
        f"{API}/conversations/list",
        json={
            "data": [_conversation("c1", "2026-02-01T00:00:00Z")],
            "meta": {"pageNumber": 1, "pageCount": 1},
        },
    )
    requests_mock.get(f"{API}/users", json={"data": []})
    TapAttention(config=CONFIG).sync_all()

    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    (record,) = [m["record"] for m in messages if m["type"] == "RECORD"]
    assert record["labels"] == {"Topic": ["Pricing"]}
    assert record["scorecardResults"][0]["summary"] == {"summaryText": "ok"}
    assert record["extractedIntelligence"]["k"]["options"] == {"values": ["v"]}


def test_users_include_deleted(requests_mock):
    requests_mock.get(f"{API}/users", json={"data": [{"uuid": "u1", "email": "a@example.com"}]})
    tap = TapAttention(config=CONFIG)
    records = list(tap.streams["users"].get_records(None))

    assert records == [{"uuid": "u1", "email": "a@example.com"}]
    assert _query(requests_mock.request_history[0])["includeDeleted"] == "true"
    assert requests_mock.request_history[0].headers["Authorization"] == "Bearer k"


@pytest.mark.parametrize("missing", ["api_key", "lookback_hours"])
def test_required_settings(missing):
    config = {k: v for k, v in CONFIG.items() if k != missing}
    with pytest.raises(ConfigValidationError) as exc:
        TapAttention(config=config, validate_config=True)
    assert any(missing in e for e in exc.value.errors)
