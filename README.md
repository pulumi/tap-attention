# tap-attention

Singer tap for [Attention](https://attention.com) call recordings, built with the
[Meltano Singer SDK](https://sdk.meltano.com). It reads the
[Attention API v2](https://docs.attention.com/api-reference/openapi.json).

## Streams

| Stream | Endpoint | Key | Replication |
| --- | --- | --- | --- |
| `conversations` | `GET /conversations` | `id` | incremental on `updatedAt` |
| `users` | `GET /users?includeDeleted=true` | `uuid` | full table |

`conversations` holds call metadata, participants and attendees (internal ones included),
the detailed transcript (speaker turns with name, email and internal/external type, plus
word timings), linked CRM records, and Attention's scorecards and extracted fields as raw
JSON.

### Sync modes

- **First sync (no bookmark):** pages through every conversation by page number, optionally
  from `start_date` (call date). The `updatedAt` filter cannot be used here because it skips
  conversations that never got an `updatedAt`.
- **Later syncs:** request `updatedAt >= min(bookmark, now - lookback_hours)` and follow
  `nextCursor`. Attention does not bump `updatedAt` for CRM link or scorecard changes, so the
  lookback re-pulls recent calls. Deduplicate by `id` downstream.

## Settings

| Setting | Required | Description |
| --- | --- | --- |
| `api_key` | yes | Organization-level API key (Settings > API Keys, admin only) |
| `lookback_hours` | yes | Re-pull conversations updated within this many hours |
| `start_date` | no | Earliest call date for the first sync |
| `api_url` | no | Defaults to `https://api.attention.tech/v2` |

## Limitations

- Private conversations are excluded unless Attention enables `filter[include_private]` for
  the organization.
- Recording media URLs are not extracted (they are short-lived signed URLs).

## Development

```bash
uv sync
uv run pytest
uvx ruff check .
uv run tap-attention --config .secrets/config.json --discover
```
