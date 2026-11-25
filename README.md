<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A Query.Farm VGI worker for DuckDB.</em></p>

# Query PagerDuty Incidents, On-Call, Schedules and Services in DuckDB

> **vgi-pagerduty** · a [Query.Farm](https://query.farm) VGI worker · written in TypeScript

[![CI](https://github.com/Query-farm/vgi-pagerduty/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-pagerduty/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

PagerDuty incident management as SQL, read-only, through the
[PagerDuty REST API v2](https://developer.pagerduty.com/api-reference/): incidents with
time to acknowledge and resolve, each incident's timeline, alerts and notes, who is (or
was) on call, schedules and escalation chains, services and the business-service graph,
users and teams, deploys and other change events, maintenance windows, Analytics metrics
and the audit trail.

```sql
CREATE SECRET pagerduty (TYPE pagerduty, API_TOKEN 'u+abc…');   -- add REGION 'eu' for EU accounts

-- MTTA / MTTR per service for high-urgency incidents since October (exact, from each timeline).
SELECT service, count(*) AS incidents,
       median(time_to_ack_seconds) / 60     AS median_minutes_to_ack,
       median(time_to_resolve_seconds) / 60 AS median_minutes_to_resolve
FROM pagerduty.main.incidents(with_timeline := true)
WHERE created_at >= TIMESTAMPTZ '2026-10-01' AND urgency = 'high'
GROUP BY service ORDER BY incidents DESC;

-- Who is first on call right now, per escalation policy.
SELECT escalation_policy_name, user_name, end_time
FROM pagerduty.main.oncalls() WHERE escalation_level = 1;

-- Deploys on the same service in the hour before each incident.
SELECT i.incident_number, i.title, c.summary AS change, c.occurred_at
FROM pagerduty.main.incidents() i
JOIN pagerduty.main.change_events() c
  ON list_contains(c.service_ids, i.service_id)
 AND c.occurred_at BETWEEN i.created_at - INTERVAL 1 HOUR AND i.created_at;
```

## Functions and tables

All live in `pagerduty.main`. Tables take no arguments; table functions take a time window
(`since :=`, `until :=`, or a `WHERE` filter on their time column) or a parent id.

| Object | Kind | What it returns |
|---|---|---|
| `incidents(since, until, date_range, team_ids, user_ids, sort_by, with_timeline := false, page_size := 100)` | function | Incidents in the [incident vocabulary](#incident-vocabulary) plus incident_number, service / escalation policy / team ids, acknowledged_by, resolved_by, escalation_count, alert_count, incident_key |
| `incident_log_entries(incident_id, since, until, is_overview := false, team_ids)` | function | The timeline: trigger, acknowledge, assign, escalate, notify, annotate, resolve … with the agent who acted |
| `incident_alerts(incident_id, since, until)` | function | Alerts grouped into each incident: severity, status, alert_key, details JSON |
| `incident_notes(incident_id, since, until)` | function | Responder notes |
| `oncalls(since, until, earliest := false)` | function | Who is on call now (no arguments) or during a window, per policy and level |
| `schedule_entries(schedule_id, since, until)` | function | Rendered (final) schedule shifts: who was on call when |
| `change_events(since, until, team_ids, integration_ids)` | function | Recent deploys and other change events ("what changed") |
| `audit_records(since, until)` | function | Audit trail of changes to users, teams, schedules, policies and services (admin key; plan add-on) |
| `analytics_incidents(since, until)` | function | Analytics API per-incident metrics: time to ack / resolve / engage / mobilize, interruptions by business, off and sleep hours (plan add-on) |
| `analytics_incident_metrics(since, until, aggregate_unit, urgency, service_ids, team_ids)` | function | Analytics API aggregates (mean time to ack / resolve, interruptions, uptime), optionally per day / week / month |
| `services` | table | Technical services, status, escalation policy, teams, timeouts, urgency rule |
| `escalation_policies` | table | Policies, loops, rule count, services |
| `escalation_rules` | table | Every policy level flattened, one row per target (user or schedule) |
| `schedules` | table | Schedules, time zone, rotation users |
| `users` | table | Users, role, teams and **contact method types only** (email / phone / sms / push — never the addresses) |
| `teams`, `team_members` | tables | Teams (with parent team) and membership roles |
| `business_services`, `service_dependencies` | tables | Business services and the dependency graph edges (each edge once) |
| `maintenance_windows` | table | Past / ongoing / future maintenance windows |
| `priorities`, `tags` | tables | Incident priorities in rank order; tag labels |
| `status_pages` | table | Status pages (plan add-on) |
| `account_features` | table | Feature detection: which add-on APIs (Analytics, Audit Records, Status Pages …) this account and key can use |

Primary keys are attested on the tables, and the references between them are declared
foreign keys (`services` / `escalation_rules` → `escalation_policies`, `team_members` →
`teams` and `users`, `business_services` → `teams`).

### Incident vocabulary

Incident-shaped rows use one cross-worker vocabulary so PagerDuty incidents `UNION ALL`
cleanly with other incident sources (Sentry, Opsgenie, incident.io … workers follow the same
names and types):

| Column | Type | Meaning |
|---|---|---|
| `incident_id` | VARCHAR NOT NULL | The source system's incident id |
| `title` | VARCHAR | Incident title |
| `status` | VARCHAR NOT NULL | Normalized: `triggered`, `acknowledged` or `resolved` (`raw_status` keeps the source value) |
| `urgency` | VARCHAR | `high` / `low` |
| `priority` | VARCHAR | Priority name (P1 …) or NULL |
| `service` | VARCHAR | Name of the affected service |
| `created_at` | TIMESTAMPTZ | When it was triggered |
| `acknowledged_at` | TIMESTAMPTZ | First acknowledgement |
| `resolved_at` | TIMESTAMPTZ | Resolution (NULL while open) |
| `time_to_ack_seconds` | DOUBLE | `acknowledged_at - created_at` in seconds |
| `time_to_resolve_seconds` | DOUBLE | `resolved_at - created_at` in seconds |
| `assignees` | VARCHAR[] | Names of the current assignees |
| `url` | VARCHAR | Link to the incident in the web UI |

`incidents()` emits all of them; `analytics_incidents()` uses the same names for the ones
the Analytics API provides (`time_to_ack_seconds` is Analytics' `seconds_to_first_ack`).

**Acknowledgement times.** PagerDuty clears an incident's `acknowledgements` once it is
resolved, so plain `incidents()` only knows `acknowledged_at` for incidents that are
acknowledged right now (`timeline_source = 'incident'`). `with_timeline := true` reads each
incident's log entries (one extra call per incident) and gives the exact first
acknowledgement, resolution, `acknowledged_by`, `resolved_by` and `escalation_count`
(`timeline_source = 'log_entries'`). Use it, or `analytics_incidents()`, for MTTA / MTTR.

## What pushes down

`WHERE` filters are translated into PagerDuty's own query parameters and re-checked exactly
by the SDK, so translation only ever narrows the request.

| Object | Pushed down |
|---|---|
| `incidents` | `status`, `urgency`, `service_id` (=, IN) → `statuses[]`, `urgencies[]`, `service_ids[]`; `created_at` ranges → `since`/`until`; `incident_key =`; `incident_id = / IN` → `GET /incidents/{id}` |
| `incident_log_entries`, `incident_alerts`, `incident_notes` | `incident_id = / IN` → only those incidents; alerts `status` → `statuses[]`; log entries `created_at` → window |
| `oncalls` | `user_id`, `schedule_id`, `escalation_policy_id` (=, IN) → `user_ids[]`, `schedule_ids[]`, `escalation_policy_ids[]` |
| `schedule_entries` | `schedule_id = / IN` → only those schedules |
| `change_events` | `occurred_at` ranges → `since`/`until` |
| `audit_records` | `action`, `root_resource_type` (=, IN) → `actions[]`, `root_resource_types[]`; `actor_type`, `actor_id`, `method_type` (=); `execution_time` ranges → `since`/`until` |
| `analytics_incidents` | `urgency`, `major` (=), `service_id` / `team_id` / `priority_id` (=, IN) → request filters; `created_at` ranges → `created_at_start`/`created_at_end` |
| `services`, `escalation_policies`, `schedules`, `teams`, `tags`, `users` | `name =` (users also `email =`, tags `label =`) → `query` |
| `escalation_rules`, `team_members` | `escalation_policy_id` / `team_id` (=, IN) → only those parents |
| `maintenance_windows` | `status =` → `filter` (past / ongoing / future) |

**Time windows.** Windowed functions read the `since`/`until` arguments, intersected with
any pushed bound on their time column; with neither they read the **last 30 days** (audit
records: 7 days; schedule entries: 7 days back to 7 days ahead). `incidents(date_range :=
'all')` reads all time, and a status filter of only `triggered`/`acknowledged` with no time
bound reads every open incident. Windows longer than PagerDuty's limits are chunked
automatically: incidents, log entries and change events in 180-day pieces (PagerDuty's
6-month maximum), audit records in 31-day pieces, Analytics in 1-year pieces. On-calls
accept at most 90 days and `analytics_incident_metrics` at most 1 year (an error says so).

**`sort_by` + `LIMIT`.** `incidents(sort_by := 'created_at:desc') LIMIT 10` reads only the
newest page(s): DuckDB stops pulling batches once the limit is met.

## Authentication

```sql
-- A REST API key (Integrations > API Access Keys), sent as `Authorization: Token token=<key>`.
CREATE SECRET pagerduty (TYPE pagerduty, API_TOKEN 'u+abc…');

-- Or an OAuth 2.0 access token (sent as `Authorization: Bearer <token>`).
CREATE SECRET pagerduty (TYPE pagerduty, OAUTH_TOKEN 'pd_oauth_…');

-- EU service region (api.eu.pagerduty.com), or any base URL (https only; http for localhost).
CREATE SECRET pagerduty_eu (TYPE pagerduty, API_TOKEN '…', REGION 'eu');
CREATE SECRET pagerduty_fixture (TYPE pagerduty, API_TOKEN 'test-token', BASE_URL 'http://127.0.0.1:8765');
```

The key is read from the secret on every call and never stored in scan state. A read-only
key is enough for everything except `audit_records`, which needs an admin key on a plan with
Audit Trail. `SELECT * FROM pagerduty.main.account_features` probes the plan-dependent APIs
(Analytics, Audit Records, Status Pages, Business Services, Change Events) with one tiny
request each; a function whose API is not available fails with a message pointing there.

## Paging and rate limits

Classic `limit`/`offset`/`more` paging, the audit API's `cursor`/`next_cursor` and the
Analytics API's `starting_after`/`last` are all kept in serializable scan state, so a long
scan resumes on any worker process over HTTP. PagerDuty's offset paging stops at 10,000
rows: the first page of every window asks for `total`, and a window holding more than that
is halved until each piece fits. A 429 waits for `ratelimit-reset` / `Retry-After` (bounded
to a minute) and retries; 5xx and network errors back off exponentially; every request has
a 30 s timeout.

## Install

**Prerequisites:** [Bun](https://bun.sh) 1.3+ (for a source checkout).

```bash
git clone --branch <tag> https://github.com/Query-farm/vgi-pagerduty.git && cd vgi-pagerduty
bun install --frozen-lockfile
```

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
ATTACH 'pagerduty' (TYPE vgi, LOCATION '/abs/path/to/vgi-pagerduty/bin/vgi-pagerduty-worker');
```

**Haybarn 2.0.1+** (`uvx haybarn-cli`; Haybarn 2.0.0 serves an older vgi extension that cannot attach this worker):

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'pagerduty' (TYPE vgi, LOCATION '/abs/path/to/vgi-pagerduty/bin/vgi-pagerduty-worker');
```

`LOCATION` is the command DuckDB runs, so use an absolute path. Tagged releases ship signed
standalone executables (no Bun needed) for Linux, macOS (arm64) and Windows; point
`LOCATION` at the downloaded binary. The same binary serves HTTP with `--http --port 8000`
(see the `Dockerfile`).

## Testing

```bash
make test-unit                 # bun unit tests (windows, chunking, offset-cap splitting, paging, backoff)
make test-sql                  # test/sql/pagerduty.test through Haybarn 2.0 (stdio)
make test-sql TRANSPORT=http   # …over HTTP (also: launch)
make check                     # everything CI runs
```

No test touches the network or a real PagerDuty account. `test/mock_api/server.py` is a
stdlib-only, strict replay server over a hand-built account
(`test/fixtures/pagerduty-account.json`, expanded into PagerDuty's documented response
shapes). It enforces the `Authorization` and versioned `Accept` headers, rejects unknown
query parameters and bare (non-`[]`) array parameters, enforces `limit` maxima, the 10,000
offset cap and every endpoint's maximum date range, pages with `offset`/`more`/`total`,
audit `cursor`s and Analytics `starting_after`, answers a 429 and a 503 once each, and has a
`limited-token` without the add-on APIs. The SQL tests read its `/__requests` log to check
exactly what was pushed down.

The metadata check runs with `execute: false` in CI: every object needs a `pagerduty`
secret for a live account.

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm
