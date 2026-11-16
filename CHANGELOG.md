# Changelog

## 0.1.2 (2026-11-16)

- Bump @query-farm/vgi to 0.50.1.
- CI tooling (haybarn-unittest, lint helpers) runs on Python 3.14.

## 0.1.1

- Catalog and schema docs are overviews instead of object lists, and schemas have their own one-line comments (vgi-lint 2.8: VGI173, VGI102).

## 0.1.0

Initial release: PagerDuty REST API v2 (read-only) as SQL — incidents with time to
acknowledge/resolve, log entries, alerts, notes, on-calls, schedule entries, change events,
audit records, Analytics raw and aggregate metrics, and the services / escalation policies /
schedules / users / teams / business services / maintenance windows / priorities / tags /
status pages catalog tables, plus account_features feature detection.

Targets: @query-farm/vgi 0.50 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
