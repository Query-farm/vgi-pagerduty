#!/usr/bin/env python3
"""A strict, offline PagerDuty REST API v2 replay server for the tests (stdlib only).

It serves the hand-built account in ``test/fixtures/pagerduty-account.json``, expanded
into PagerDuty's documented response shapes, and behaves like PagerDuty where the
worker depends on it:

* **Auth.** ``Authorization: Token token=test-token`` (full account),
  ``Token token=limited-token`` (no Analytics / Audit / Status Pages: 402 / 403) or
  ``Bearer test-oauth``. Anything else is a 401 (code 2006). The ``Accept`` header must
  ask for ``application/vnd.pagerduty+json;version=2``.
* **Strict parameters.** Each route accepts only its documented query parameters;
  array parameters must use the bracket form (``statuses[]=a&statuses[]=b``), so a bare
  ``statuses=`` is an unknown parameter. Unknown parameters, bad enum values, ``limit``
  above the maximum, ``offset + limit`` past 10,000 and over-long date ranges
  (incidents / log entries: 6 months; on-calls: 90 days; audit: 31 days; analytics:
  1 year) answer 400 with PagerDuty's ``{"error": {"message", "code", "errors"}}``.
* **Paging.** Classic ``limit`` / ``offset`` / ``more`` (and ``total`` only when
  ``total=true``); cursor paging (``cursor`` / ``next_cursor``) on ``/audit/records``;
  ``starting_after`` / ``last`` / ``more`` on the Analytics API.
* **Time windows.** Incidents and log entries default to the last 30 days unless
  ``date_range=all``; audit records to the last 24 hours. Schedules render a weekly
  rotation from ``rotation_epoch`` and clip entries to ``since``/``until``.
* **Rate limits.** The first ``/services`` request answers 429 with
  ``ratelimit-reset: 1`` and ``Retry-After: 1``; the first ``/priorities`` request
  answers 503. Both retry paths always run.
* **Introspection.** ``GET /__requests?path=/incidents`` (or ``prefix=``) returns every
  recorded request: ``seq``, ``method``, ``path``, ``query`` (JSON object string; repeated
  parameters become lists), ``body`` (JSON string for POST) and ``auth``.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as dt
import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

HERE = Path(__file__).resolve().parent
ACCOUNT = json.loads((HERE.parent / "fixtures" / "pagerduty-account.json").read_text())
API = "https://api.pagerduty.com"
WEB = "https://acme.pagerduty.com"
TOKENS = {"Token token=test-token": "full", "Token token=limited-token": "limited", "Bearer test-oauth": "full"}
OFFSET_CAP = 10_000
DAY = dt.timedelta(days=1)

LOCK = threading.Lock()
REQUESTS: list[dict] = []
ONCE: set[str] = set()


class ApiError(Exception):
    def __init__(self, status: int, code: int, message: str, errors: list[str] | None = None, headers: dict | None = None):
        super().__init__(message)
        self.status, self.code, self.message, self.errors, self.headers = status, code, message, errors or [], headers or {}


def bad(msg: str) -> ApiError:
    return ApiError(400, 2001, "Invalid Input Provided", [msg])


def parse_time(v: str | None, name: str) -> dt.datetime | None:
    if v is None:
        return None
    try:
        t = dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        raise bad(f"{name} is not a valid ISO 8601 date") from None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def ts(v: str | None) -> dt.datetime | None:
    return None if v is None else dt.datetime.fromisoformat(v.replace("Z", "+00:00"))


def iso(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# ------------------------------------------------------------------ references

USERS = {u["id"]: u for u in ACCOUNT["users"]}
TEAMS = {t["id"]: t for t in ACCOUNT["teams"]}
SERVICES = {s["id"]: s for s in ACCOUNT["services"]}
SCHEDULES = {s["id"]: s for s in ACCOUNT["schedules"]}
POLICIES = {p["id"]: p for p in ACCOUNT["escalation_policies"]}
PRIORITIES = {p["id"]: p for p in ACCOUNT["priorities"]}
BUSINESS = {b["id"]: b for b in ACCOUNT["business_services"]}
INCIDENTS = {i["id"]: i for i in ACCOUNT["incidents"]}

COLLECTION = {"user": "users", "team": "teams", "service": "services", "schedule": "schedules",
              "escalation_policy": "escalation_policies", "priority": "priorities", "business_service": "business_services"}
NAMES = {"user": USERS, "team": TEAMS, "service": SERVICES, "schedule": SCHEDULES, "escalation_policy": POLICIES,
         "priority": PRIORITIES, "business_service": BUSINESS}


def ref(kind: str, rid: str | None) -> dict | None:
    if rid is None:
        return None
    obj = NAMES.get(kind, {}).get(rid, {})
    coll = COLLECTION.get(kind, kind + "s")
    return {"id": rid, "type": f"{kind}_reference", "summary": obj.get("name", rid),
            "self": f"{API}/{coll}/{rid}", "html_url": f"{WEB}/{coll}/{rid}"}


def user_obj(u: dict) -> dict:
    return {"id": u["id"], "type": "user", "summary": u["name"], "name": u["name"], "email": u["email"],
            "role": u["role"], "job_title": u["job_title"], "time_zone": u["time_zone"], "description": u["description"],
            "invitation_sent": u["invitation_sent"], "color": "purple", "avatar_url": f"https://example.com/{u['id']}.png",
            "teams": [ref("team", t) for t in u["teams"]],
            # References only: summaries are labels, never the address/number.
            "contact_methods": [{"id": cid, "type": f"{kind}_contact_method_reference", "summary": "Default",
                                 "self": f"{API}/users/{u['id']}/contact_methods/{cid}", "html_url": None}
                                for cid, kind in u["contact_methods"]],
            "notification_rules": [], "self": f"{API}/users/{u['id']}", "html_url": f"{WEB}/users/{u['id']}"}


def team_obj(t: dict) -> dict:
    return {"id": t["id"], "type": "team", "summary": t["name"], "name": t["name"], "description": t["description"],
            "parent": ref("team", t["parent"]), "self": f"{API}/teams/{t['id']}", "html_url": f"{WEB}/teams/{t['id']}"}


def service_obj(s: dict) -> dict:
    return {"id": s["id"], "type": "service", "summary": s["name"], "name": s["name"], "description": s["description"],
            "status": s["status"], "escalation_policy": ref("escalation_policy", s["escalation_policy"]),
            "teams": [ref("team", t) for t in s["teams"]], "created_at": s["created_at"], "updated_at": s["updated_at"],
            "last_incident_timestamp": s["last_incident_timestamp"], "acknowledgement_timeout": s["acknowledgement_timeout"],
            "auto_resolve_timeout": s["auto_resolve_timeout"], "alert_creation": s["alert_creation"],
            "incident_urgency_rule": s["incident_urgency_rule"],
            "integrations": [{"id": i, "type": "generic_events_api_inbound_integration_reference", "summary": i} for i in s["integrations"]],
            "self": f"{API}/services/{s['id']}", "html_url": f"{WEB}/service-directory/{s['id']}"}


def policy_obj(p: dict) -> dict:
    return {"id": p["id"], "type": "escalation_policy", "summary": p["name"], "name": p["name"],
            "description": p["description"], "num_loops": p["num_loops"],
            "on_call_handoff_notifications": p["on_call_handoff_notifications"],
            "escalation_rules": [{"id": rid, "escalation_delay_in_minutes": delay,
                                  "targets": [ref(kind, tid) for kind, tid in targets]}
                                 for rid, delay, targets in p["rules"]],
            "services": [ref("service", s) for s in p["services"]], "teams": [ref("team", t) for t in p["teams"]],
            "self": f"{API}/escalation_policies/{p['id']}", "html_url": f"{WEB}/escalation_policies/{p['id']}"}


def schedule_obj(s: dict) -> dict:
    return {"id": s["id"], "type": "schedule", "summary": s["name"], "name": s["name"], "description": s["description"],
            "time_zone": s["time_zone"], "escalation_policies": [ref("escalation_policy", e) for e in s["escalation_policies"]],
            "users": [ref("user", u) for u in s["rotation"]], "teams": [ref("team", t) for t in s["teams"]],
            "self": f"{API}/schedules/{s['id']}", "html_url": f"{WEB}/schedules/{s['id']}"}


def rotation_entries(s: dict, since: dt.datetime, until: dt.datetime) -> list[dict]:
    """Weekly rotation from rotation_epoch, clipped to [since, until)."""
    epoch = ts(ACCOUNT["rotation_epoch"])
    step = dt.timedelta(days=ACCOUNT["rotation_days"])
    k = (since - epoch) // step
    out = []
    while True:
        start = epoch + k * step
        if start >= until:
            break
        end = start + step
        a, b = max(start, since), min(end, until)
        if a < b:
            out.append({"start": iso(a), "end": iso(b), "user": ref("user", s["rotation"][k % len(s["rotation"])])})
        k += 1
    return out


def rotation_shifts(s: dict, since: dt.datetime, until: dt.datetime) -> list[tuple]:
    """Whole (unclipped) shifts overlapping [since, until], or containing since when equal."""
    epoch = ts(ACCOUNT["rotation_epoch"])
    step = dt.timedelta(days=ACCOUNT["rotation_days"])
    k = (since - epoch) // step
    out = []
    while True:
        start = epoch + k * step
        if start > until or (start == until and since != until):
            break
        out.append((start, start + step, s["rotation"][k % len(s["rotation"])]))
        k += 1
    return out


def incident_obj(i: dict) -> dict:
    svc = SERVICES[i["service"]]
    log = i["log"]
    acks = [e for e in log if e[0] == "acknowledge"]
    status = i["status"]
    resolved = next((e for e in reversed(log) if e[0] == "resolve"), None)
    last = log[-1]
    return {
        "id": i["id"], "type": "incident", "summary": f"[#{i['number']}] {i['title']}", "incident_number": i["number"],
        "title": i["title"], "status": status, "urgency": i["urgency"], "incident_key": i["incident_key"],
        "created_at": i["created_at"], "updated_at": last[1],
        "service": ref("service", i["service"]),
        "escalation_policy": ref("escalation_policy", svc["escalation_policy"]),
        "teams": [ref("team", t) for t in svc["teams"]],
        "priority": None if i["priority"] is None else {**ref("priority", i["priority"]), "type": "priority",
                                                        "name": PRIORITIES[i["priority"]]["name"]},
        "assignments": [{"at": i["created_at"], "assignee": ref("user", u)} for u in i["assigned"]],
        # PagerDuty keeps acknowledgements only while the incident is acknowledged.
        "acknowledgements": [{"at": e[1], "acknowledger": ref("user", e[3])} for e in acks] if status == "acknowledged" else [],
        "last_status_change_at": last[1] if status != "triggered" else i["created_at"],
        "last_status_change_by": ref(last[2], last[3]),
        "resolved_at": resolved[1] if resolved and status == "resolved" else None,
        "alert_counts": {"all": len(i["alerts"]), "triggered": sum(a[2] == "triggered" for a in i["alerts"]),
                         "resolved": sum(a[2] == "resolved" for a in i["alerts"])},
        "first_trigger_log_entry": {"id": f"{i['id']}-L0", "type": "trigger_log_entry_reference"},
        "self": f"{API}/incidents/{i['id']}", "html_url": f"{WEB}/incidents/{i['id']}",
    }


def log_entries_of(i: dict) -> list[dict]:
    svc = SERVICES[i["service"]]
    out = []
    for n, e in enumerate(i["log"]):
        kind, at, agent_kind, agent_id = e[:4]
        extra = e[4] if len(e) > 4 else {}
        entry = {"id": f"{i['id']}-L{n}", "type": f"{kind}_log_entry", "created_at": at,
                 "summary": f"{kind.capitalize()}d by {NAMES.get(agent_kind, {}).get(agent_id, {}).get('name', agent_id)}",
                 "agent": ref(agent_kind, agent_id),
                 "channel": {"type": {"trigger": "api", "notify": "auto", "escalate": "timeout"}.get(kind, "web_ui")},
                 "service": ref("service", i["service"]), "teams": [ref("team", t) for t in svc["teams"]],
                 "incident": {"id": i["id"], "type": "incident_reference", "summary": i["title"],
                              "self": f"{API}/incidents/{i['id']}", "html_url": f"{WEB}/incidents/{i['id']}"},
                 "contexts": [], "self": f"{API}/log_entries/{i['id']}-L{n}", "html_url": None}
        if "assignees" in extra:
            entry["assignees"] = [ref("user", u) for u in extra["assignees"]]
        if "user" in extra:
            entry["user"] = ref("user", extra["user"])
            entry["notification"] = {"type": extra["notification"], "status": "success"}
        if "note" in extra:
            entry["channel"] = {"type": "note", "summary": extra["note"]}
        if kind == "trigger":
            entry["event_details"] = {"description": i["title"]}
        out.append(entry)
    return out


def alert_obj(i: dict, a: list) -> dict:
    aid, severity, status, key, summary, created = a
    return {"id": aid, "type": "alert", "summary": summary, "severity": severity, "status": status, "alert_key": key,
            "created_at": created, "suppressed": False, "service": ref("service", i["service"]),
            "incident": {"id": i["id"], "type": "incident_reference"},
            "integration": {"id": SERVICES[i["service"]]["integrations"][0] if SERVICES[i["service"]]["integrations"] else None,
                            "type": "generic_events_api_inbound_integration_reference"},
            "body": {"type": "alert_body", "details": {"source": "monitor", "key": key}, "contexts": []},
            "self": f"{API}/alerts/{aid}", "html_url": f"{WEB}/alerts/{aid}"}


# ------------------------------------------------------------------ helpers

def page(items: list, q: dict, key: str, max_limit: int = 100) -> dict:
    limit = int(one(q, "limit") or 25)
    offset = int(one(q, "offset") or 0)
    if limit < 1 or limit > max_limit:
        raise bad(f"limit must be between 1 and {max_limit}")
    if offset < 0 or offset + limit > OFFSET_CAP:
        raise bad(f"offset + limit must not exceed {OFFSET_CAP}")
    chunk = items[offset:offset + limit]
    total = len(items) if one(q, "total") == "true" else None
    return {key: chunk, "limit": limit, "offset": offset, "more": offset + len(chunk) < len(items), "total": total}


def one(q: dict, name: str) -> str | None:
    v = q.get(name)
    if v is None:
        return None
    if len(v) > 1:
        raise bad(f"{name} must be given once")
    return v[0]


def many(q: dict, name: str, allowed: set[str] | None = None) -> list[str] | None:
    v = q.get(f"{name}[]")
    if v is None:
        return None
    if allowed is not None:
        wrong = [x for x in v if x not in allowed]
        if wrong:
            raise bad(f"{name}[] has invalid value(s): {wrong}")
    return v


def window(q: dict, default: dt.timedelta, max_range: dt.timedelta, what: str) -> tuple[dt.datetime, dt.datetime]:
    since, until = parse_time(one(q, "since"), "since"), parse_time(one(q, "until"), "until")
    until = until or (since + default if since else now())
    since = since or until - default
    if until < since:
        raise bad("until must be after since")
    if until - since > max_range:
        raise bad(f"{what}: the date range cannot exceed {max_range.days} days")
    return since, until


def name_query(items: list[dict], q: dict, *fields: str) -> list[dict]:
    text = (one(q, "query") or "").lower()
    if not text:
        return items
    return [x for x in items if any(text in str(x.get(f) or "").lower() for f in fields)]


# ------------------------------------------------------------------ routes

COMMON = {"limit", "offset", "total"}


def r_incidents(q: dict, _m: list, tier: str) -> dict:
    statuses = many(q, "statuses", {"triggered", "acknowledged", "resolved"})
    urgencies = many(q, "urgencies", {"high", "low"})
    svc = many(q, "service_ids")
    team = many(q, "team_ids")
    users = many(q, "user_ids")
    sort = one(q, "sort_by") or "created_at:asc"
    m = re.fullmatch(r"(created_at|incident_number|resolved_at|urgency)(:(asc|desc))?", sort)
    if not m:
        raise bad(f"sort_by '{sort}' is not supported")
    date_range = one(q, "date_range")
    if date_range not in (None, "all"):
        raise bad("date_range must be 'all'")
    items = [incident_obj(i) for i in ACCOUNT["incidents"]]
    if date_range != "all":
        since, until = window(q, 30 * DAY, 183 * DAY, "incidents")
        items = [x for x in items if since <= ts(x["created_at"]) < until]
    if statuses:
        items = [x for x in items if x["status"] in statuses]
    if urgencies:
        items = [x for x in items if x["urgency"] in urgencies]
    if svc:
        items = [x for x in items if x["service"]["id"] in svc]
    if team:
        items = [x for x in items if any(t["id"] in team for t in x["teams"])]
    if users:
        items = [x for x in items if any(a["assignee"]["id"] in users for a in x["assignments"])]
    if one(q, "incident_key"):
        items = [x for x in items if x["incident_key"] == one(q, "incident_key")]
    field = m[1]
    items.sort(key=lambda x: (x.get(field) is None, str(x.get(field) or "")), reverse=m[3] == "desc")
    return page(items, q, "incidents")


def incident_or_404(iid: str) -> dict:
    if iid not in INCIDENTS:
        raise ApiError(404, 2100, "Not Found")
    return INCIDENTS[iid]


def r_incident(q: dict, m: list, tier: str) -> dict:
    return {"incident": incident_obj(incident_or_404(m[1]))}


OVERVIEW = {"trigger", "acknowledge", "unacknowledge", "resolve", "escalate", "assign"}


def filter_entries(entries: list[dict], q: dict) -> list[dict]:
    if one(q, "is_overview") == "true":
        entries = [e for e in entries if e["type"].removesuffix("_log_entry") in OVERVIEW]
    return entries


def r_incident_log(q: dict, m: list, tier: str) -> dict:
    i = incident_or_404(m[1])
    entries = filter_entries(log_entries_of(i), q)
    if one(q, "since") or one(q, "until"):
        since, until = window(q, 30 * DAY, 3650 * DAY, "log_entries")
        entries = [e for e in entries if since <= ts(e["created_at"]) < until]
    return page(entries, q, "log_entries")


def r_log_entries(q: dict, _m: list, tier: str) -> dict:
    since, until = window(q, 30 * DAY, 183 * DAY, "log_entries")
    team = many(q, "team_ids")
    entries = [e for i in ACCOUNT["incidents"] for e in log_entries_of(i)]
    entries = [e for e in filter_entries(entries, q) if since <= ts(e["created_at"]) < until]
    if team:
        entries = [e for e in entries if any(t["id"] in team for t in e["teams"])]
    entries.sort(key=lambda e: e["created_at"], reverse=True)
    return page(entries, q, "log_entries")


def r_alerts(q: dict, m: list, tier: str) -> dict:
    i = incident_or_404(m[1])
    statuses = many(q, "statuses", {"triggered", "resolved"})
    items = [alert_obj(i, a) for a in i["alerts"]]
    if statuses:
        items = [a for a in items if a["status"] in statuses]
    return page(items, q, "alerts")


def r_notes(q: dict, m: list, tier: str) -> dict:
    i = incident_or_404(m[1])
    return {"notes": [{"id": nid, "user": ref("user", uid), "channel": {"summary": "The PagerDuty website or APIs"},
                       "content": text, "created_at": at} for nid, uid, at, text in i["notes"]]}


def r_oncalls(q: dict, _m: list, tier: str) -> dict:
    tz = one(q, "time_zone")
    if tz not in (None, "UTC", "Etc/UTC"):
        raise bad("this fixture only renders UTC")
    since, until = parse_time(one(q, "since"), "since"), parse_time(one(q, "until"), "until")
    if since is None and until is None:
        since = until = now()
    since, until = since or until, until or since
    if until < since:
        raise bad("until must be after since")
    if until - since > 90 * DAY:
        raise bad("oncalls: the date range cannot exceed 90 days")
    users, scheds, eps = many(q, "user_ids"), many(q, "schedule_ids"), many(q, "escalation_policy_ids")
    out = []
    for p in ACCOUNT["escalation_policies"]:
        for level, (_rid, _delay, targets) in enumerate(p["rules"], start=1):
            for kind, tid in targets:
                if kind == "schedule":
                    for start, end, uid in rotation_shifts(SCHEDULES[tid], since, until):
                        out.append({"escalation_policy": ref("escalation_policy", p["id"]), "escalation_level": level,
                                    "schedule": ref("schedule", tid), "user": ref("user", uid),
                                    "start": iso(start), "end": iso(end)})
                else:
                    out.append({"escalation_policy": ref("escalation_policy", p["id"]), "escalation_level": level,
                                "schedule": None, "user": ref("user", tid), "start": None, "end": None})
    if users:
        out = [o for o in out if o["user"]["id"] in users]
    if scheds:
        out = [o for o in out if o["schedule"] and o["schedule"]["id"] in scheds]
    if eps:
        out = [o for o in out if o["escalation_policy"]["id"] in eps]
    if one(q, "earliest") == "true":
        seen, first = set(), []
        for o in out:
            k = (o["user"]["id"], o["escalation_policy"]["id"], o["escalation_level"])
            if k not in seen:
                seen.add(k)
                first.append(o)
        out = first
    return page(out, q, "oncalls")


def r_schedules(q: dict, _m: list, tier: str) -> dict:
    return page(name_query([schedule_obj(s) for s in ACCOUNT["schedules"]], q, "name"), q, "schedules")


def r_schedule(q: dict, m: list, tier: str) -> dict:
    s = SCHEDULES.get(m[1])
    if s is None:
        raise ApiError(404, 2100, "Not Found")
    if one(q, "time_zone") not in (None, "UTC", "Etc/UTC"):
        raise bad("this fixture only renders UTC")
    since, until = window(q, 14 * DAY, 3650 * DAY, "schedule")
    entries = rotation_entries(s, since, until)
    return {"schedule": {**schedule_obj(s), "final_schedule": {"name": "Final Schedule", "rendered_schedule_entries": entries,
                                                               "rendered_coverage_percentage": 100.0}}}


def r_services(q: dict, _m: list, tier: str) -> dict:
    items = name_query([service_obj(s) for s in ACCOUNT["services"]], q, "name")
    team = many(q, "team_ids")
    if team:
        items = [s for s in items if any(t["id"] in team for t in s["teams"])]
    return page(items, q, "services")


def r_policies(q: dict, _m: list, tier: str) -> dict:
    return page(name_query([policy_obj(p) for p in ACCOUNT["escalation_policies"]], q, "name"), q, "escalation_policies")


def r_policy(q: dict, m: list, tier: str) -> dict:
    if m[1] not in POLICIES:
        raise ApiError(404, 2100, "Not Found")
    return {"escalation_policy": policy_obj(POLICIES[m[1]])}


def r_users(q: dict, _m: list, tier: str) -> dict:
    if "contact_methods" in (q.get("include[]") or []):
        raise bad("this fixture refuses include[]=contact_methods: the worker must never read addresses")
    return page(name_query([user_obj(u) for u in ACCOUNT["users"]], q, "name", "email"), q, "users")


def r_teams(q: dict, _m: list, tier: str) -> dict:
    return page(name_query([team_obj(t) for t in ACCOUNT["teams"]], q, "name"), q, "teams")


def r_members(q: dict, m: list, tier: str) -> dict:
    t = TEAMS.get(m[1])
    if t is None:
        raise ApiError(404, 2100, "Not Found")
    return page([{"user": ref("user", u), "role": role} for u, role in t["members"]], q, "members")


def r_business(q: dict, _m: list, tier: str) -> dict:
    items = [{"id": b["id"], "type": "business_service", "summary": b["name"], "name": b["name"],
              "description": b["description"], "point_of_contact": b["point_of_contact"],
              "team": {"id": b["team"], "type": "team_reference", "self": f"{API}/teams/{b['team']}"},
              "self": f"{API}/business_services/{b['id']}", "html_url": f"{WEB}/business_services/{b['id']}"}
             for b in ACCOUNT["business_services"]]
    return page(items, q, "business_services")


def r_dependencies(kind: str):
    def handler(q: dict, m: list, tier: str) -> dict:
        table = BUSINESS if kind == "business_service" else SERVICES
        if m[1] not in table:
            raise ApiError(404, 2100, "Not Found")
        rels = [{"id": rid, "type": "service_dependency",
                 "dependent_service": {"id": did, "type": f"{dk}_reference"},
                 "supporting_service": {"id": sid, "type": f"{sk}_reference"}}
                for rid, dk, did, sk, sid in ACCOUNT["dependencies"]
                if (dk == kind and did == m[1]) or (sk == kind and sid == m[1])]
        return {"relationships": rels}
    return handler


def r_maintenance(q: dict, _m: list, tier: str) -> dict:
    f = one(q, "filter") or "open"
    if f not in {"past", "future", "ongoing", "open", "all"}:
        raise bad("filter must be past, future, ongoing, open or all")
    t = now()
    items = []
    for w in ACCOUNT["maintenance_windows"]:
        s, e = ts(w["start_time"]), ts(w["end_time"])
        state = "past" if e <= t else "future" if s > t else "ongoing"
        if f == "all" or f == state or (f == "open" and state != "past"):
            items.append({"id": w["id"], "type": "maintenance_window", "summary": w["description"],
                          "sequence_number": w["sequence_number"], "description": w["description"],
                          "start_time": w["start_time"], "end_time": w["end_time"], "created_by": ref("user", w["created_by"]),
                          "services": [ref("service", s) for s in w["services"]], "teams": [ref("team", x) for x in w["teams"]],
                          "self": f"{API}/maintenance_windows/{w['id']}", "html_url": f"{WEB}/maintenance_windows/{w['id']}"})
    return page(items, q, "maintenance_windows")


def r_priorities(q: dict, _m: list, tier: str) -> dict:
    return page([{"id": p["id"], "type": "priority", "summary": p["name"], "name": p["name"], "description": p["description"],
                  "self": f"{API}/priorities/{p['id']}"} for p in ACCOUNT["priorities"]], q, "priorities")


def r_tags(q: dict, _m: list, tier: str) -> dict:
    items = [{"id": t["id"], "type": "tag", "summary": t["label"], "label": t["label"], "self": f"{API}/tags/{t['id']}"}
             for t in ACCOUNT["tags"]]
    return page(name_query(items, q, "label"), q, "tags")


def r_change_events(q: dict, _m: list, tier: str) -> dict:
    since, until = window(q, 30 * DAY, 3650 * DAY, "change_events")
    team, integ = many(q, "team_ids"), many(q, "integration_ids")
    items = []
    for c in ACCOUNT["change_events"]:
        t = ts(c["timestamp"])
        if not (since <= t < until):
            continue
        if team and not any(t2 in team for s in c["services"] for t2 in SERVICES[s]["teams"]):
            continue
        if integ and c["integration"] not in integ:
            continue
        items.append({"id": c["id"], "type": "change_event", "summary": c["summary"], "timestamp": c["timestamp"],
                      "source": c["source"], "services": [ref("service", s) for s in c["services"]],
                      "integration": {"id": c["integration"], "type": "inbound_integration_reference"},
                      "links": [{"href": h, "text": x} for h, x in c["links"]], "custom_details": c["custom_details"]})
    items.sort(key=lambda c: c["timestamp"], reverse=True)
    return page(items, q, "change_events")


def r_status_pages(q: dict, _m: list, tier: str) -> dict:
    if tier != "full":
        raise ApiError(402, 2012, "Status Pages are not enabled for this account")
    return page([{**s, "type": "status_page"} for s in ACCOUNT["status_pages"]], q, "status_pages")


AUDIT_TYPES = {"users", "teams", "schedules", "escalation_policies", "services"}
AUDIT_REF = {"user_reference": "users", "team_reference": "teams", "schedule_reference": "schedules",
             "escalation_policy_reference": "escalation_policies", "service_reference": "services"}


def r_audit(q: dict, _m: list, tier: str) -> dict:
    if tier != "full":
        raise ApiError(403, 2010, "Access Denied", ["Audit records require an admin key on a plan with Audit Trail"])
    since, until = window(q, DAY, 31 * DAY, "audit records")
    limit = int(one(q, "limit") or 10)
    if limit < 1 or limit > 100:
        raise bad("limit must be between 1 and 100")
    actions = many(q, "actions", {"create", "update", "delete"})
    types = many(q, "root_resource_types", AUDIT_TYPES)
    actor_type, actor_id, method = one(q, "actor_type"), one(q, "actor_id"), one(q, "method_type")
    if actor_type not in (None, "user_reference", "api_key_reference", "app_reference"):
        raise bad("actor_type is invalid")
    if actor_id and not actor_type:
        raise bad("actor_id requires actor_type")
    items = []
    for r in ACCOUNT["audit_records"]:
        t = ts(r["execution_time"])
        if not (since <= t < until):
            continue
        if actions and r["action"] not in actions:
            continue
        if types and AUDIT_REF[r["root"][0]] not in types:
            continue
        if actor_type and r["actor"][0] != actor_type:
            continue
        if actor_id and r["actor"][1] != actor_id:
            continue
        if method and r["method"][0] != method:
            continue
        items.append({"id": r["id"], "self": f"{API}/audit/records/{r['id']}", "execution_time": r["execution_time"],
                      "execution_context": {"request_id": f"req-{r['id']}", "remote_address": r["remote_address"]},
                      "actors": [{"id": r["actor"][1], "type": r["actor"][0], "summary": r["actor"][2]}],
                      "method": {"type": r["method"][0], "description": r["method"][1], "truncated_token": r["method"][2]},
                      "root_resource": {"id": r["root"][1], "type": r["root"][0], "summary": r["root"][2]},
                      "action": r["action"],
                      "details": {"resource": {"id": r["root"][1], "type": r["root"][0], "summary": r["root"][2]},
                                  "fields": [{"name": n, "before_value": b, "value": v} for n, b, v in r["fields"]]}})
    items.sort(key=lambda r: r["execution_time"], reverse=True)
    start = 0
    cur = one(q, "cursor")
    if cur:
        try:
            start = int(base64.urlsafe_b64decode(cur.encode()).decode())
        except ValueError:
            raise bad("cursor is invalid") from None
    chunk = items[start:start + limit]
    nxt = base64.urlsafe_b64encode(str(start + limit).encode()).decode() if start + limit < len(items) else None
    return {"records": chunk, "next_cursor": nxt, "limit": limit, "response_metadata": None}


# ------------------------------------------------------------------ analytics

def raw_metrics(i: dict) -> dict:
    log = i["log"]
    created = ts(i["created_at"])
    first = lambda kind: next((ts(e[1]) for e in log if e[0] == kind), None)  # noqa: E731
    ack, res = first("acknowledge"), next((ts(e[1]) for e in reversed(log) if e[0] == "resolve"), None)
    notifies = [ts(e[1]) for e in log if e[0] == "notify"]
    sleep = sum(1 for t in notifies if t.hour >= 22 or t.hour < 6)
    business = sum(1 for t in notifies if t.weekday() < 5 and 9 <= t.hour < 17)
    svc = SERVICES[i["service"]]
    team = svc["teams"][0]
    return {"id": i["id"], "incident_number": i["number"], "description": i["title"], "service_id": svc["id"],
            "service_name": svc["name"], "team_id": team, "team_name": TEAMS[team]["name"],
            "escalation_policy_id": svc["escalation_policy"], "created_at": i["created_at"],
            "resolved_at": iso(res) if res and i["status"] == "resolved" else None, "urgency": i["urgency"],
            "major": i["priority"] == "PPRI1", "priority_id": i["priority"],
            "priority_name": PRIORITIES[i["priority"]]["name"] if i["priority"] else None,
            "seconds_to_first_ack": (ack - created).total_seconds() if ack else None,
            "seconds_to_resolve": (res - created).total_seconds() if res and i["status"] == "resolved" else None,
            "seconds_to_engage": (ack - created).total_seconds() if ack else None, "seconds_to_mobilize": None,
            "engaged_seconds": (res - ack).total_seconds() if res and ack else None,
            "engaged_user_count": len({e[3] for e in log if e[2] == "user"}),
            "escalation_count": sum(1 for e in log if e[0] == "escalate"),
            "assignment_count": sum(1 for e in log if e[0] == "assign") + len(i["assigned"]),
            "total_notifications": len(notifies), "total_interruptions": len(notifies),
            "business_hour_interruptions": business, "sleep_hour_interruptions": sleep,
            "off_hour_interruptions": len(notifies) - business - sleep}


RAW_FILTERS = {"created_at_start", "created_at_end", "urgency", "major", "service_ids", "team_ids", "priority_ids",
               "priority_names", "incident_type"}


def analytics_selection(body: dict, tier: str, allowed_top: set[str]) -> list[dict]:
    if tier != "full":
        raise ApiError(402, 2013, "Analytics is not available on this account's plan")
    unknown = set(body) - allowed_top
    if unknown:
        raise bad(f"unknown body field(s): {sorted(unknown)}")
    f = body.get("filters") or {}
    if set(f) - RAW_FILTERS:
        raise bad(f"unknown filter(s): {sorted(set(f) - RAW_FILTERS)}")
    start, end = parse_time(f.get("created_at_start"), "created_at_start"), parse_time(f.get("created_at_end"), "created_at_end")
    if start is None or end is None:
        raise bad("filters.created_at_start and filters.created_at_end are required")
    if end - start > 366 * DAY:
        raise bad("the created_at range cannot exceed 1 year")
    rows = [raw_metrics(i) for i in ACCOUNT["incidents"] if start <= ts(i["created_at"]) < end]
    if f.get("urgency"):
        rows = [r for r in rows if r["urgency"] == f["urgency"]]
    if "major" in f:
        rows = [r for r in rows if r["major"] == f["major"]]
    for key, col in (("service_ids", "service_id"), ("team_ids", "team_id"), ("priority_ids", "priority_id")):
        if f.get(key):
            rows = [r for r in rows if r[col] in f[key]]
    return rows


def r_raw_incidents(body: dict, tier: str) -> dict:
    rows = analytics_selection(body, tier, {"filters", "limit", "starting_after", "ending_before", "order", "order_by", "time_zone"})
    limit = int(body.get("limit", 20))
    if limit < 1 or limit > 1000:
        raise bad("limit must be between 1 and 1000")
    if body.get("order_by", "created_at") != "created_at" or body.get("order", "desc") not in ("asc", "desc"):
        raise bad("order_by must be created_at; order asc or desc")
    rows.sort(key=lambda r: (r["created_at"], r["id"]), reverse=body.get("order", "desc") == "desc")
    start = 0
    if body.get("starting_after"):
        ids = [r["id"] for r in rows]
        if body["starting_after"] not in ids:
            raise bad("starting_after is not a valid cursor")
        start = ids.index(body["starting_after"]) + 1
    chunk = rows[start:start + limit]
    return {"data": chunk, "more": start + limit < len(rows), "first": chunk[0]["id"] if chunk else None,
            "last": chunk[-1]["id"] if chunk else None, "limit": limit, "order": body.get("order", "desc"),
            "order_by": "created_at", "starting_after": body.get("starting_after"), "filters": body.get("filters")}


def mean(xs: list) -> float | None:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def r_metrics(body: dict, tier: str) -> dict:
    rows = analytics_selection(body, tier, {"filters", "aggregate_unit", "time_zone"})
    unit = body.get("aggregate_unit")
    if unit not in (None, "day", "week", "month"):
        raise bad("aggregate_unit must be day, week or month")

    def bucket(r: dict) -> str | None:
        t = ts(r["created_at"])
        if unit == "day":
            return iso(t.replace(hour=0, minute=0, second=0))
        if unit == "week":
            d = t.replace(hour=0, minute=0, second=0) - dt.timedelta(days=t.weekday())
            return iso(d)
        if unit == "month":
            return iso(t.replace(day=1, hour=0, minute=0, second=0))
        return None

    groups: dict = {}
    for r in rows:
        groups.setdefault(bucket(r), []).append(r)
    data = []
    for key in sorted(groups, key=lambda k: k or ""):
        g = groups[key]
        data.append({"range_start": key, "total_incident_count": len(g),
                     "mean_seconds_to_first_ack": mean([r["seconds_to_first_ack"] for r in g]),
                     "mean_seconds_to_resolve": mean([r["seconds_to_resolve"] for r in g]),
                     "mean_seconds_to_engage": mean([r["seconds_to_engage"] for r in g]), "mean_seconds_to_mobilize": None,
                     "total_interruptions": sum(r["total_interruptions"] for r in g),
                     "total_business_hour_interruptions": sum(r["business_hour_interruptions"] for r in g),
                     "total_off_hour_interruptions": sum(r["off_hour_interruptions"] for r in g),
                     "total_sleep_hour_interruptions": sum(r["sleep_hour_interruptions"] for r in g),
                     "total_notifications": sum(r["total_notifications"] for r in g),
                     "total_escalation_count": sum(r["escalation_count"] for r in g),
                     "mean_assignment_count": mean([r["assignment_count"] for r in g]), "up_time_pct": None})
    return {"data": data, "aggregate_unit": unit, "filters": body.get("filters"), "time_zone": body.get("time_zone")}


ID = r"([A-Za-z0-9_-]+)"
ROUTES = [
    (r"/incidents", r_incidents, COMMON | {"statuses[]", "urgencies[]", "service_ids[]", "team_ids[]", "user_ids[]", "since",
                                           "until", "date_range", "sort_by", "incident_key", "time_zone", "include[]"}),
    (rf"/incidents/{ID}", r_incident, {"include[]"}),
    (rf"/incidents/{ID}/log_entries", r_incident_log, COMMON | {"time_zone", "since", "until", "is_overview", "include[]"}),
    (rf"/incidents/{ID}/alerts", r_alerts, COMMON | {"statuses[]", "alert_key", "sort_by", "include[]"}),
    (rf"/incidents/{ID}/notes", r_notes, set()),
    (r"/log_entries", r_log_entries, COMMON | {"time_zone", "since", "until", "is_overview", "include[]", "team_ids[]"}),
    (r"/oncalls", r_oncalls, COMMON | {"time_zone", "include[]", "user_ids[]", "escalation_policy_ids[]", "schedule_ids[]",
                                       "since", "until", "earliest"}),
    (r"/schedules", r_schedules, COMMON | {"query", "include[]", "time_zone"}),
    (rf"/schedules/{ID}", r_schedule, {"since", "until", "time_zone", "overflow"}),
    (r"/services", r_services, COMMON | {"query", "team_ids[]", "include[]", "sort_by", "time_zone"}),
    (r"/escalation_policies", r_policies, COMMON | {"query", "user_ids[]", "team_ids[]", "include[]", "sort_by"}),
    (rf"/escalation_policies/{ID}", r_policy, {"include[]"}),
    (r"/users", r_users, COMMON | {"query", "team_ids[]", "include[]"}),
    (r"/teams", r_teams, COMMON | {"query"}),
    (rf"/teams/{ID}/members", r_members, COMMON | {"include[]"}),
    (r"/business_services", r_business, COMMON),
    (rf"/service_dependencies/business_services/{ID}", r_dependencies("business_service"), set()),
    (rf"/service_dependencies/technical_services/{ID}", r_dependencies("service"), set()),
    (r"/maintenance_windows", r_maintenance, COMMON | {"query", "team_ids[]", "service_ids[]", "filter", "include[]"}),
    (r"/priorities", r_priorities, COMMON),
    (r"/tags", r_tags, COMMON | {"query"}),
    (r"/change_events", r_change_events, COMMON | {"since", "until", "team_ids[]", "integration_ids[]"}),
    (r"/status_pages", r_status_pages, COMMON),
    (r"/audit/records", r_audit, {"limit", "cursor", "since", "until", "root_resource_types[]", "actor_type", "actor_id",
                                   "method_type", "method_truncated_token", "actions[]"}),
]
POST_ROUTES = {"/analytics/raw/incidents": r_raw_incidents, "/analytics/metrics/incidents/all": r_metrics}
ONCE_FAIL = {"/services": 429, "/priorities": 503}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("pagerduty_mock: " + (format % args) + "\n")

    def _send(self, status: int, body: object, headers: dict | None = None) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for k, v in {"ratelimit-limit": "960", "ratelimit-remaining": "950", "ratelimit-reset": "30", **(headers or {})}.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def _error(self, e: ApiError) -> None:
        self._send(e.status, {"error": {"message": e.message, "code": e.code, "errors": e.errors}}, e.headers)

    def _auth(self) -> str:
        tier = TOKENS.get(self.headers.get("Authorization", ""))
        if tier is None:
            raise ApiError(401, 2006, "Invalid Credentials")
        if "application/vnd.pagerduty+json;version=2" not in self.headers.get("Accept", ""):
            raise ApiError(406, 2001, "Accept must be application/vnd.pagerduty+json;version=2")
        return tier

    def _record(self, method: str, path: str, raw: dict, body: object) -> None:
        auth = self.headers.get("Authorization", "")
        kind = "token" if auth.startswith("Token ") else "bearer" if auth.startswith("Bearer ") else "none"
        query = {k: (v[0] if len(v) == 1 else v) for k, v in raw.items()}
        with LOCK:
            REQUESTS.append({"seq": len(REQUESTS) + 1, "method": method, "path": path,
                             "query": json.dumps(query, sort_keys=True),
                             "body": json.dumps(body, sort_keys=True) if body is not None else None, "auth": kind})

    def _once(self, path: str) -> None:
        status = ONCE_FAIL.get(path)
        if status is None:
            return
        with LOCK:
            first = path not in ONCE
            ONCE.add(path)
        if first and status == 429:
            raise ApiError(429, 2020, "Rate Limit Exceeded", headers={"ratelimit-remaining": "0", "ratelimit-reset": "1",
                                                                       "Retry-After": "1"})
        if first:
            raise ApiError(503, 2000, "Service Unavailable")

    def do_GET(self) -> None:
        url = urlparse(self.path)
        raw = parse_qs(url.query, keep_blank_values=True)
        if url.path == "/health":
            self._send(200, {"status": "ok"})
            return
        if url.path == "/__requests":
            p, pre = (raw.get("path") or [None])[0], (raw.get("prefix") or [None])[0]
            with LOCK:
                rows = [r for r in REQUESTS if (not p or r["path"] == p) and (not pre or r["path"].startswith(pre))]
            self._send(200, rows)
            return
        try:
            for pattern, fn, allowed in ROUTES:
                m = re.fullmatch(pattern, url.path)
                if not m:
                    continue
                self._record("GET", url.path, raw, None)
                tier = self._auth()
                unknown = set(raw) - allowed
                if unknown:
                    raise bad(f"unknown query parameter(s): {sorted(unknown)}")
                self._once(url.path)
                self._send(200, fn(raw, [m[0], *(unquote(g) for g in m.groups())], tier))
                return
            raise ApiError(404, 2100, f"Not Found: {url.path}")
        except ApiError as e:
            self._error(e)

    def do_POST(self) -> None:
        url = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        text = self.rfile.read(length).decode() if length else ""
        try:
            fn = POST_ROUTES.get(url.path)
            if fn is None:
                raise ApiError(404, 2100, f"Not Found: {url.path}")
            try:
                body = json.loads(text) if text else {}
            except ValueError:
                raise bad("body is not JSON") from None
            self._record("POST", url.path, parse_qs(url.query), body)
            tier = self._auth()
            if url.query:
                raise bad("the Analytics API takes no query parameters")
            if "application/json" not in self.headers.get("Content-Type", ""):
                raise bad("Content-Type must be application/json")
            self._send(200, fn(body, tier))
        except ApiError as e:
            self._error(e)


def main() -> None:
    """Serve until interrupted, printing ``PORT:<n>`` once bound."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--port", type=int, default=0, help="port to bind (default: any free port)")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
