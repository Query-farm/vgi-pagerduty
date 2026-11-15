/**
 * The PagerDuty table functions and tables: one spec per REST API v2 endpoint.
 *
 * Incident-shaped rows share a cross-worker vocabulary (see README "Incident
 * vocabulary"): incident_id, title, status (triggered / acknowledged / resolved),
 * urgency, priority, service, created_at, acknowledged_at, resolved_at,
 * time_to_ack_seconds, time_to_resolve_seconds, assignees, url.
 */

import { bool, int64, list, timestampMicros, utf8, type VgiFunction } from "@query-farm/vgi";
import { type PdConfig, PdError, pdRequest, type Query } from "./client.ts";
import { type Constraint, eqValues, timeBounds, toEpochMs } from "./pushdown.ts";
import {
  asStr,
  at,
  C,
  type Col,
  chunkWindow,
  definePdCatalogTable,
  definePdTable,
  epochMs,
  getAll,
  iso,
  type Phase,
  pluck,
  type Row,
  type ScanContext,
  secondsBetween,
} from "./table.ts";

const Q = "pagerduty.main";
const DAY = 86_400_000;
const enc = encodeURIComponent;
const TS = () => timestampMicros("UTC");
const STR_LIST = () => list(utf8());

/** PagerDuty's documented maximum date ranges. */
export const MAX_INCIDENT_WINDOW_MS = 180 * DAY;
export const MAX_AUDIT_WINDOW_MS = 31 * DAY;
export const MAX_ANALYTICS_WINDOW_MS = 365 * DAY;
export const MAX_ONCALL_WINDOW_MS = 90 * DAY;
export const DEFAULT_LOOKBACK_MS = 30 * DAY;

export const INCIDENT_STATUSES = ["triggered", "acknowledged", "resolved"];
const URGENCIES = ["high", "low"];
const PAGE_SIZE_DOC = "Rows requested per PagerDuty page (named; at most 100).";
const SINCE_DOC = "Start of the time window (inclusive). Defaults from a pushed time filter, else 30 days ago.";
const UNTIL_DOC = "End of the time window (exclusive). Defaults from a pushed time filter, else now.";

const pageSize = (v: unknown, max = 100) => Math.max(1, Math.min(Number(v ?? max), max));
/** Drop PagerDuty's `_reference` / `_log_entry` suffixes from a type. */
const bare = (v: unknown, suffix: string) => (typeof v === "string" ? v.replace(new RegExp(`${suffix}$`), "") : null);
/** Repeat an array parameter (`statuses[]=a&statuses[]=b`). */
const arr = (name: string, values: readonly string[] | null | undefined): Query =>
  (values ?? []).map((v) => [`${name}[]`, v] as [string, string]);
/** A VARCHAR[] argument (may arrive as an Arrow vector or array). */
function listArg(v: unknown): string[] | undefined {
  if (v == null) return undefined;
  const items = Array.isArray(v)
    ? v
    : typeof (v as { toArray?: unknown }).toArray === "function"
      ? [...(v as Iterable<unknown>)]
      : [];
  const out = items.filter((x): x is string => typeof x === "string" && x !== "");
  return out.length ? out : undefined;
}
/** Pushed `= / IN` values limited to an allowed set (others cannot match a valid API value). */
function allowed(cs: Constraint[], column: string, values: readonly string[]): string[] | undefined {
  const vs = eqValues(cs, column);
  return vs ? vs.filter((v) => values.includes(v)) : undefined;
}

/**
 * The scan's time window: the since/until arguments intersected with pushed bounds on
 * `column`; a missing upper bound is the scan's start, a missing lower bound
 * `lookbackMs` before the upper bound. Undefined when the window is empty.
 */
export function windowOf(
  scan: ScanContext<Record<string, unknown>>,
  column: string,
  lookbackMs = DEFAULT_LOOKBACK_MS,
): { lo: number; hi: number } | undefined {
  const b = timeBounds(scan.constraints, column);
  const since = toEpochMs(scan.args.since);
  const until = toEpochMs(scan.args.until);
  const lows = [since, b.lower].filter((x): x is number => x !== undefined);
  const highs = [until, b.upper].filter((x): x is number => x !== undefined);
  const hi = highs.length ? Math.min(...highs) : scan.anchorMs;
  const lo = lows.length ? Math.max(...lows) : hi - lookbackMs;
  return lo < hi ? { lo, hi } : undefined;
}

/** One phase per chunk of a window that the endpoint caps at `maxMs`. */
function windowPhases(
  base: Omit<Phase, "window">,
  w: { lo: number; hi: number },
  maxMs: number,
  desc = false,
): Phase[] {
  return chunkWindow(w.lo, w.hi, maxMs, desc).map(([lo, hi]) => ({
    ...base,
    window: { lo, hi, style: "query", desc },
  }));
}

// ---------------------------------------------------------------- incidents

/** Normalized incident status (PagerDuty's three, lower case). */
export function normStatus(v: unknown): string | null {
  if (typeof v !== "string") return null;
  return v.toLowerCase();
}

/** Earliest `at` of a list of acknowledgements/assignments. */
function earliestAt(v: unknown): string | null {
  if (!Array.isArray(v)) return null;
  let best: string | null = null;
  for (const x of v) {
    const t = typeof x?.at === "string" ? x.at : null;
    if (t && (best === null || Date.parse(t) < Date.parse(best))) best = t;
  }
  return best;
}

interface Timeline {
  ack: string | null;
  ackBy: string | null;
  resolve: string | null;
  resolveBy: string | null;
  escalations: number;
}

/** First acknowledgement and last resolution in an incident's log entries. */
export function timelineOf(entries: Row[]): Timeline {
  const t: Timeline = { ack: null, ackBy: null, resolve: null, resolveBy: null, escalations: 0 };
  const sorted = [...entries].sort((a, b) => Date.parse(a.created_at) - Date.parse(b.created_at));
  for (const e of sorted) {
    const type = bare(e.type, "_log_entry");
    if (type === "acknowledge" && t.ack === null) {
      t.ack = e.created_at;
      t.ackBy = asStr(at(e, "agent.summary"));
    } else if (type === "resolve") {
      t.resolve = e.created_at;
      t.resolveBy = asStr(at(e, "agent.summary"));
    } else if (type === "escalate") t.escalations += 1;
  }
  return t;
}

const ackAt = (r: Row): string | null => (r.__timeline ? r.__timeline.ack : (earliestAt(r.acknowledgements) ?? null));
const resolvedAt = (r: Row): string | null => {
  if (r.__timeline) return r.__timeline.resolve;
  if (typeof r.resolved_at === "string") return r.resolved_at;
  return normStatus(r.status) === "resolved" ? (r.last_status_change_at ?? null) : null;
};

function incidentColumns(): Col[] {
  return [
    C.str("incident_id", "PagerDuty incident id (e.g. Q1ABCDEF2GHIJK).", "id", false),
    C.big("incident_number", "The account-wide incident number shown in the PagerDuty UI.", "incident_number"),
    C.str("title", "Incident title.", "title"),
    C.str("status", "Normalized status: triggered, acknowledged or resolved.", (r) => normStatus(r.status), false),
    C.str("raw_status", "The status exactly as PagerDuty returned it.", "status"),
    C.str("urgency", "Urgency: high or low.", "urgency"),
    C.str(
      "priority",
      "Priority name (e.g. P1), or NULL when none is set.",
      (r) => at(r, "priority.summary") ?? at(r, "priority.name"),
    ),
    C.str("priority_id", "Priority id.", "priority.id"),
    C.str("service", "Name of the service the incident is on.", "service.summary"),
    C.str("service_id", "Service id.", "service.id"),
    C.str("escalation_policy", "Escalation policy name.", "escalation_policy.summary"),
    C.str("escalation_policy_id", "Escalation policy id.", "escalation_policy.id"),
    C.strList("team_ids", "Ids of the teams the incident belongs to.", (r) => pluck(r.teams, "id")),
    C.strList("teams", "Names of the teams the incident belongs to.", (r) => pluck(r.teams, "summary")),
    C.time("created_at", "When the incident was triggered.", "created_at"),
    C.time("updated_at", "When the incident last changed.", "updated_at"),
    C.time(
      "acknowledged_at",
      "First acknowledgement: from the log entries with with_timeline := true, else the current acknowledgements (cleared once resolved).",
      ackAt,
    ),
    C.time("resolved_at", "When the incident was resolved (NULL while open).", resolvedAt),
    C.num("time_to_ack_seconds", "Seconds from created_at to acknowledged_at.", (r) =>
      secondsBetween(r.created_at, ackAt(r)),
    ),
    C.num("time_to_resolve_seconds", "Seconds from created_at to resolved_at.", (r) =>
      secondsBetween(r.created_at, resolvedAt(r)),
    ),
    C.str(
      "acknowledged_by",
      "Who acknowledged first (log entries with with_timeline := true, else current acknowledgers).",
      (r) => (r.__timeline ? r.__timeline.ackBy : at(r, "acknowledgements.0.acknowledger.summary")),
    ),
    C.str(
      "resolved_by",
      "Who resolved it (with_timeline := true), else the last status changer of a resolved incident.",
      (r) =>
        r.__timeline
          ? r.__timeline.resolveBy
          : normStatus(r.status) === "resolved"
            ? at(r, "last_status_change_by.summary")
            : null,
    ),
    C.int("escalation_count", "Escalations in the log entries (with_timeline := true only, else NULL).", (r) =>
      r.__timeline ? r.__timeline.escalations : null,
    ),
    C.strList("assignees", "Names of the users currently assigned.", (r) => pluck(r.assignments, "assignee.summary")),
    C.strList("assignee_ids", "Ids of the users currently assigned.", (r) => pluck(r.assignments, "assignee.id")),
    C.int("alert_count", "Alerts grouped into the incident.", "alert_counts.all"),
    C.str("incident_key", "De-duplication key of the incident.", "incident_key"),
    C.time("last_status_change_at", "When the status last changed.", "last_status_change_at"),
    C.str("timeline_source", "Where acknowledged_at / resolved_at came from: log_entries or incident.", (r) =>
      r.__timeline ? "log_entries" : "incident",
    ),
    C.str("url", "Link to the incident in the PagerDuty web UI.", "html_url"),
  ];
}

interface IncidentArgs {
  since: unknown;
  until: unknown;
  date_range: string | null;
  team_ids: unknown;
  user_ids: unknown;
  sort_by: string | null;
  with_timeline: boolean | null;
  page_size: bigint;
  [k: string]: unknown;
}

/** The incident listing phases shared by incidents() and the incident fan-outs. */
function incidentListPhases(scan: ScanContext<IncidentArgs>, child?: string): Phase[] {
  const cs = scan.constraints;
  const sort = scan.args.sort_by ?? "created_at:asc";
  const q: Query = [
    ...arr("statuses", allowed(cs, "status", INCIDENT_STATUSES)),
    ...arr("urgencies", allowed(cs, "urgency", URGENCIES)),
    ...arr("service_ids", eqValues(cs, "service_id")),
    ...arr("team_ids", listArg(scan.args.team_ids)),
    ...arr("user_ids", listArg(scan.args.user_ids)),
    ["sort_by", sort],
  ];
  const key = eqValues(cs, "incident_key");
  if (key?.length === 1 && key[0]) q.push(["incident_key", key[0]]);
  const base: Omit<Phase, "window"> = {
    path: "/incidents",
    q,
    key: "incidents",
    paging: "offset",
    limit: pageSize(scan.args.page_size),
    child,
  };
  const b = timeBounds(cs, "created_at");
  const explicitTime =
    scan.args.since != null || scan.args.until != null || b.lower !== undefined || b.upper !== undefined;
  // Open incidents are few and can be old: a status filter of only triggered/acknowledged
  // with no time bound reads every open incident instead of the last 30 days.
  const statuses = allowed(cs, "status", INCIDENT_STATUSES);
  const openOnly = !!statuses?.length && !statuses.includes("resolved");
  const all = scan.args.date_range === "all" || (openOnly && !explicitTime);
  if (all && scan.args.since == null && b.lower === undefined) {
    return [{ ...base, q: [...q, ["date_range", "all"]] }];
  }
  const w = windowOf(scan, "created_at");
  return w ? windowPhases(base, w, MAX_INCIDENT_WINDOW_MS, sort === "created_at:desc") : [];
}

const TIME_ARGS = { since: TS(), until: TS() };
const TIME_ARG_DOCS = { since: SINCE_DOC, until: UNTIL_DOC };

export const incidents = definePdTable<IncidentArgs>({
  name: "incidents",
  description: "PagerDuty incidents with status, urgency, priority, assignees and time to ack / resolve",
  title: "PagerDuty Incidents",
  category: "incidents",
  keywords: "pagerduty, incidents, outages, pages, mttr, mtta, time to acknowledge, time to resolve, on-call, sre",
  docLlm:
    "PagerDuty incidents (GET /incidents), one row each: incident_id, incident_number, title, status " +
    "(triggered / acknowledged / resolved), urgency, priority, service, escalation_policy, teams, created_at, " +
    "acknowledged_at, resolved_at, time_to_ack_seconds, time_to_resolve_seconds, acknowledged_by, resolved_by, " +
    "assignees, alert_count, url. The window covers incidents CREATED between since and until: by default the " +
    "last 30 days, or the range a WHERE created_at filter names (any length: it is read in 180-day chunks); " +
    "date_range := 'all' reads all time; a status filter of only triggered/acknowledged reads every open " +
    "incident. WHERE status / urgency / service_id (= or IN), incident_key = and incident_id = / IN push down. " +
    "acknowledged_at comes from the incident's current acknowledgements, which PagerDuty clears on resolve; pass " +
    "with_timeline := true to read each incident's log entries (one extra call per incident) for exact first-ack, " +
    "resolve, resolver and escalation_count — use it for MTTA/MTTR. team_ids := [...] / user_ids := [...] " +
    "(current assignee) filter server-side. sort_by := 'created_at:desc' plus LIMIT reads only the newest pages.",
  docMd:
    "## incidents(since, until, date_range, team_ids, user_ids, sort_by, with_timeline := false, page_size := 100)\n\n" +
    "Incidents created in a time window (default: the last 30 days). Long windows are chunked into PagerDuty's " +
    "6-month maximum, and a chunk with more than 10,000 incidents is split again.",
  pushdown:
    "`status`, `urgency`, `service_id` (=, IN) → `statuses[]`, `urgencies[]`, `service_ids[]`; `created_at` ranges → " +
    "`since`/`until`; `incident_key =` → `incident_key`; `incident_id = / IN` → `GET /incidents/{id}`.",
  args: {
    since: TS(),
    until: TS(),
    date_range: utf8(),
    team_ids: STR_LIST(),
    user_ids: STR_LIST(),
    sort_by: utf8(),
    with_timeline: bool,
    page_size: int64(),
  },
  argDefaults: {
    since: null,
    until: null,
    date_range: null,
    team_ids: null,
    user_ids: null,
    sort_by: null,
    with_timeline: false,
    page_size: 100,
  },
  argDocs: {
    ...TIME_ARG_DOCS,
    date_range:
      "'all' reads incidents from all time; 'window' (or NULL) uses since/until or the default 30-day window.",
    team_ids: "Only incidents of these team ids (PagerDuty team_ids[]).",
    user_ids: "Only incidents currently assigned to these user ids (PagerDuty user_ids[]).",
    sort_by:
      "Order PagerDuty returns rows in: created_at, incident_number, resolved_at or urgency, with :asc or :desc.",
    with_timeline: "Read each incident's log entries for exact acknowledged_at / resolved_at / escalation_count.",
    page_size: PAGE_SIZE_DOC,
  },
  argConstraints: {
    date_range: { choices: ["all", "window"] },
    sort_by: {
      choices: ["created_at", "incident_number", "resolved_at", "urgency"].flatMap((k) => [`${k}:asc`, `${k}:desc`]),
    },
    page_size: { ge: 1, le: 100 },
  },
  columns: incidentColumns(),
  plan: (scan) => {
    const ids = eqValues(scan.constraints, "incident_id");
    if (ids) {
      return ids.map((id) => ({ path: `/incidents/${enc(id)}`, key: "incident", paging: "none", notFoundEmpty: true }));
    }
    return incidentListPhases(scan);
  },
  expand: async (cfg, rows, scan) => {
    if (!scan.args.with_timeline) return rows;
    const out: Row[] = [];
    for (const r of rows) {
      const entries = await getAll(cfg, `/incidents/${enc(String(r.id))}/log_entries`, "log_entries", [
        ["is_overview", "true"],
      ]);
      out.push({ ...r, __timeline: timelineOf(entries) });
    }
    return out;
  },
  examples: [
    {
      sql: `SELECT service, count(*) AS incidents, median(time_to_resolve_seconds) / 60 AS median_minutes_to_resolve FROM ${Q}.incidents(with_timeline := true) WHERE created_at >= TIMESTAMPTZ '2026-10-01' AND urgency = 'high' GROUP BY service ORDER BY incidents DESC`,
      description: "High-urgency incidents and median time to resolve per service since October",
    },
    {
      sql: `SELECT incident_number, title, service, created_at FROM ${Q}.incidents() WHERE status IN ('triggered', 'acknowledged') ORDER BY created_at`,
      description: "Every open incident (an open-only status filter reads all time)",
    },
  ],
});

// --------------------------------------------------------------- log entries

function logEntryColumns(): Col[] {
  return [
    C.str("log_entry_id", "Log entry id.", "id", false),
    C.str("incident_id", "The incident this entry belongs to.", (r) => at(r, "incident.id") ?? r.__ctx?.incident_id),
    C.str(
      "type",
      "Entry type without the _log_entry suffix: trigger, acknowledge, unacknowledge, assign, escalate, notify, annotate, snooze, resolve, ...",
      (r) => bare(r.type, "_log_entry"),
    ),
    C.str("raw_type", "PagerDuty's entry type, e.g. acknowledge_log_entry.", "type"),
    C.str("summary", "Human-readable summary of the entry.", "summary"),
    C.time("created_at", "When it happened.", "created_at"),
    C.str("agent_type", "Who or what acted: user, service, integration, ... (the _reference suffix dropped).", (r) =>
      bare(at(r, "agent.type"), "_reference"),
    ),
    C.str("agent_id", "Id of the acting user / service / integration.", "agent.id"),
    C.str("agent_name", "Name of the acting user / service / integration.", "agent.summary"),
    C.str(
      "channel_type",
      "Channel the action came through: web_trigger, api, email, sms, auto, timeout, ...",
      "channel.type",
    ),
    C.str("service_id", "Service of the incident.", "service.id"),
    C.str("service", "Service name.", "service.summary"),
    C.strList("assignees", "Users assigned by an assign/escalate entry.", (r) => pluck(r.assignees, "summary")),
    C.strList("assignee_ids", "Ids of the users assigned by an assign/escalate entry.", (r) =>
      pluck(r.assignees, "id"),
    ),
    C.str("user_name", "User notified (notify entries).", "user.summary"),
    C.str("notification_type", "How a notify entry reached the user: sms, phone, email, push.", "notification.type"),
    C.str("note", "Note text of an annotate entry.", (r) =>
      bare(r.type, "_log_entry") === "annotate" ? (at(r, "channel.summary") ?? r.summary) : null,
    ),
  ];
}

export const incidentLogEntries = definePdTable<{
  incident_id: string | null;
  since: unknown;
  until: unknown;
  is_overview: boolean | null;
  team_ids: unknown;
  page_size: bigint;
}>({
  name: "incident_log_entries",
  description:
    "The timeline of PagerDuty incidents: trigger, acknowledge, assign, escalate, notify and resolve entries",
  title: "PagerDuty Incident Log Entries",
  category: "incidents",
  keywords:
    "pagerduty, log entries, incident timeline, acknowledge, escalate, assign, resolve, notifications, audit trail",
  docLlm:
    "Incident log entries (the timeline). With incident_id := 'Q…' or WHERE incident_id = / IN, reads " +
    "GET /incidents/{id}/log_entries for those incidents (all time); otherwise the account-wide GET /log_entries " +
    "for entries created in the window (default last 30 days, or the range a WHERE created_at filter names). " +
    "Columns: log_entry_id, incident_id, type (trigger / acknowledge / assign / escalate / notify / annotate / " +
    "resolve …), summary, created_at, agent_type / agent_id / agent_name (who acted), channel_type, service, " +
    "assignees, user_name + notification_type for notify entries, note. is_overview := true keeps only the major " +
    "state changes. Compute time to acknowledge as min(created_at) FILTER (type = 'acknowledge') minus the " +
    "trigger entry's created_at.",
  docMd:
    "## incident_log_entries(incident_id, since, until, is_overview := false, team_ids, page_size := 100)\n\n" +
    "Who did what to which incident, and when.",
  pushdown: "`incident_id = / IN` → per-incident endpoint; `created_at` ranges → `since`/`until`.",
  args: { incident_id: utf8(), ...TIME_ARGS, is_overview: bool, team_ids: STR_LIST(), page_size: int64() },
  argDefaults: { incident_id: null, since: null, until: null, is_overview: false, team_ids: null, page_size: 100 },
  argDocs: {
    incident_id: "Read one incident's timeline (all time) instead of the account-wide window.",
    ...TIME_ARG_DOCS,
    is_overview: "Only the major entries (state changes) rather than every notification.",
    team_ids: "Account-wide listing only: entries of these teams' incidents.",
    page_size: PAGE_SIZE_DOC,
  },
  argConstraints: { page_size: { ge: 1, le: 100 } },
  columns: logEntryColumns(),
  plan: (scan) => {
    const ids = scan.args.incident_id ? [scan.args.incident_id] : eqValues(scan.constraints, "incident_id");
    const overview: Query = [["is_overview", String(!!scan.args.is_overview)]];
    const limit = pageSize(scan.args.page_size);
    if (ids) {
      const q: Query = [...overview];
      if (scan.args.since != null) q.push(["since", iso(toEpochMs(scan.args.since) ?? 0)]);
      if (scan.args.until != null) q.push(["until", iso(toEpochMs(scan.args.until) ?? 0)]);
      return ids.map((id) => ({
        path: `/incidents/${enc(id)}/log_entries`,
        q,
        key: "log_entries",
        paging: "offset",
        limit,
        ctx: { incident_id: id },
        notFoundEmpty: true,
      }));
    }
    const w = windowOf(scan, "created_at");
    const base = {
      path: "/log_entries",
      q: [...overview, ...arr("team_ids", listArg(scan.args.team_ids))],
      key: "log_entries",
      paging: "offset" as const,
      limit,
    };
    return w ? windowPhases(base, w, MAX_INCIDENT_WINDOW_MS) : [];
  },
  examples: [
    {
      sql: `SELECT type, agent_name, created_at FROM ${Q}.incident_log_entries(incident_id := 'Q2CHECKOUT500') ORDER BY created_at`,
      description: "The timeline of one incident",
    },
    {
      sql: `SELECT agent_name, count(*) AS acks FROM ${Q}.incident_log_entries() WHERE type = 'acknowledge' AND created_at >= TIMESTAMPTZ '2026-10-01' GROUP BY agent_name ORDER BY acks DESC`,
      description: "Who acknowledges the most incidents",
    },
  ],
});

// ------------------------------------------------------- alerts and notes (fan-out)

type FanArgs = { incident_id: string | null; since: unknown; until: unknown; page_size: bigint };

/** Incident ids from the argument or a pushed `= / IN`, else undefined (fan out). */
const incidentIds = (scan: ScanContext<FanArgs>) =>
  scan.args.incident_id ? [scan.args.incident_id] : eqValues(scan.constraints, "incident_id");

/** Parent incidents for a fan-out: the window of incidents() with no other filters. */
function parentIncidents(scan: ScanContext<FanArgs>, child: string): Phase[] {
  const parentScan = {
    args: { since: scan.args.since, until: scan.args.until, page_size: 100n } as unknown as IncidentArgs,
    constraints: [],
    anchorMs: scan.anchorMs,
  };
  return incidentListPhases(parentScan, child);
}

const FAN_DOC =
  "With incident_id := 'Q…' or WHERE incident_id = / IN only those incidents are read; otherwise every incident " +
  "CREATED in the since/until window (default the last 30 days) is listed and each one read in turn (one extra " +
  "call per incident).";

export const incidentAlerts = definePdTable<FanArgs>({
  name: "incident_alerts",
  description: "The alerts grouped into PagerDuty incidents (the monitoring events behind each page)",
  title: "PagerDuty Incident Alerts",
  category: "incidents",
  keywords: "pagerduty, alerts, events, monitoring, alert grouping, severity, integrations, noise",
  docLlm:
    `Alerts of PagerDuty incidents (GET /incidents/{id}/alerts). ${FAN_DOC} Columns: alert_id, incident_id, ` +
    "alert_key, status (triggered / resolved), severity (critical / error / warning / info), summary, created_at, " +
    "service_id, service, integration_id, suppressed, url, details (the event's custom details as JSON). " +
    "WHERE status = / IN pushes down as statuses[]. Use it to find noisy sources: alerts per incident, per " +
    "service or per alert_key.",
  docMd: `## incident_alerts(incident_id, since, until, page_size := 100)\n\nAlerts per incident. ${FAN_DOC}`,
  pushdown: "`incident_id = / IN` → only those incidents; `status = / IN` → `statuses[]`.",
  args: { incident_id: utf8(), ...TIME_ARGS, page_size: int64() },
  argDefaults: { incident_id: null, since: null, until: null, page_size: 100 },
  argDocs: { incident_id: "Read only this incident's alerts.", ...TIME_ARG_DOCS, page_size: PAGE_SIZE_DOC },
  argConstraints: { page_size: { ge: 1, le: 100 } },
  columns: [
    C.str("alert_id", "Alert id.", "id", false),
    C.str(
      "incident_id",
      "The incident the alert is grouped into.",
      (r) => at(r, "incident.id") ?? r.__ctx?.incident_id,
    ),
    C.str("alert_key", "De-duplication key of the alert.", "alert_key"),
    C.str("status", "triggered or resolved.", "status"),
    C.str("severity", "critical, error, warning or info.", "severity"),
    C.str("summary", "Alert summary.", "summary"),
    C.time("created_at", "When the alert was created.", "created_at"),
    C.str("service_id", "Service id.", "service.id"),
    C.str("service", "Service name.", "service.summary"),
    C.str("integration_id", "The integration that sent the event.", "integration.id"),
    C.bool("suppressed", "True when the alert was suppressed (did not create an incident).", "suppressed"),
    C.str("url", "Link to the alert in the PagerDuty web UI.", "html_url"),
    C.json("details", "The event's custom details (body.details) as JSON.", "body.details"),
  ],
  plan: (scan) => {
    const ids = incidentIds(scan);
    return ids ? ids.map((id) => alertPhase(id, scan)) : parentIncidents(scan, "alerts");
  },
  children: { alerts: (parent, scan) => (typeof parent.id === "string" ? alertPhase(parent.id, scan) : null) },
  examples: [
    {
      sql: `SELECT severity, count(*) AS alerts FROM ${Q}.incident_alerts(incident_id := 'Q2CHECKOUT500') GROUP BY severity ORDER BY alerts DESC`,
      description: "Alert severities behind one incident",
    },
    {
      sql: `SELECT service, alert_key, count(*) AS alerts FROM ${Q}.incident_alerts(since := TIMESTAMPTZ '2026-10-01') GROUP BY ALL ORDER BY alerts DESC LIMIT 10`,
      description: "The noisiest alert keys since October",
    },
  ],
});

function alertPhase(id: string, scan: ScanContext<FanArgs>): Phase {
  return {
    path: `/incidents/${enc(id)}/alerts`,
    q: arr("statuses", allowed(scan.constraints, "status", ["triggered", "resolved"])),
    key: "alerts",
    paging: "offset",
    limit: pageSize(scan.args.page_size),
    ctx: { incident_id: id },
    notFoundEmpty: true,
  };
}

const notePhase = (id: string): Phase => ({
  path: `/incidents/${enc(id)}/notes`,
  key: "notes",
  paging: "none",
  ctx: { incident_id: id },
  notFoundEmpty: true,
});

export const incidentNotes = definePdTable<FanArgs>({
  name: "incident_notes",
  description: "Notes responders added to PagerDuty incidents",
  title: "PagerDuty Incident Notes",
  category: "incidents",
  keywords: "pagerduty, incident notes, annotations, comments, responders, postmortem",
  docLlm:
    `Notes on PagerDuty incidents (GET /incidents/{id}/notes). ${FAN_DOC} Columns: note_id, incident_id, ` +
    "created_at, user_id, user_name (author), channel, content.",
  docMd: `## incident_notes(incident_id, since, until)\n\nResponder notes per incident. ${FAN_DOC}`,
  pushdown: "`incident_id = / IN` → only those incidents.",
  args: { incident_id: utf8(), ...TIME_ARGS, page_size: int64() },
  argDefaults: { incident_id: null, since: null, until: null, page_size: 100 },
  argDocs: {
    incident_id: "Read only this incident's notes.",
    ...TIME_ARG_DOCS,
    page_size: "Page size of the parent incident listing (named; at most 100).",
  },
  argConstraints: { page_size: { ge: 1, le: 100 } },
  columns: [
    C.str("note_id", "Note id.", "id", false),
    C.ctx("incident_id", "The incident the note is on.", "incident_id"),
    C.time("created_at", "When the note was added.", "created_at"),
    C.str("user_id", "Author's user id.", "user.id"),
    C.str("user_name", "Author's name.", "user.summary"),
    C.str("channel", "Where the note came from (e.g. The PagerDuty website or APIs).", "channel.summary"),
    C.str("content", "The note text.", "content"),
  ],
  plan: (scan) => {
    const ids = incidentIds(scan);
    return ids ? ids.map(notePhase) : parentIncidents(scan, "notes");
  },
  children: { notes: (parent) => (typeof parent.id === "string" ? notePhase(parent.id) : null) },
  examples: [
    {
      sql: `SELECT user_name, created_at, content FROM ${Q}.incident_notes(incident_id := 'Q2CHECKOUT500') ORDER BY created_at`,
      description: "Responder notes on one incident",
    },
  ],
});

// ------------------------------------------------------------------- on-call

export const oncalls = definePdTable<{ since: unknown; until: unknown; earliest: boolean | null; page_size: bigint }>({
  name: "oncalls",
  description: "Who is (or was) on call: one row per user, escalation policy level and schedule",
  title: "PagerDuty On-Calls",
  category: "oncall",
  keywords: "pagerduty, on-call, oncall, who is on call, rotation, escalation level, schedule, shifts",
  docLlm:
    "On-call entries (GET /oncalls): user_id, user_name, schedule_id, schedule_name (NULL when the user is a " +
    "direct escalation target), escalation_policy_id, escalation_policy_name, escalation_level (1 = first " +
    "responder), start_time, end_time (NULL = permanent). With no arguments it answers 'who is on call right " +
    "now'; since := / until := (at most 90 days apart) answer 'who was on call when'. earliest := true keeps " +
    "only the next shift per user/policy/level. WHERE user_id / schedule_id / escalation_policy_id (= / IN) " +
    "push down.",
  docMd:
    "## oncalls(since, until, earliest := false, page_size := 100)\n\nWho is on call now, or during a window, at each escalation level.",
  pushdown:
    "`user_id`, `schedule_id`, `escalation_policy_id` (=, IN) → `user_ids[]`, `schedule_ids[]`, `escalation_policy_ids[]`.",
  args: { since: TS(), until: TS(), earliest: bool, page_size: int64() },
  argDefaults: { since: null, until: null, earliest: false, page_size: 100 },
  argDocs: {
    since: "Start of the window to report on-call shifts for (default: now).",
    until: "End of the window (default: now). PagerDuty caps the since..until span; see the function docs.",
    earliest: "Only the earliest on-call shift per user, escalation policy and level.",
    page_size: PAGE_SIZE_DOC,
  },
  argConstraints: { page_size: { ge: 1, le: 100 } },
  columns: [
    C.str("user_id", "On-call user id.", "user.id"),
    C.str("user_name", "On-call user name.", "user.summary"),
    C.str("schedule_id", "Schedule that put the user on call (NULL for a direct escalation target).", "schedule.id"),
    C.str("schedule_name", "Schedule name.", "schedule.summary"),
    C.str("escalation_policy_id", "Escalation policy id.", "escalation_policy.id"),
    C.str("escalation_policy_name", "Escalation policy name.", "escalation_policy.summary"),
    C.int("escalation_level", "Escalation level (1 = first responder).", "escalation_level"),
    C.time("start_time", "Shift start (NULL for a permanent on-call).", "start"),
    C.time("end_time", "Shift end (NULL for a permanent on-call).", "end"),
  ],
  plan: ({ args, constraints }) => {
    const q: Query = [
      ...arr("user_ids", eqValues(constraints, "user_id")),
      ...arr("schedule_ids", eqValues(constraints, "schedule_id")),
      ...arr("escalation_policy_ids", eqValues(constraints, "escalation_policy_id")),
      ["time_zone", "UTC"],
    ];
    const [s, u] = [toEpochMs(args.since), toEpochMs(args.until)];
    if (s !== undefined && u !== undefined && u - s > MAX_ONCALL_WINDOW_MS) {
      throw new PdError("oncalls: since and until may be at most 90 days apart (PagerDuty's limit)");
    }
    if (s !== undefined) q.push(["since", iso(s)]);
    if (u !== undefined) q.push(["until", iso(u)]);
    if (args.earliest) q.push(["earliest", "true"]);
    return [{ path: "/oncalls", q, key: "oncalls", paging: "offset", limit: pageSize(args.page_size) }];
  },
  examples: [
    {
      sql: `SELECT escalation_policy_name, escalation_level, user_name FROM ${Q}.oncalls() ORDER BY escalation_policy_name, escalation_level`,
      description: "Who is on call right now, per escalation policy and level",
    },
    {
      sql: `SELECT user_name, sum(epoch(end_time) - epoch(start_time)) / 3600 AS hours FROM ${Q}.oncalls(since := TIMESTAMPTZ '2026-11-01', until := TIMESTAMPTZ '2026-11-08') WHERE escalation_level = 1 GROUP BY user_name ORDER BY hours DESC`,
      description: "First-responder hours per person in one week",
    },
  ],
});

type SchedArgs = { schedule_id: string | null; since: unknown; until: unknown };

function scheduleWindow(scan: ScanContext<SchedArgs>): { lo: number; hi: number } {
  const lo = toEpochMs(scan.args.since) ?? scan.anchorMs - 7 * DAY;
  const hi = toEpochMs(scan.args.until) ?? lo + 14 * DAY;
  return { lo, hi };
}

const schedulePhase = (id: string, w: { lo: number; hi: number }): Phase => ({
  path: `/schedules/${enc(id)}`,
  q: [
    ["since", iso(w.lo)],
    ["until", iso(w.hi)],
    ["time_zone", "UTC"],
  ],
  key: "schedule",
  paging: "none",
  notFoundEmpty: true,
});

export const scheduleEntries = definePdTable<SchedArgs>({
  name: "schedule_entries",
  description: "Rendered on-call shifts of PagerDuty schedules: who was on call when",
  title: "PagerDuty Schedule Entries",
  category: "oncall",
  keywords: "pagerduty, schedule, rotation, shifts, on-call history, who was on call, coverage, final schedule",
  docLlm:
    "The final (rendered, overrides applied) on-call shifts of schedules between since and until (GET " +
    "/schedules/{id}?since&until): schedule_id, schedule_name, user_id, user_name, start_time, end_time, " +
    "duration_seconds. Shifts are clipped to the window. schedule_id := 'P…' or WHERE schedule_id = / IN reads " +
    "those schedules; otherwise every schedule is read (one call each). Default window: 7 days ago to 7 days " +
    "ahead. Use it for on-call load and coverage gaps.",
  docMd:
    "## schedule_entries(schedule_id, since, until)\n\nWho was (or will be) on call in each schedule, shift by shift.",
  pushdown: "`schedule_id = / IN` → only those schedules.",
  args: { schedule_id: utf8(), ...TIME_ARGS },
  argDefaults: { schedule_id: null, since: null, until: null },
  argDocs: {
    schedule_id: "Read only this schedule.",
    since: "Start of the window (default: 7 days ago).",
    until: "End of the window (default: 14 days after since).",
  },
  columns: [
    C.str("schedule_id", "Schedule id.", (r) => r.__sched?.id),
    C.str("schedule_name", "Schedule name.", (r) => r.__sched?.name),
    C.str("user_id", "On-call user id.", "user.id"),
    C.str("user_name", "On-call user name.", "user.summary"),
    C.time("start_time", "Shift start (clipped to the window).", "start"),
    C.time("end_time", "Shift end (clipped to the window).", "end"),
    C.num("duration_seconds", "Shift length in seconds.", (r) => secondsBetween(r.start, r.end)),
  ],
  plan: (scan) => {
    const w = scheduleWindow(scan);
    const ids = scan.args.schedule_id ? [scan.args.schedule_id] : eqValues(scan.constraints, "schedule_id");
    if (ids) return ids.map((id) => schedulePhase(id, w));
    return [{ path: "/schedules", key: "schedules", paging: "offset", limit: 100, child: "entries" }];
  },
  children: {
    entries: (parent, scan) => (typeof parent.id === "string" ? schedulePhase(parent.id, scheduleWindow(scan)) : null),
  },
  explode: (sched) => {
    const entries = at(sched, "final_schedule.rendered_schedule_entries");
    return Array.isArray(entries) ? entries.map((e: Row) => ({ ...e, __sched: sched, __ctx: sched.__ctx })) : [];
  },
  examples: [
    {
      sql: `SELECT schedule_name, user_name, sum(duration_seconds) / 3600 AS hours FROM ${Q}.schedule_entries(since := TIMESTAMPTZ '2026-11-01', until := TIMESTAMPTZ '2026-11-08') GROUP BY ALL ORDER BY hours DESC`,
      description: "On-call hours per person and schedule for one week",
    },
  ],
});

// ------------------------------------------------------------ change events

export const changeEvents = definePdTable<{
  since: unknown;
  until: unknown;
  team_ids: unknown;
  integration_ids: unknown;
  page_size: bigint;
}>({
  name: "change_events",
  description: "Recent PagerDuty change events: deploys, config changes and other 'what changed' signals",
  title: "PagerDuty Change Events",
  category: "changes",
  keywords: "pagerduty, change events, deploys, deployments, releases, what changed, change correlation, ci/cd",
  docLlm:
    "Change events (GET /change_events): change_event_id, summary, occurred_at, source, service_ids, services, " +
    "integration_id, links (href/text), custom_details JSON. The window covers change events between since and " +
    "until (default the last 30 days, or a WHERE occurred_at range). team_ids := [...] / integration_ids := [...] " +
    "filter server-side. Join to incidents on service and time (e.g. changes in the hour before created_at) to " +
    "find likely causes.",
  docMd: "## change_events(since, until, team_ids, integration_ids, page_size := 100)\n\nThe 'what changed' feed.",
  pushdown: "`occurred_at` ranges → `since`/`until`.",
  args: { ...TIME_ARGS, team_ids: STR_LIST(), integration_ids: STR_LIST(), page_size: int64() },
  argDefaults: { since: null, until: null, team_ids: null, integration_ids: null, page_size: 100 },
  argDocs: {
    ...TIME_ARG_DOCS,
    team_ids: "Only change events of these teams.",
    integration_ids: "Only change events sent through these integrations.",
    page_size: PAGE_SIZE_DOC,
  },
  argConstraints: { page_size: { ge: 1, le: 100 } },
  columns: [
    C.str("change_event_id", "Change event id.", "id", false),
    C.str("summary", "What changed.", "summary"),
    C.time("occurred_at", "When the change happened (the event timestamp).", "timestamp"),
    C.str("source", "The system that reported the change.", "source"),
    C.strList("service_ids", "Ids of the affected services.", (r) => pluck(r.services, "id")),
    C.strList("services", "Names of the affected services.", (r) => pluck(r.services, "summary")),
    C.str("integration_id", "Integration that received the event.", "integration.id"),
    C.structList(
      "links",
      "Links attached to the event (build, diff, ...).",
      [
        { name: "href", path: "href" },
        { name: "text", path: "text" },
      ],
      "links",
    ),
    C.json("custom_details", "The event's custom details as JSON.", "custom_details"),
  ],
  plan: (scan) => {
    const w = windowOf(scan as ScanContext<Record<string, unknown>>, "occurred_at");
    const base = {
      path: "/change_events",
      q: [
        ...arr("team_ids", listArg(scan.args.team_ids)),
        ...arr("integration_ids", listArg(scan.args.integration_ids)),
      ],
      key: "change_events",
      paging: "offset" as const,
      limit: pageSize(scan.args.page_size),
    };
    return w ? windowPhases(base, w, MAX_INCIDENT_WINDOW_MS) : [];
  },
  examples: [
    {
      sql: `SELECT occurred_at, summary, services FROM ${Q}.change_events() WHERE occurred_at >= TIMESTAMPTZ '2026-11-01' ORDER BY occurred_at DESC`,
      description: "Changes since November 1st, newest first",
    },
    {
      sql: `SELECT i.incident_number, c.summary AS change, c.occurred_at FROM ${Q}.incidents(date_range := 'all') i JOIN ${Q}.change_events(since := TIMESTAMPTZ '2026-11-01') c ON list_contains(c.service_ids, i.service_id) AND c.occurred_at BETWEEN i.created_at - INTERVAL 1 HOUR AND i.created_at ORDER BY i.incident_number`,
      description: "Changes on the same service in the hour before each incident",
    },
  ],
});

// --------------------------------------------------------------- audit records

const RESOURCE_TYPES: Record<string, string> = {
  user_reference: "users",
  team_reference: "teams",
  schedule_reference: "schedules",
  escalation_policy_reference: "escalation_policies",
  service_reference: "services",
};

export const auditRecords = definePdTable<{ since: unknown; until: unknown; page_size: bigint }>({
  name: "audit_records",
  description: "PagerDuty audit trail: who created, changed or deleted users, teams, schedules, policies and services",
  title: "PagerDuty Audit Records",
  category: "admin",
  keywords: "pagerduty, audit, audit log, audit trail, configuration changes, compliance, who changed what, admin",
  docLlm:
    "Audit records (GET /audit/records; an admin key on a plan with Audit Trail — check account_features). " +
    "Columns: record_id, execution_time, action (create / update / delete), root_resource_type (users / teams / " +
    "schedules / escalation_policies / services), root_resource_id, root_resource_name, actor_type " +
    "(user_reference / api_key_reference / app_reference), actor_id, actor_name, method_type, " +
    "method_description, method_truncated_token, request_id, remote_address, changed_fields (name, before_value, " +
    "value), details JSON. The window is since..until (default the last 7 days, or a WHERE execution_time " +
    "range), read in PagerDuty's 31-day chunks with cursor paging. WHERE action / root_resource_type (= / IN), " +
    "actor_type =, actor_id = (with actor_type) and method_type = push down.",
  docMd: "## audit_records(since, until, page_size := 100)\n\nConfiguration changes, by whom and through what.",
  pushdown:
    "`action`, `root_resource_type` (=, IN) → `actions[]`, `root_resource_types[]`; `actor_type =`, `actor_id =`, " +
    "`method_type =`; `execution_time` ranges → `since`/`until`.",
  args: { ...TIME_ARGS, page_size: int64() },
  argDefaults: { since: null, until: null, page_size: 100 },
  argDocs: {
    since: "Start of the window (inclusive). Defaults from a pushed execution_time filter, else 7 days ago.",
    until: UNTIL_DOC,
    page_size: PAGE_SIZE_DOC,
  },
  argConstraints: { page_size: { ge: 1, le: 100 } },
  feature: "The Audit Records API",
  columns: [
    C.str("record_id", "Audit record id.", "id", false),
    C.time("execution_time", "When the change was made.", "execution_time"),
    C.str("action", "create, update or delete.", "action"),
    C.str("root_resource_type", "users, teams, schedules, escalation_policies or services.", (r) => {
      const t = at(r, "root_resource.type");
      return typeof t === "string" ? (RESOURCE_TYPES[t] ?? t) : null;
    }),
    C.str("root_resource_id", "Id of the changed resource.", "root_resource.id"),
    C.str("root_resource_name", "Name of the changed resource.", "root_resource.summary"),
    C.str("actor_type", "user_reference, api_key_reference or app_reference.", "actors.0.type"),
    C.str("actor_id", "Id of the actor.", "actors.0.id"),
    C.str("actor_name", "Name of the actor.", "actors.0.summary"),
    C.str(
      "method_type",
      "How the change was made: browser, oauth, api_token, identity_provider, other.",
      "method.type",
    ),
    C.str("method_description", "Description of the method (e.g. the API key's name).", "method.description"),
    C.str("method_truncated_token", "Last characters of the API key used, if any.", "method.truncated_token"),
    C.str("request_id", "PagerDuty request id.", "execution_context.request_id"),
    C.str("remote_address", "IP address the change came from.", "execution_context.remote_address"),
    C.structList(
      "changed_fields",
      "Fields the change touched, with the value before and after.",
      [
        { name: "name", path: "name" },
        { name: "before_value", path: "before_value" },
        { name: "value", path: "value" },
      ],
      "details.fields",
    ),
    C.json("details", "The full change details as JSON.", "details"),
  ],
  plan: (scan) => {
    const cs = scan.constraints;
    const q: Query = [
      ...arr("actions", allowed(cs, "action", ["create", "update", "delete"])),
      ...arr("root_resource_types", allowed(cs, "root_resource_type", Object.values(RESOURCE_TYPES))),
    ];
    const actorType = eqValues(cs, "actor_type");
    if (actorType?.length === 1 && actorType[0]) {
      q.push(["actor_type", actorType[0]]);
      const actorId = eqValues(cs, "actor_id");
      if (actorId?.length === 1 && actorId[0]) q.push(["actor_id", actorId[0]]);
    }
    const method = eqValues(cs, "method_type");
    if (method?.length === 1 && method[0]) q.push(["method_type", method[0]]);
    const w = windowOf(scan as ScanContext<Record<string, unknown>>, "execution_time", 7 * DAY);
    const base = {
      path: "/audit/records",
      q,
      key: "records",
      paging: "cursor" as const,
      limit: pageSize(scan.args.page_size),
    };
    return w ? windowPhases(base, w, MAX_AUDIT_WINDOW_MS) : [];
  },
  examples: [
    {
      sql: `SELECT execution_time, actor_name, action, root_resource_type, root_resource_name FROM ${Q}.audit_records() WHERE execution_time >= TIMESTAMPTZ '2026-11-01' AND action = 'delete' ORDER BY execution_time`,
      description: "Everything deleted since November 1st, and by whom",
    },
  ],
});

// ------------------------------------------------------------------ analytics

type AnalyticsArgs = { since: unknown; until: unknown; page_size: bigint };

export const analyticsIncidents = definePdTable<AnalyticsArgs>({
  name: "analytics_incidents",
  description: "PagerDuty Analytics per-incident metrics: time to ack / resolve / engage, interruptions, escalations",
  title: "PagerDuty Incident Analytics",
  category: "analytics",
  keywords:
    "pagerduty, analytics, mtta, mttr, time to acknowledge, time to resolve, interruptions, sleep hours, engagement, sre metrics",
  docLlm:
    "PagerDuty Analytics raw incident data (POST /analytics/raw/incidents; needs Analytics on the plan — check " +
    "account_features): incident_id, incident_number, title, service, service_id, team, team_id, " +
    "escalation_policy_id, created_at, resolved_at, urgency, major, priority, priority_id, time_to_ack_seconds " +
    "(seconds_to_first_ack), time_to_resolve_seconds, seconds_to_engage, seconds_to_mobilize, engaged_seconds, " +
    "engaged_user_count, escalation_count, assignment_count, total_notifications, total_interruptions, " +
    "business_hour_interruptions, off_hour_interruptions, sleep_hour_interruptions. Window: incidents created " +
    "between since and until (default the last 30 days, or a WHERE created_at range), read in 1-year chunks. " +
    "WHERE urgency =, major =, service_id / team_id / priority_id (= / IN) push into the request filters. " +
    "Analytics data lags live incidents by a few minutes to hours.",
  docMd:
    "## analytics_incidents(since, until, page_size := 1000)\n\nPer-incident response metrics from the Analytics API: MTTA, MTTR and human cost.",
  pushdown:
    "`urgency =`, `major =`, `service_id` / `team_id` / `priority_id` (=, IN) → request filters; `created_at` ranges → `created_at_start`/`created_at_end`.",
  args: { ...TIME_ARGS, page_size: int64() },
  argDefaults: { since: null, until: null, page_size: 1000 },
  argDocs: { ...TIME_ARG_DOCS, page_size: "Rows per Analytics API page (named; at most 1000)." },
  argConstraints: { page_size: { ge: 1, le: 1000 } },
  feature: "The PagerDuty Analytics API",
  columns: [
    C.str("incident_id", "Incident id.", "id", false),
    C.big("incident_number", "Incident number.", "incident_number"),
    C.str("title", "Incident title.", "description"),
    C.str("service", "Service name.", "service_name"),
    C.str("service_id", "Service id.", "service_id"),
    C.str("team", "Team name.", "team_name"),
    C.str("team_id", "Team id.", "team_id"),
    C.str("escalation_policy_id", "Escalation policy id.", "escalation_policy_id"),
    C.time("created_at", "When the incident was triggered.", "created_at"),
    C.time("resolved_at", "When it was resolved.", "resolved_at"),
    C.str("urgency", "high or low.", "urgency"),
    C.bool("major", "True for a major incident.", "major"),
    C.str("priority", "Priority name.", "priority_name"),
    C.str("priority_id", "Priority id.", "priority_id"),
    C.num("time_to_ack_seconds", "Seconds to the first acknowledgement.", "seconds_to_first_ack"),
    C.num("time_to_resolve_seconds", "Seconds to resolution.", "seconds_to_resolve"),
    C.num("seconds_to_engage", "Seconds until the first responder engaged.", "seconds_to_engage"),
    C.num("seconds_to_mobilize", "Seconds until additional responders were mobilized.", "seconds_to_mobilize"),
    C.num("engaged_seconds", "Total responder time spent engaged.", "engaged_seconds"),
    C.int("engaged_user_count", "Distinct responders who engaged.", "engaged_user_count"),
    C.int("escalation_count", "Escalations.", "escalation_count"),
    C.int("assignment_count", "Assignments.", "assignment_count"),
    C.int("total_notifications", "Notifications sent.", "total_notifications"),
    C.int("total_interruptions", "Interruptions (notifications that paged a human).", "total_interruptions"),
    C.int("business_hour_interruptions", "Interruptions during business hours.", "business_hour_interruptions"),
    C.int("off_hour_interruptions", "Interruptions outside business hours.", "off_hour_interruptions"),
    C.int("sleep_hour_interruptions", "Interruptions during sleep hours.", "sleep_hour_interruptions"),
  ],
  plan: (scan) => {
    const cs = scan.constraints;
    const filters: Record<string, unknown> = {};
    const urg = allowed(cs, "urgency", URGENCIES);
    if (urg?.length === 1) filters.urgency = urg[0];
    const major = cs.find(
      (c) => c.column === "major" && c.kind === "cmp" && c.op === "eq" && typeof c.value === "boolean",
    );
    if (major && major.kind === "cmp") filters.major = major.value;
    for (const [col, key] of [
      ["service_id", "service_ids"],
      ["team_id", "team_ids"],
      ["priority_id", "priority_ids"],
    ] as const) {
      const vs = eqValues(cs, col);
      if (vs) filters[key] = vs;
    }
    const w = windowOf(scan as ScanContext<Record<string, unknown>>, "created_at");
    if (!w) return [];
    return chunkWindow(w.lo, w.hi, MAX_ANALYTICS_WINDOW_MS).map(([lo, hi]) => ({
      path: "/analytics/raw/incidents",
      method: "POST" as const,
      body: { filters, order: "asc", order_by: "created_at", time_zone: "Etc/UTC" },
      key: "data",
      paging: "analytics" as const,
      limit: pageSize(scan.args.page_size, 1000),
      window: { lo, hi, style: "analytics" as const },
    }));
  },
  examples: [
    {
      sql: `SELECT service, count(*) AS incidents, avg(time_to_ack_seconds) / 60 AS mtta_minutes, sum(sleep_hour_interruptions) AS sleep_interruptions FROM ${Q}.analytics_incidents(since := TIMESTAMPTZ '2026-10-01') GROUP BY service ORDER BY incidents DESC`,
      description: "MTTA and sleep-hour interruptions per service since October",
    },
  ],
});

export const analyticsIncidentMetrics = definePdTable<{
  since: unknown;
  until: unknown;
  aggregate_unit: string | null;
  urgency: string | null;
  service_ids: unknown;
  team_ids: unknown;
}>({
  name: "analytics_incident_metrics",
  description:
    "PagerDuty Analytics aggregate incident metrics (mean time to ack / resolve, interruptions), optionally per day, week or month",
  title: "PagerDuty Aggregate Incident Metrics",
  category: "analytics",
  keywords:
    "pagerduty, analytics, mtta, mttr, mean time to acknowledge, mean time to resolve, uptime, interruptions, trend",
  docLlm:
    "Aggregated incident metrics from the Analytics API (POST /analytics/metrics/incidents/all; needs Analytics — " +
    "check account_features) for incidents created between since and until (default the last 30 days, at most 1 " +
    "year). aggregate_unit := 'day' / 'week' / 'month' returns one row per period (range_start), else one row " +
    "for the whole window. urgency := 'high'|'low', service_ids := [...], team_ids := [...] filter the incidents " +
    "counted. Columns: range_start, total_incident_count, mean_time_to_ack_seconds, mean_time_to_resolve_seconds, " +
    "mean_seconds_to_engage, mean_seconds_to_mobilize, total_interruptions, total_business_hour_interruptions, " +
    "total_off_hour_interruptions, total_sleep_hour_interruptions, total_notifications, total_escalation_count, " +
    "mean_assignment_count, up_time_pct. Filters are arguments, not WHERE clauses, because the rows are aggregates.",
  docMd:
    "## analytics_incident_metrics(since, until, aggregate_unit, urgency, service_ids, team_ids)\n\nMTTA / MTTR trends computed by PagerDuty.",
  args: { ...TIME_ARGS, aggregate_unit: utf8(), urgency: utf8(), service_ids: STR_LIST(), team_ids: STR_LIST() },
  argDefaults: { since: null, until: null, aggregate_unit: null, urgency: null, service_ids: null, team_ids: null },
  argDocs: {
    since: "Start of the window (default 30 days before until).",
    until: "End of the window (default now). PagerDuty caps the since..until span; see the function docs.",
    aggregate_unit: "day, week or month for one row per period; NULL for one row.",
    urgency: "Count only high or low urgency incidents.",
    service_ids: "Count only incidents of these services.",
    team_ids: "Count only incidents of these teams.",
  },
  argConstraints: { aggregate_unit: { choices: ["day", "week", "month"] }, urgency: { choices: URGENCIES } },
  feature: "The PagerDuty Analytics API",
  columns: [
    C.time("range_start", "Start of the period (NULL without aggregate_unit).", "range_start"),
    C.int("total_incident_count", "Incidents created in the period.", "total_incident_count"),
    C.num("mean_time_to_ack_seconds", "Mean seconds to first acknowledgement.", "mean_seconds_to_first_ack"),
    C.num("mean_time_to_resolve_seconds", "Mean seconds to resolution.", "mean_seconds_to_resolve"),
    C.num("mean_seconds_to_engage", "Mean seconds until a responder engaged.", "mean_seconds_to_engage"),
    C.num("mean_seconds_to_mobilize", "Mean seconds until more responders were mobilized.", "mean_seconds_to_mobilize"),
    C.int("total_interruptions", "Interruptions.", "total_interruptions"),
    C.int("total_business_hour_interruptions", "Interruptions in business hours.", "total_business_hour_interruptions"),
    C.int("total_off_hour_interruptions", "Interruptions off hours.", "total_off_hour_interruptions"),
    C.int("total_sleep_hour_interruptions", "Interruptions in sleep hours.", "total_sleep_hour_interruptions"),
    C.int("total_notifications", "Notifications.", "total_notifications"),
    C.int("total_escalation_count", "Escalations.", "total_escalation_count"),
    C.num("mean_assignment_count", "Mean assignments per incident.", "mean_assignment_count"),
    C.num("up_time_pct", "Percentage of the period with no open incident.", "up_time_pct"),
  ],
  plan: (scan) => {
    const w = windowOf(scan as ScanContext<Record<string, unknown>>, "range_start");
    if (!w) return [];
    if (w.hi - w.lo > MAX_ANALYTICS_WINDOW_MS) {
      throw new PdError("analytics_incident_metrics: since and until may be at most 1 year apart (PagerDuty's limit)");
    }
    const filters: Record<string, unknown> = { created_at_start: iso(w.lo), created_at_end: iso(w.hi) };
    if (scan.args.urgency) filters.urgency = scan.args.urgency;
    const svc = listArg(scan.args.service_ids);
    if (svc) filters.service_ids = svc;
    const teams = listArg(scan.args.team_ids);
    if (teams) filters.team_ids = teams;
    const body: Record<string, unknown> = { filters, time_zone: "Etc/UTC" };
    if (scan.args.aggregate_unit) body.aggregate_unit = scan.args.aggregate_unit;
    return [{ path: "/analytics/metrics/incidents/all", method: "POST", body, key: "data", paging: "none" }];
  },
  examples: [
    {
      sql: `SELECT range_start, total_incident_count, mean_time_to_ack_seconds / 60 AS mtta_minutes FROM ${Q}.analytics_incident_metrics(since := TIMESTAMPTZ '2026-08-01', until := TIMESTAMPTZ '2026-11-01', aggregate_unit := 'month') ORDER BY range_start`,
      description: "Monthly incident count and MTTA trend",
    },
  ],
});

// ------------------------------------------------------------ catalog tables

/** Push `name = 'x'` into PagerDuty's `query` (a looser name match; re-checked exactly). */
function nameQuery(cs: Constraint[], ...columns: string[]): Query {
  for (const col of columns) {
    const v = eqValues(cs, col);
    if (v?.length === 1 && v[0]) return [["query", v[0]]];
  }
  return [];
}

const list100 = (path: string, key: string, q: Query = []): Phase => ({ path, q, key, paging: "offset", limit: 100 });

export const services = definePdCatalogTable({
  name: "services",
  description: "PagerDuty services: what can be paged, its escalation policy, teams and incident settings",
  title: "PagerDuty Services",
  category: "services",
  keywords: "pagerduty, services, technical services, components, escalation policy, alerting, service catalog",
  docLlm:
    "Technical services (GET /services): service_id, name, description, status (active / warning / critical / " +
    "maintenance / disabled), escalation_policy_id, escalation_policy, team_ids, teams, created_at, updated_at, " +
    "last_incident_at, acknowledgement_timeout and auto_resolve_timeout (seconds), alert_creation, urgency_rule, " +
    "integration_count, url. Join escalation_policies ON escalation_policies.escalation_policy_id = " +
    "services.escalation_policy_id for the paging policy, and incidents() ON incidents.service_id = " +
    "services.service_id for history. WHERE name = 'x' pushes down as PagerDuty's name query.",
  docMd: "## services\n\nEvery technical service with its escalation policy and teams.",
  pushdown: "`name =` → `query`.",
  pk: "service_id",
  fks: [{ column: "escalation_policy_id", table: "escalation_policies", references: "escalation_policy_id" }],
  columns: [
    C.str("service_id", "PagerDuty id of the technical service (joins incidents.service_id).", "id", false),
    C.str("name", "Service name.", "name"),
    C.str("description", "Free-text summary of what the service is.", "description"),
    C.str("status", "active, warning, critical, maintenance or disabled.", "status"),
    C.str(
      "escalation_policy_id",
      "The policy that pages for this service (joins escalation_policies).",
      "escalation_policy.id",
    ),
    C.str("escalation_policy", "Escalation policy name.", "escalation_policy.summary"),
    C.strList("team_ids", "Owning team ids.", (r) => pluck(r.teams, "id")),
    C.strList("teams", "Owning team names.", (r) => pluck(r.teams, "summary")),
    C.time("created_at", "When the service was created.", "created_at"),
    C.time("updated_at", "When the service last changed.", "updated_at"),
    C.time("last_incident_at", "When its last incident was triggered.", "last_incident_timestamp"),
    C.int(
      "acknowledgement_timeout",
      "Seconds before an acknowledged incident re-triggers (NULL = never).",
      "acknowledgement_timeout",
    ),
    C.int(
      "auto_resolve_timeout",
      "Seconds before an open incident auto-resolves (NULL = never).",
      "auto_resolve_timeout",
    ),
    C.str("alert_creation", "create_incidents or create_alerts_and_incidents.", "alert_creation"),
    C.str(
      "urgency_rule",
      "Incident urgency rule: constant:high, constant:low, severity_based or use_support_hours.",
      (r) => {
        const t = at(r, "incident_urgency_rule.type");
        const u = at(r, "incident_urgency_rule.urgency");
        return t === "constant" && typeof u === "string" ? `constant:${u}` : t;
      },
    ),
    C.int("integration_count", "Number of integrations sending events to the service.", (r) =>
      Array.isArray(r.integrations) ? r.integrations.length : null,
    ),
    C.str("url", "Link to the service in the PagerDuty web UI.", "html_url"),
  ],
  plan: ({ constraints }) => [list100("/services", "services", nameQuery(constraints, "name"))],
  examples: [
    {
      sql: `SELECT name, status, escalation_policy FROM ${Q}.services WHERE status <> 'active' ORDER BY name`,
      description: "Services that are not healthy right now",
    },
    {
      sql: `SELECT s.name, count(i.incident_id) AS incidents FROM ${Q}.services s LEFT JOIN ${Q}.incidents(date_range := 'all') i ON i.service_id = s.service_id GROUP BY s.name ORDER BY incidents DESC`,
      description: "Incident count per service",
    },
  ],
});

export const escalationPolicies = definePdCatalogTable({
  name: "escalation_policies",
  description: "PagerDuty escalation policies: who gets paged, in what order, for which services",
  title: "PagerDuty Escalation Policies",
  category: "oncall",
  keywords: "pagerduty, escalation policies, escalation, paging order, on-call routing, services",
  docLlm:
    "Escalation policies (GET /escalation_policies): escalation_policy_id, name, description, num_loops, " +
    "on_call_handoff_notifications, rule_count, service_ids, team_ids, url. The rules and their targets are in " +
    "escalation_rules. WHERE name = 'x' pushes down as PagerDuty's name query.",
  docMd: "## escalation_policies\n\nEvery escalation policy; see `escalation_rules` for its levels.",
  pushdown: "`name =` → `query`.",
  pk: "escalation_policy_id",
  columns: [
    C.str("escalation_policy_id", "PagerDuty id of the escalation policy (e.g. PEP1234).", "id", false),
    C.str("name", "Policy name.", "name"),
    C.str("description", "Free-text purpose of the policy.", "description"),
    C.int("num_loops", "Count of extra passes through all rules after the last level (0 = no repeat).", "num_loops"),
    C.str("on_call_handoff_notifications", "if_has_services or always.", "on_call_handoff_notifications"),
    C.int("rule_count", "Number of escalation rules (levels).", (r) =>
      Array.isArray(r.escalation_rules) ? r.escalation_rules.length : null,
    ),
    C.strList("service_ids", "Services using the policy.", (r) => pluck(r.services, "id")),
    C.strList("team_ids", "Teams owning the policy.", (r) => pluck(r.teams, "id")),
    C.str("url", "Link to the policy in the PagerDuty web UI.", "html_url"),
  ],
  plan: ({ constraints }) => [list100("/escalation_policies", "escalation_policies", nameQuery(constraints, "name"))],
  examples: [
    {
      sql: `SELECT name, rule_count, len(service_ids) AS services FROM ${Q}.escalation_policies ORDER BY name`,
      description: "Policies with their number of levels and services",
    },
  ],
});

export const escalationRules = definePdCatalogTable({
  name: "escalation_rules",
  description: "Escalation policy rules flattened: one row per policy level and target (user or schedule)",
  title: "PagerDuty Escalation Rules",
  category: "oncall",
  keywords: "pagerduty, escalation rules, escalation levels, targets, schedules, users, paging chain",
  docLlm:
    "The levels of every escalation policy, one row per target: escalation_policy_id, escalation_policy, level " +
    "(1 = first), rule_id, escalation_delay_in_minutes, target_type (user / schedule), target_id, target_name. " +
    "WHERE escalation_policy_id = / IN reads only those policies.",
  docMd: "## escalation_rules\n\nWho each escalation policy pages at each level.",
  pushdown: "`escalation_policy_id = / IN` → `GET /escalation_policies/{id}`.",
  pk: ["escalation_policy_id", "level", "target_type", "target_id"],
  fks: [{ column: "escalation_policy_id", table: "escalation_policies", references: "escalation_policy_id" }],
  columns: [
    C.str(
      "escalation_policy_id",
      "The policy this level belongs to (joins escalation_policies).",
      (r) => r.__policy?.id,
      false,
    ),
    C.str("escalation_policy", "Escalation policy name.", (r) => r.__policy?.name),
    C.int("level", "Escalation level (1 = first).", (r) => r.__level, false),
    C.str("rule_id", "Escalation rule id.", (r) => r.__rule?.id),
    C.int(
      "escalation_delay_in_minutes",
      "Delay in minutes before an unacknowledged incident escalates past this level.",
      (r) => r.__rule?.escalation_delay_in_minutes,
    ),
    C.str("target_type", "user or schedule.", (r) => bare(r.type, "_reference"), false),
    C.str("target_id", "Target user or schedule id.", "id", false),
    C.str("target_name", "Target user or schedule name.", "summary"),
  ],
  plan: ({ constraints }) => {
    const ids = eqValues(constraints, "escalation_policy_id");
    if (ids) {
      return ids.map((id) => ({
        path: `/escalation_policies/${enc(id)}`,
        key: "escalation_policy",
        paging: "none" as const,
        notFoundEmpty: true,
      }));
    }
    return [list100("/escalation_policies", "escalation_policies")];
  },
  explode: (policy) =>
    (Array.isArray(policy.escalation_rules) ? policy.escalation_rules : []).flatMap((rule: Row, i: number) =>
      (Array.isArray(rule.targets) ? rule.targets : []).map((t: Row) => ({
        ...t,
        __policy: policy,
        __rule: rule,
        __level: i + 1,
      })),
    ),
  examples: [
    {
      sql: `SELECT escalation_policy, level, target_type, target_name FROM ${Q}.escalation_rules ORDER BY escalation_policy, level, target_name`,
      description: "The full paging chain of every policy",
    },
  ],
});

export const schedules = definePdCatalogTable({
  name: "schedules",
  description: "PagerDuty on-call schedules with their users, teams and escalation policies",
  title: "PagerDuty Schedules",
  category: "oncall",
  keywords: "pagerduty, schedules, rotations, on-call schedule, time zone, coverage",
  docLlm:
    "On-call schedules (GET /schedules): schedule_id, name, description, time_zone, escalation_policy_ids, " +
    "user_ids, users, team_ids, url. Shifts are in schedule_entries; current on-call in oncalls(). WHERE name = " +
    "'x' pushes down as PagerDuty's name query.",
  docMd: "## schedules\n\nEvery on-call schedule; `schedule_entries()` renders its shifts.",
  pushdown: "`name =` → `query`.",
  pk: "schedule_id",
  columns: [
    C.str("schedule_id", "PagerDuty id of the schedule (joins oncalls and schedule_entries).", "id", false),
    C.str("name", "Schedule name.", "name"),
    C.str("description", "Free-text purpose of the rotation.", "description"),
    C.str("time_zone", "The schedule's time zone.", "time_zone"),
    C.strList("escalation_policy_ids", "Escalation policies that use the schedule.", (r) =>
      pluck(r.escalation_policies, "id"),
    ),
    C.strList("user_ids", "Users in the schedule's rotations.", (r) => pluck(r.users, "id")),
    C.strList("users", "Names of the users in the rotations.", (r) => pluck(r.users, "summary")),
    C.strList("team_ids", "Teams owning the schedule.", (r) => pluck(r.teams, "id")),
    C.str("url", "Link to the schedule in the PagerDuty web UI.", "html_url"),
  ],
  plan: ({ constraints }) => [list100("/schedules", "schedules", nameQuery(constraints, "name"))],
  examples: [
    {
      sql: `SELECT name, time_zone, len(user_ids) AS people FROM ${Q}.schedules ORDER BY name`,
      description: "Schedules and how many people rotate through each",
    },
  ],
});

const CONTACT_TYPES: Record<string, string> = {
  email_contact_method_reference: "email",
  phone_contact_method_reference: "phone",
  sms_contact_method_reference: "sms",
  push_notification_contact_method_reference: "push",
};

export const users = definePdCatalogTable({
  name: "users",
  description: "PagerDuty users with role, teams and which contact method types they have (never the addresses)",
  title: "PagerDuty Users",
  category: "people",
  keywords: "pagerduty, users, responders, people, roles, contact methods, teams, directory",
  docLlm:
    "Users (GET /users): user_id, name, email (the login), role (owner / admin / user / limited_user / observer / " +
    "restricted_access / read_only_user …), job_title, time_zone, description, invitation_sent, team_ids, teams, " +
    "contact_method_types (distinct email / phone / sms / push — phone numbers and addresses are never read), " +
    "contact_method_count, url. WHERE name = or email = pushes down as PagerDuty's user query.",
  docMd: "## users\n\nEvery PagerDuty user. Contact methods appear as types only.",
  pushdown: "`name =` / `email =` → `query`.",
  pk: "user_id",
  columns: [
    C.str("user_id", "PagerDuty id of the user (e.g. PU1234).", "id", false),
    C.str("name", "Full name.", "name"),
    C.str("email", "Login email address.", "email"),
    C.str(
      "role",
      "Account role: owner, admin, user, limited_user, observer, restricted_access, read_only_user, ...",
      "role",
    ),
    C.str("job_title", "The user's job title from their profile.", "job_title"),
    C.str("time_zone", "The user's time zone.", "time_zone"),
    C.str("description", "Bio / description.", "description"),
    C.bool("invitation_sent", "True while the user has not accepted the invitation.", "invitation_sent"),
    C.strList("team_ids", "Ids of the teams the user belongs to.", (r) => pluck(r.teams, "id")),
    C.strList("teams", "Names of the teams the user belongs to.", (r) => pluck(r.teams, "summary")),
    C.strList("contact_method_types", "Distinct contact method types: email, phone, sms, push.", (r) =>
      Array.isArray(r.contact_methods)
        ? [...new Set(r.contact_methods.map((c: Row) => CONTACT_TYPES[c.type] ?? bare(c.type, "_reference")))].sort()
        : null,
    ),
    C.int("contact_method_count", "Number of contact methods.", (r) =>
      Array.isArray(r.contact_methods) ? r.contact_methods.length : null,
    ),
    C.str("url", "Link to the user in the PagerDuty web UI.", "html_url"),
  ],
  plan: ({ constraints }) => [list100("/users", "users", nameQuery(constraints, "email", "name"))],
  examples: [
    {
      sql: `SELECT name, role, contact_method_types FROM ${Q}.users WHERE NOT list_contains(contact_method_types, 'phone') ORDER BY name`,
      description: "Users with no phone contact method (pages may not wake them)",
    },
  ],
});

export const teams = definePdCatalogTable({
  name: "teams",
  description: "PagerDuty teams: the groups that own services, schedules and escalation policies",
  title: "PagerDuty Teams",
  category: "people",
  keywords: "pagerduty, teams, groups, ownership, organization",
  docLlm:
    "Teams (GET /teams): team_id, name, description, parent_team_id, url. Members are in team_members. WHERE " +
    "name = 'x' pushes down as PagerDuty's name query.",
  docMd: "## teams\n\nEvery team; `team_members` lists who is on each.",
  pushdown: "`name =` → `query`.",
  pk: "team_id",
  columns: [
    C.str("team_id", "PagerDuty id of the team (e.g. PT1234).", "id", false),
    C.str("name", "Team name.", "name"),
    C.str("description", "Free-text summary of what the team owns.", "description"),
    C.str("parent_team_id", "Parent team id (team hierarchies).", "parent.id"),
    C.str("url", "Link to the team in the PagerDuty web UI.", "html_url"),
  ],
  plan: ({ constraints }) => [list100("/teams", "teams", nameQuery(constraints, "name"))],
  examples: [{ sql: `SELECT name, description FROM ${Q}.teams ORDER BY name`, description: "All teams" }],
});

const memberPhase = (id: string, name?: string): Phase => ({
  path: `/teams/${enc(id)}/members`,
  key: "members",
  paging: "offset",
  limit: 100,
  ctx: name ? { team_id: id, team_name: name } : { team_id: id },
  notFoundEmpty: true,
});

export const teamMembers = definePdCatalogTable({
  name: "team_members",
  description: "Members of PagerDuty teams with their team role",
  title: "PagerDuty Team Members",
  category: "people",
  keywords: "pagerduty, team members, membership, team role, manager, responder, observer",
  docLlm:
    "Team membership (GET /teams/{id}/members): team_id, team_name, user_id, user_name, role (manager / " +
    "responder / observer). Join users ON users.user_id = team_members.user_id for contact method types and " +
    "roles, and teams ON teams.team_id = team_members.team_id. WHERE team_id = / IN reads only those teams; " +
    "otherwise every team is read (one call each).",
  docMd: "## team_members\n\nWho is on each team, and as what.",
  pushdown: "`team_id = / IN` → only those teams.",
  pk: ["team_id", "user_id"],
  fks: [
    { column: "team_id", table: "teams", references: "team_id" },
    { column: "user_id", table: "users", references: "user_id" },
  ],
  columns: [
    C.ctx("team_id", "The team (joins teams).", "team_id"),
    C.str("team_name", "Name of the team (NULL when read by team_id filter).", (r) => r.__ctx?.team_name),
    C.str("user_id", "The member (joins users).", "user.id", false),
    C.str("user_name", "Member name.", "user.summary"),
    C.str("role", "Team role: manager, responder or observer.", "role"),
  ],
  plan: ({ constraints }) => {
    const ids = eqValues(constraints, "team_id");
    return ids ? ids.map((id) => memberPhase(id)) : [{ ...list100("/teams", "teams"), child: "members" }];
  },
  children: {
    members: (team) => (typeof team.id === "string" ? memberPhase(team.id, asStr(team.name) ?? undefined) : null),
  },
  examples: [
    {
      sql: `SELECT team_name, count(*) FILTER (role = 'manager') AS managers, count(*) AS members FROM ${Q}.team_members GROUP BY team_name ORDER BY team_name`,
      description: "Team sizes and managers",
    },
  ],
});

export const businessServices = definePdCatalogTable({
  name: "business_services",
  description: "PagerDuty business services: the customer-facing capabilities technical services support",
  title: "PagerDuty Business Services",
  category: "services",
  keywords: "pagerduty, business services, service graph, customer impact, status, capabilities",
  docLlm:
    "Business services (GET /business_services): business_service_id, name, description, point_of_contact, " +
    "team_id, url. Join teams ON teams.team_id = business_services.team_id for the owning team; the graph " +
    "edges to technical services are in service_dependencies (dependent_service_id = business_service_id).",
  docMd: "## business_services\n\nBusiness services; see `service_dependencies` for the graph.",
  pk: "business_service_id",
  fks: [{ column: "team_id", table: "teams", references: "team_id" }],
  columns: [
    C.str("business_service_id", "PagerDuty id of the business service (e.g. PBS1234).", "id", false),
    C.str("name", "Business service name as shown on the service graph.", "name"),
    C.str("description", "Free-text summary of what the business service covers.", "description"),
    C.str(
      "point_of_contact",
      "Who to contact about the business service (free text, often an email or channel).",
      "point_of_contact",
    ),
    C.str("team_id", "Owning team id.", "team.id"),
    C.str("url", "Link in the PagerDuty web UI.", "html_url"),
  ],
  plan: () => [list100("/business_services", "business_services")],
  examples: [
    {
      sql: `SELECT name, point_of_contact FROM ${Q}.business_services ORDER BY name`,
      description: "All business services",
    },
  ],
});

const depPhase = (kind: "business_services" | "technical_services", id: string): Phase => ({
  path: `/service_dependencies/${kind}/${enc(id)}`,
  key: "relationships",
  paging: "none",
  ctx: { parent_id: id },
  notFoundEmpty: true,
});

export const serviceDependencies = definePdCatalogTable({
  name: "service_dependencies",
  description: "The PagerDuty service graph: one row per dependency edge between business and technical services",
  title: "PagerDuty Service Dependencies",
  category: "services",
  keywords: "pagerduty, service dependencies, service graph, dependency graph, blast radius, impact, topology",
  docLlm:
    "Service dependency edges (GET /service_dependencies/business_services/{id} and /technical_services/{id} for " +
    "every service): relationship_id, dependent_service_id + dependent_service_type (business_service / service) " +
    "depends on supporting_service_id + supporting_service_type. Each edge appears once. Use a recursive CTE " +
    "over it for blast radius ('which business services does Payments support?'). One call per service.",
  docMd: "## service_dependencies\n\nDependent → supporting edges of the service graph, each once.",
  pk: "relationship_id",
  columns: [
    C.str("relationship_id", "Dependency id.", "id", false),
    C.str("dependent_service_id", "The service that depends on the other.", "dependent_service.id"),
    C.str("dependent_service_type", "business_service or service (technical).", (r) =>
      bare(at(r, "dependent_service.type"), "_reference"),
    ),
    C.str("supporting_service_id", "The service it depends on.", "supporting_service.id"),
    C.str("supporting_service_type", "business_service or service (technical).", (r) =>
      bare(at(r, "supporting_service.type"), "_reference"),
    ),
  ],
  plan: () => [
    { ...list100("/business_services", "business_services"), child: "business" },
    { ...list100("/services", "services"), child: "technical" },
  ],
  children: {
    business: (p) => (typeof p.id === "string" ? depPhase("business_services", p.id) : null),
    technical: (p) => (typeof p.id === "string" ? depPhase("technical_services", p.id) : null),
  },
  // Both ends report an edge; keep it only from its dependent's listing.
  keep: (r) => at(r, "dependent_service.id") === r.__ctx?.parent_id,
  examples: [
    {
      sql: `SELECT b.name AS business_service, s.name AS supported_by FROM ${Q}.service_dependencies d JOIN ${Q}.business_services b ON b.business_service_id = d.dependent_service_id JOIN ${Q}.services s ON s.service_id = d.supporting_service_id ORDER BY 1, 2`,
      description: "Which technical services each business service depends on",
    },
  ],
});

/** past / ongoing / future relative to the scan's start. */
function windowStatus(r: Row, nowMs: number): string | null {
  const [s, e] = [epochMs(r.start_time), epochMs(r.end_time)];
  if (s === null || e === null) return null;
  return e <= nowMs ? "past" : s > nowMs ? "future" : "ongoing";
}

export const maintenanceWindows = definePdCatalogTable({
  name: "maintenance_windows",
  description: "PagerDuty maintenance windows (planned periods when services do not page)",
  title: "PagerDuty Maintenance Windows",
  category: "changes",
  keywords: "pagerduty, maintenance windows, planned maintenance, silence, downtime, suppression",
  docLlm:
    "Maintenance windows (GET /maintenance_windows): maintenance_window_id, sequence_number, description, " +
    "start_time, end_time, status (past / ongoing / future, relative to the query's start), created_by, " +
    "service_ids, services, team_ids, url. WHERE status = 'ongoing' (or past / future) pushes down as " +
    "PagerDuty's filter.",
  docMd: "## maintenance_windows\n\nPast, ongoing and future maintenance windows.",
  pushdown: "`status =` → `filter` (past / ongoing / future).",
  pk: "maintenance_window_id",
  columns: [
    C.str("maintenance_window_id", "PagerDuty id of the maintenance window.", "id", false),
    C.big(
      "sequence_number",
      "Account-wide ordinal of the window (1 = first created), as shown in the UI.",
      "sequence_number",
    ),
    C.str("description", "Why the services are in maintenance.", "description"),
    C.time("start_time", "When paging is suppressed from.", "start_time"),
    C.time("end_time", "When paging resumes.", "end_time"),
    C.str("status", "past, ongoing or future (relative to when the query started).", (r) =>
      r.__ctx?.now ? windowStatus(r, Number(r.__ctx.now)) : null,
    ),
    C.str("created_by", "Who created it.", "created_by.summary"),
    C.strList("service_ids", "Services in maintenance.", (r) => pluck(r.services, "id")),
    C.strList("services", "Names of the services in maintenance.", (r) => pluck(r.services, "summary")),
    C.strList("team_ids", "Teams.", (r) => pluck(r.teams, "id")),
    C.str("url", "Link in the PagerDuty web UI.", "html_url"),
  ],
  plan: ({ constraints, anchorMs }) => {
    const st = allowed(constraints, "status", ["past", "ongoing", "future"]);
    const filter = st?.length === 1 && st[0] ? st[0] : "all";
    return [
      {
        ...list100("/maintenance_windows", "maintenance_windows", [["filter", filter]]),
        ctx: { now: String(anchorMs) },
      },
    ];
  },
  examples: [
    {
      sql: `SELECT description, start_time, end_time, services FROM ${Q}.maintenance_windows WHERE status = 'ongoing' ORDER BY start_time`,
      description: "Maintenance windows in effect right now",
    },
  ],
});

export const priorities = definePdCatalogTable({
  name: "priorities",
  description: "The account's incident priorities (P1, P2, ...) in rank order",
  title: "PagerDuty Priorities",
  category: "incidents",
  keywords: "pagerduty, priorities, severity, p1, p2, sev, incident priority",
  docLlm:
    "Incident priorities (GET /priorities): priority_id, name (e.g. P1), description, rank (1 = most severe, PagerDuty's order).",
  docMd: "## priorities\n\nIncident priorities, most severe first.",
  pk: "priority_id",
  columns: [
    C.str("priority_id", "PagerDuty id of the priority (joins incidents.priority_id).", "id", false),
    C.str("name", "Priority name, e.g. P1.", "name"),
    C.str("description", "What the priority level means in this account.", "description"),
    C.int("rank", "Position in PagerDuty's order (1 = most severe).", (r) => (r.__pos ?? 0) + 1),
  ],
  plan: () => [list100("/priorities", "priorities")],
  examples: [
    { sql: `SELECT rank, name, description FROM ${Q}.priorities ORDER BY rank`, description: "Priorities in order" },
  ],
});

export const tags = definePdCatalogTable({
  name: "tags",
  description: "PagerDuty tags (labels attached to users, teams and escalation policies)",
  title: "PagerDuty Tags",
  category: "people",
  keywords: "pagerduty, tags, labels, metadata",
  docLlm: "Tags (GET /tags): tag_id, label. WHERE label = 'x' pushes down as PagerDuty's query.",
  docMd: "## tags\n\nEvery tag label.",
  pushdown: "`label =` → `query`.",
  pk: "tag_id",
  columns: [C.str("tag_id", "PagerDuty id of the tag.", "id", false), C.str("label", "Tag label.", "label")],
  plan: ({ constraints }) => [list100("/tags", "tags", nameQuery(constraints, "label"))],
  examples: [{ sql: `SELECT label FROM ${Q}.tags ORDER BY label`, description: "All tag labels" }],
});

export const statusPages = definePdCatalogTable({
  name: "status_pages",
  description: "PagerDuty status pages (public and private), when the account has Status Pages",
  title: "PagerDuty Status Pages",
  category: "services",
  keywords: "pagerduty, status pages, public status, customer communication",
  docLlm:
    "Status pages (GET /status_pages; needs Status Pages — check account_features): status_page_id, name, " +
    "status_page_type (public / private), url, published_at.",
  docMd: "## status_pages\n\nThe account's status pages.",
  pk: "status_page_id",
  feature: "The Status Pages API",
  columns: [
    C.str("status_page_id", "PagerDuty id of the status page.", "id", false),
    C.str("name", "Title shown on the status page.", "name"),
    C.str("status_page_type", "public or private.", "status_page_type"),
    C.str("url", "The status page URL.", "url"),
    C.time("published_at", "When it was published.", "published_at"),
  ],
  plan: () => [list100("/status_pages", "status_pages")],
  examples: [
    { sql: `SELECT name, status_page_type, url FROM ${Q}.status_pages ORDER BY name`, description: "Status pages" },
  ],
});

/** The add-on APIs account_features probes, and what each one powers. */
export const FEATURE_PROBES: {
  feature: string;
  usedBy: string;
  path: string;
  method?: "POST";
  body?: (now: number) => unknown;
}[] = [
  { feature: "rest_api", usedBy: "every function", path: "/priorities" },
  {
    feature: "analytics",
    usedBy: "analytics_incidents, analytics_incident_metrics",
    path: "/analytics/raw/incidents",
    method: "POST",
    body: (now) => ({ filters: { created_at_start: iso(now - DAY), created_at_end: iso(now) }, limit: 1 }),
  },
  { feature: "audit_records", usedBy: "audit_records", path: "/audit/records" },
  { feature: "status_pages", usedBy: "status_pages", path: "/status_pages" },
  { feature: "business_services", usedBy: "business_services, service_dependencies", path: "/business_services" },
  { feature: "change_events", usedBy: "change_events", path: "/change_events" },
];

export const accountFeatures = definePdCatalogTable({
  name: "account_features",
  description: "Which PagerDuty add-on APIs (Analytics, Audit Records, Status Pages, ...) this account and key can use",
  title: "PagerDuty Account Features",
  category: "admin",
  keywords: "pagerduty, features, plan, entitlements, analytics, audit, status pages, capabilities, permissions",
  docLlm:
    "Probes each optional PagerDuty API with one tiny request and reports feature, available, http_status, detail " +
    "and used_by (the functions that need it). Features: rest_api, analytics, audit_records, status_pages, " +
    "business_services, change_events. Run it first when analytics_*, audit_records or status_pages fail: a 402 " +
    "or 403 means the plan or the key's role lacks that API. An invalid key fails the query (HTTP 401).",
  docMd: "## account_features\n\nFeature detection for the plan-dependent APIs.",
  pk: "feature",
  columns: [
    C.str(
      "feature",
      "rest_api, analytics, audit_records, status_pages, business_services or change_events.",
      "feature",
      false,
    ),
    C.bool("available", "True when the probe succeeded.", "available"),
    C.int(
      "http_status",
      "HTTP status code the probe request returned (200 = available, 402/403 = not on plan or role).",
      "http_status",
    ),
    C.str("detail", "PagerDuty's error message when unavailable.", "detail"),
    C.str("used_by", "Functions and tables that need the feature.", "used_by"),
  ],
  plan: () => [],
  custom: async (cfg: PdConfig, scan) => {
    const rows: Row[] = [];
    for (const p of FEATURE_PROBES) {
      try {
        await pdRequest(cfg, p.path, {
          method: p.method,
          query: p.method ? undefined : [["limit", "1"]],
          body: p.body?.(scan.anchorMs),
        });
        rows.push({ feature: p.feature, available: true, http_status: 200, detail: null, used_by: p.usedBy });
      } catch (err) {
        if (!(err instanceof PdError) || err.status === 0 || err.status === 401 || err.status >= 500) throw err;
        rows.push({
          feature: p.feature,
          available: false,
          http_status: err.status,
          detail: err.message,
          used_by: p.usedBy,
        });
      }
    }
    return rows;
  },
  examples: [
    {
      sql: `SELECT feature, available, used_by FROM ${Q}.account_features ORDER BY feature`,
      description: "Which optional APIs this key can use",
    },
  ],
});

export const FUNCTIONS: VgiFunction[] = [
  incidents,
  incidentLogEntries,
  incidentAlerts,
  incidentNotes,
  oncalls,
  scheduleEntries,
  changeEvents,
  auditRecords,
  analyticsIncidents,
  analyticsIncidentMetrics,
];

export const TABLES = [
  services,
  escalationPolicies,
  escalationRules,
  schedules,
  users,
  teams,
  teamMembers,
  businessServices,
  serviceDependencies,
  maintenanceWindows,
  priorities,
  tags,
  statusPages,
  accountFeatures,
];
