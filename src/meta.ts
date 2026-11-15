/**
 * Discovery metadata: the one file to edit when you rename or extend the worker.
 *
 * DuckDB, coding agents and the `vgi-lint` metadata checker all read these
 * tags. Keep every object documented and `make lint-explain` stays at 100.
 *
 * - Catalog-level tags (title, license, author, links, long description) live in
 *   `CATALOG_TAGS` below.
 * - Every function carries `objectTags(...)`: a title, a category from
 *   `SCHEMA_CATEGORIES`, search keywords, and LLM- and human-facing docs.
 */

import { field, type VgiDataType, type VgiField } from "@query-farm/vgi";

export const CATALOG_NAME = "pagerduty";
export const VERSION = "0.1.0";

export const SOURCE_URL = "https://github.com/Query-farm/vgi-pagerduty";

/**
 * Categories group functions in docs and agent tools. Every object's
 * `vgi.category` must name one of these.
 */
export const SCHEMA_CATEGORIES = [
  {
    name: "incidents",
    title: "Incidents",
    description: "Incidents, their timelines (log entries), alerts, notes and priorities.",
  },
  {
    name: "oncall",
    title: "On-Call",
    description: "Who is on call now or was on call when: on-calls, schedules, shifts and escalation policies.",
  },
  { name: "services", title: "Services", description: "Technical and business services and the service graph." },
  { name: "people", title: "People", description: "Users, teams, team membership and tags." },
  { name: "changes", title: "Changes", description: "Change events (deploys) and maintenance windows." },
  {
    name: "analytics",
    title: "Analytics",
    description: "PagerDuty Analytics API: per-incident and aggregate MTTA, MTTR and interruptions.",
  },
  { name: "admin", title: "Administration", description: "Audit records and add-on API feature detection." },
] as const;

export type Category = (typeof SCHEMA_CATEGORIES)[number]["name"];

/** Turn `"a, b, c"` into the JSON array string `vgi.keywords` requires. */
export function keywordsArray(keywords: string): string {
  return JSON.stringify(
    keywords
      .split(",")
      .map((k) => k.trim())
      .filter(Boolean),
  );
}

/** Build the standard per-object discovery tags. */
export function objectTags(opts: {
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
}): Record<string, string> {
  return {
    "vgi.title": opts.title,
    "vgi.category": opts.category,
    "vgi.keywords": keywordsArray(opts.keywords),
    "vgi.doc_llm": opts.docLlm,
    "vgi.doc_md": opts.docMd,
  };
}

/** Build an Arrow field whose `comment` shows up in DuckDB's `DESCRIBE` and agent tools. */
export function column(name: string, type: VgiDataType, comment: string, nullable = true): VgiField {
  return field(name, type, nullable, new Map([["comment", comment]]));
}

const SOURCES_MD =
  "### Sources and references\n\n" +
  "- [PagerDuty REST API v2](https://developer.pagerduty.com/api-reference/) — the endpoints this worker reads\n" +
  "- [Pagination](https://developer.pagerduty.com/docs/pagination) (offset/limit/more and cursor) and " +
  "[rate limits](https://developer.pagerduty.com/docs/rest-api-rate-limits)\n" +
  "- [Authentication](https://developer.pagerduty.com/docs/authentication) (REST API keys and OAuth) and " +
  "[service regions](https://support.pagerduty.com/main/docs/service-regions)\n" +
  "- [Analytics API](https://developer.pagerduty.com/api-reference/c2NoOjI3NDgwODk-analytics) and " +
  "[Audit Records](https://developer.pagerduty.com/docs/audit-records-api)";

export const CATALOG_COMMENT =
  "PagerDuty incident management as SQL: incidents and their timelines, on-call, schedules, services, teams, change events, analytics and audit records, read-only from the PagerDuty REST API.";

export const CATALOG_DOC_LLM =
  "Read-only SQL access to a PagerDuty account through the REST API v2. Reach for it to answer incident " +
  "and on-call questions: how many incidents a service had, MTTA / MTTR (incidents(with_timeline := true) or " +
  "the Analytics API), who acknowledged or escalated what (incident_log_entries), which alerts were noisy, " +
  "who is on call now or was on call when (oncalls, schedule_entries), escalation chains (escalation_rules), " +
  "service ownership and the business-service dependency graph, recent deploys (change_events) and " +
  "configuration changes (audit_records). Every object needs a `pagerduty` DuckDB secret: API_TOKEN (a REST " +
  "API key, sent as `Token token=`) or OAUTH_TOKEN (Bearer), REGION 'us' (default) or 'eu', or BASE_URL. " +
  "Time-windowed functions default to the last 30 days; a WHERE filter on their time column or since := / " +
  "until := picks the window, and long windows are chunked to PagerDuty's limits. WHERE filters on status, " +
  "urgency, service_id and similar push down into PagerDuty's array parameters (statuses[] …) and are " +
  "re-checked exactly. Incident rows use a shared vocabulary (incident_id, title, status triggered / " +
  "acknowledged / resolved, urgency, priority, service, created_at, acknowledged_at, resolved_at, " +
  "time_to_ack_seconds, time_to_resolve_seconds, assignees, url) so they UNION with other incident workers. " +
  "account_features shows whether the plan has Analytics, Audit Records and Status Pages. Upstream docs: " +
  "https://developer.pagerduty.com/api-reference/ ; rate limits " +
  "https://developer.pagerduty.com/docs/rest-api-rate-limits .";

export const CATALOG_DOC_MD =
  "# PagerDuty\n\n" +
  "Query a PagerDuty account from DuckDB: incidents with time to acknowledge and resolve, incident timelines, " +
  "alerts and notes, who is on call, schedules and escalation policies, services and the business-service " +
  "graph, users and teams, change events, maintenance windows, Analytics metrics and audit records. " +
  "Read-only.\n\n" +
  "Create a `pagerduty` secret with a REST API key (`API_TOKEN`) or an OAuth token (`OAUTH_TOKEN`); add " +
  "`REGION 'eu'` for EU-hosted accounts. Filters are translated into PagerDuty's query parameters and time " +
  "windows, offset and cursor paging are resumable, and 429 rate limits are retried after the reset.\n\n" +
  SOURCES_MD;

export const SCHEMA_DOC_LLM =
  "PagerDuty REST API v2 collections as SQL. Account-wide collections without arguments are tables " +
  "(services, users, teams, schedules, escalation_policies, escalation_rules, team_members, business_services, " +
  "service_dependencies, maintenance_windows, priorities, tags, status_pages, account_features). Time-windowed " +
  "or per-incident data are table functions with named arguments: incidents(), incident_log_entries(), " +
  "incident_alerts(), incident_notes(), oncalls(), schedule_entries(), change_events(), audit_records(), " +
  "analytics_incidents(), analytics_incident_metrics(). Per-parent collections take the parent id as a named " +
  "argument or a pushed `= / IN` filter and otherwise read every parent (one extra call each). All objects " +
  "need a `pagerduty` secret.";

export const SCHEMA_DOC_MD =
  "## main\n\n" +
  "PagerDuty incidents, on-call, services, people, changes, analytics and audit records.\n\n" +
  "Tables need no arguments; table functions take a time window (`since := `, `until := `, or a WHERE filter " +
  "on their time column) or a parent id.\n\n" +
  "All objects need a `pagerduty` secret; filters push down into PagerDuty's query parameters.";

export const AGENT_TEST_TASKS = [
  {
    name: "open_incidents",
    prompt: "Which PagerDuty incidents are still open (not resolved)? Give their numbers and titles.",
  },
  {
    name: "incidents_per_service",
    prompt: "How many PagerDuty incidents has each service had in total, across all time?",
  },
  {
    name: "mtta_checkout",
    prompt: "What was the time to acknowledge, in seconds, of PagerDuty incident number 1042?",
  },
  { name: "who_acked", prompt: "Who acknowledged PagerDuty incident Q2CHECKOUT500?" },
  { name: "incident_alert_count", prompt: "How many alerts does incident Q2CHECKOUT500 have, by severity?" },
  { name: "oncall_now", prompt: "Who is the first-level on-call for the Checkout escalation policy right now?" },
  {
    name: "escalation_chain",
    prompt: "List the levels and targets of the Payments escalation policy in order.",
  },
  { name: "team_members", prompt: "Who are the managers of PagerDuty teams?" },
  {
    name: "business_dependencies",
    prompt: "Which technical services does the Online Store business service depend on?",
  },
  { name: "users_without_phone", prompt: "Which PagerDuty users have no phone contact method?" },
  { name: "priorities_order", prompt: "List the incident priorities from most to least severe." },
  { name: "features", prompt: "Does this PagerDuty account have the Analytics API available?" },
  {
    name: "monthly_mtta",
    prompt:
      "Using PagerDuty Analytics, what was the incident count and mean time to acknowledge per month from September to November 2026?",
  },
  {
    name: "sleep_interruptions",
    prompt:
      "Using PagerDuty Analytics, which incidents created during 2026 woke someone up (sleep-hour interruptions)?",
  },
  { name: "recent_deletes", prompt: "Who deleted anything in PagerDuty during November 2026, and what was it?" },
  {
    name: "deploy_before_incident",
    prompt: "Which change events were recorded on the Checkout API service between 1 and 15 November 2026?",
  },
  { name: "policy_services", prompt: "Which services use the Checkout escalation policy?" },
  { name: "incident_notes", prompt: "What notes did responders add to incident Q2CHECKOUT500?" },
  { name: "ongoing_maintenance", prompt: "Which PagerDuty maintenance windows are in effect right now?" },
  {
    name: "oncall_hours",
    prompt: "How many hours was each person on the Payments Primary schedule between 1 and 29 November 2026?",
  },
  { name: "schedule_time_zones", prompt: "What time zone does each PagerDuty schedule use?" },
  { name: "status_page_list", prompt: "Which public status pages does the PagerDuty account have?" },
  { name: "tag_labels", prompt: "List the PagerDuty tag labels." },
  { name: "team_hierarchy", prompt: "Which PagerDuty teams have a parent team, and which one?" },
];

/** Bind-only probes: each binds the function and shows its columns without fetching (LIMIT 0). */
export const EXECUTABLE_EXAMPLES = [
  {
    name: "incidents_columns",
    description: "The incidents() result columns (binds only; create a pagerduty secret and drop LIMIT 0 for rows)",
    sql: "SELECT incident_id, title, status, urgency, time_to_ack_seconds FROM pagerduty.main.incidents() LIMIT 0",
  },
  {
    name: "oncalls_columns",
    description: "The oncalls() result columns (binds only; create a pagerduty secret and drop LIMIT 0 for rows)",
    sql: "SELECT user_name, escalation_policy_name, escalation_level FROM pagerduty.main.oncalls() LIMIT 0",
  },
];

export const CATALOG_TAGS: Record<string, string> = {
  "vgi.title": "PagerDuty Incident Management",
  "vgi.subtitle": "PagerDuty incidents, on-call, schedules, services, teams, changes and analytics as SQL tables.",
  "vgi.doc_llm": CATALOG_DOC_LLM,
  "vgi.doc_md": CATALOG_DOC_MD,
  "vgi.source_url": SOURCE_URL,
  "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
  "vgi.license": "0BSD",
  "vgi.author": "Query Farm LLC",
  "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
  "vgi.keywords": keywordsArray(
    "pagerduty, incident management, incidents, on-call, oncall, sre, mtta, mttr, escalation, schedules, alerts, change events, devops",
  ),
  "vgi.support_contact": `${SOURCE_URL}/issues`,
  "vgi.support_policy_url": `${SOURCE_URL}#support`,
  // Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
  "vgi.agent_test_tasks": JSON.stringify(AGENT_TEST_TASKS),
  "vgi.executable_examples": JSON.stringify(EXECUTABLE_EXAMPLES),
};

export const SCHEMA_TAGS: Record<string, string> = {
  "vgi.title": "PagerDuty REST API",
  domain: "incident-management",
  "vgi.keywords": keywordsArray("pagerduty, incidents, on-call, services, users, teams, schedules, analytics, audit"),
  "vgi.doc_llm": SCHEMA_DOC_LLM,
  "vgi.doc_md": SCHEMA_DOC_MD,
  "vgi.categories": JSON.stringify(SCHEMA_CATEGORIES),
};
