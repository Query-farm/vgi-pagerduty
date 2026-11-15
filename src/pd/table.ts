/**
 * One factory for every PagerDuty list endpoint.
 *
 * A spec names the columns (with a JSON extractor each) and turns the arguments and
 * the pushed filters into a queue of request *phases*. The factory turns that into a
 * VGI table function that:
 *
 * - streams one PagerDuty page per Arrow batch (no empty batches);
 * - keeps the whole cursor (the pending phases, each with its own `offset`, `cursor`
 *   or analytics `starting_after`) in serializable state, so a scan resumes on any
 *   process over HTTP and survives rate-limit pauses;
 * - splits time windows: PagerDuty caps a date range (6 months for incidents, 31 days
 *   for audit records, 1 year for analytics) and offset paging at 10,000 rows, so a
 *   long window is chunked up front and a chunk whose `total` exceeds the offset cap
 *   is halved until it fits;
 * - fans out over parents (incidents → alerts/notes, teams → members, schedules →
 *   entries) one parent page at a time, so the queue stays small;
 * - reads the `pagerduty` secret on every call and never stores it in state;
 * - pushes filters down (narrowing only) and lets the SDK re-apply them exactly.
 */

import {
  batchFromColumns,
  bool,
  constraintRecord,
  defineTableFunction,
  field,
  float64,
  formatPushedFilters,
  int32,
  int64,
  list,
  schema,
  struct,
  type TableDescriptor,
  timestampMicros,
  utf8,
  type VgiDataType,
  type VgiFunction,
} from "@query-farm/vgi";
import { log } from "../log.ts";
import { type Category, column, objectTags } from "../meta.ts";
import { configFromSecrets, type PdConfig, PdError, pdRequest, type Query, SECRET_TYPE } from "./client.ts";
import { type Constraint, constraintsOf } from "./pushdown.ts";

// ------------------------------------------------------------------ column helpers

export interface Col {
  name: string;
  type: VgiDataType;
  /** DuckDB spelling for vgi.result_columns_schema. */
  sqlType: string;
  doc: string;
  nullable?: boolean;
  get: (row: Row) => unknown;
}

// biome-ignore lint/suspicious/noExplicitAny: PagerDuty JSON is untyped.
export type Row = Record<string, any> & { __ctx?: Record<string, string>; __pos?: number };

/** Walk a dotted path through untyped JSON. */
export function at(row: unknown, path: string): unknown {
  if (path === "") return row;
  let cur: unknown = row;
  for (const p of path.split(".")) {
    if (cur == null || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[p];
  }
  return cur;
}

export const asStr = (v: unknown): string | null =>
  v == null ? null : typeof v === "string" ? v : typeof v === "object" ? JSON.stringify(v) : String(v);
export const asBool = (v: unknown): boolean | null => (typeof v === "boolean" ? v : null);
export const asInt = (v: unknown): number | null =>
  typeof v === "number" && Number.isFinite(v) ? Math.trunc(v) : null;
export const asBig = (v: unknown): bigint | null =>
  typeof v === "number" && Number.isFinite(v) ? BigInt(Math.trunc(v)) : null;
export const asNum = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
/** ISO 8601 → epoch milliseconds. */
export function epochMs(v: unknown): number | null {
  if (typeof v !== "string" || v === "") return null;
  const ms = Date.parse(v);
  return Number.isNaN(ms) ? null : ms;
}
/** ISO 8601 → TIMESTAMPTZ microseconds. */
export function asTime(v: unknown): bigint | null {
  const ms = epochMs(v);
  return ms === null ? null : BigInt(ms) * 1000n;
}
/** Seconds between two ISO timestamps (null when either is missing). */
export function secondsBetween(from: unknown, to: unknown): number | null {
  const [a, b] = [epochMs(from), epochMs(to)];
  return a === null || b === null ? null : (b - a) / 1000;
}
export const asStrList = (v: unknown): string[] | null =>
  Array.isArray(v) ? v.filter((x) => x != null).map((x) => (typeof x === "string" ? x : JSON.stringify(x))) : null;
export const asJson = (v: unknown): string | null => (v == null ? null : JSON.stringify(v));
/** Map a list of references to one of their fields (`teams` → ids). */
export const pluck = (v: unknown, path: string): string[] | null =>
  Array.isArray(v) ? v.map((x) => asStr(at(x, path))).filter((x): x is string => x !== null) : null;

const JSON_META: [string, string] = ["ARROW:extension:name", "arrow.json"];

type Getter = string | ((r: Row) => unknown);
const read = (r: Row, g: Getter) => (typeof g === "string" ? at(r, g) : g(r));

function mkCol(
  name: string,
  type: VgiDataType,
  sqlType: string,
  doc: string,
  get: (r: Row) => unknown,
  nullable = true,
) {
  return { name, type, sqlType, doc, get, nullable };
}

export const C = {
  str: (name: string, doc: string, g: Getter, nullable = true): Col =>
    mkCol(name, utf8(), "VARCHAR", doc, (r) => asStr(read(r, g)), nullable),
  time: (name: string, doc: string, g: Getter): Col =>
    mkCol(name, timestampMicros("UTC"), "TIMESTAMP WITH TIME ZONE", doc, (r) => asTime(read(r, g))),
  bool: (name: string, doc: string, g: Getter): Col => mkCol(name, bool, "BOOLEAN", doc, (r) => asBool(read(r, g))),
  int: (name: string, doc: string, g: Getter, nullable = true): Col =>
    mkCol(name, int32, "INTEGER", doc, (r) => asInt(read(r, g)), nullable),
  big: (name: string, doc: string, g: Getter): Col => mkCol(name, int64(), "BIGINT", doc, (r) => asBig(read(r, g))),
  num: (name: string, doc: string, g: Getter): Col => mkCol(name, float64(), "DOUBLE", doc, (r) => asNum(read(r, g))),
  strList: (name: string, doc: string, g: Getter): Col =>
    mkCol(name, list(utf8()), "VARCHAR[]", doc, (r) => asStrList(read(r, g))),
  json: (name: string, doc: string, g: Getter): Col => mkCol(name, utf8(), "JSON", doc, (r) => asJson(read(r, g))),
  ctx: (name: string, doc: string, key: string): Col =>
    mkCol(name, utf8(), "VARCHAR", doc, (r) => r.__ctx?.[key] ?? null, false),
  structList: (name: string, doc: string, fields: { name: string; path: string }[], g: Getter): Col =>
    mkCol(
      name,
      list(struct(fields.map((f) => field(f.name, utf8())) as never)),
      `STRUCT(${fields.map((f) => `${f.name} VARCHAR`).join(", ")})[]`,
      doc,
      (r) => {
        const v = read(r, g);
        if (!Array.isArray(v)) return null;
        return v.map((item) => Object.fromEntries(fields.map((f) => [f.name, asStr(at(item, f.path))])));
      },
    ),
};

// ----------------------------------------------------------------------- phases

/** PagerDuty's offset paging stops at offset + limit = 10,000. */
export const OFFSET_CAP = 10_000;
/** A window this short is not split further. */
const MIN_SPLIT_MS = 60_000;

export interface TimeWindow {
  /** Inclusive lower bound, epoch ms. */
  lo: number;
  /** Exclusive upper bound, epoch ms. */
  hi: number;
  /** How the window travels: `since`/`until` query parameters or analytics body filters. */
  style: "query" | "analytics";
  /** Newest-first: split halves are queued newer half first. */
  desc?: boolean;
}

/** One request stream of a scan (one endpoint, one window, one parent). */
export interface Phase {
  path: string;
  q?: Query;
  method?: "GET" | "POST";
  body?: Record<string, unknown>;
  /** Dotted path of the row array in the response (`incidents`, `schedule.final_schedule.rendered_schedule_entries`). */
  key: string;
  paging: "offset" | "cursor" | "analytics" | "none";
  limit?: number;
  ctx?: Record<string, string>;
  /** The rows are parents: each becomes a child phase via `spec.children[child]`. */
  child?: string;
  window?: TimeWindow;
  /** A 404 means "no such object": zero rows rather than an error. */
  notFoundEmpty?: boolean;
  offset?: number;
  cursor?: string;
}

export const iso = (ms: number) => new Date(ms).toISOString();

/** Cut [lo, hi) into consecutive windows of at most `maxMs`, in scan order. */
export function chunkWindow(lo: number, hi: number, maxMs: number, desc = false): [number, number][] {
  const out: [number, number][] = [];
  for (let a = lo; a < hi; a += maxMs) out.push([a, Math.min(a + maxMs, hi)]);
  return desc ? out.reverse() : out;
}

/** Halve a window that holds more rows than offset paging can reach, or undefined if it is too short. */
export function splitWindow(w: TimeWindow): TimeWindow[] | undefined {
  if (w.hi - w.lo < MIN_SPLIT_MS) return undefined;
  const mid = w.lo + Math.floor((w.hi - w.lo) / 2);
  const halves = [
    { ...w, hi: mid },
    { ...w, lo: mid },
  ];
  return w.desc ? halves.reverse() : halves;
}

/** The HTTP request for the phase's next page. */
export function pageRequest(p: Phase): { path: string; query: Query; body?: unknown; method: "GET" | "POST" } {
  const query: Query = [...(p.q ?? [])];
  let body: Record<string, unknown> | undefined = p.body ? { ...p.body } : undefined;
  if (p.window?.style === "query") query.push(["since", iso(p.window.lo)], ["until", iso(p.window.hi)]);
  if (p.paging === "offset") {
    query.push(["limit", String(p.limit ?? 100)], ["offset", String(p.offset ?? 0)]);
    if (p.window && !p.offset) query.push(["total", "true"]);
  } else if (p.paging === "cursor") {
    query.push(["limit", String(p.limit ?? 100)]);
    if (p.cursor) query.push(["cursor", p.cursor]);
  } else if (p.paging === "analytics") {
    body = { ...(body ?? {}), limit: p.limit ?? 1000 };
    if (p.window) {
      const filters = { ...((body.filters as Record<string, unknown>) ?? {}) };
      filters.created_at_start = iso(p.window.lo);
      filters.created_at_end = iso(p.window.hi);
      body.filters = filters;
    }
    if (p.cursor) body.starting_after = p.cursor;
  }
  return { path: p.path, query, body, method: p.method ?? "GET" };
}

/** Rows of a response body at a dotted key (an object becomes one row). */
export function rowsAt(body: unknown, key: string): Row[] {
  const v = at(body, key);
  if (Array.isArray(v)) return v.filter((x) => x && typeof x === "object") as Row[];
  if (v && typeof v === "object") return [v as Row];
  return [];
}

/** Advance a phase past the page it just read; true when the phase is finished. */
export function advance(p: Phase, body: unknown, rows: Row[]): boolean {
  const b = (body ?? {}) as Record<string, unknown>;
  if (p.paging === "offset") {
    if (b.more !== true || rows.length === 0) return true;
    p.offset = (p.offset ?? 0) + rows.length;
    if (p.offset + (p.limit ?? 100) > OFFSET_CAP) {
      throw new PdError(
        `${p.path}: more than ${OFFSET_CAP} rows match and PagerDuty's offset paging stops there; ` +
          "narrow the query (a shorter since/until window or a created_at filter)",
      );
    }
    return false;
  }
  if (p.paging === "cursor") {
    if (typeof b.next_cursor !== "string" || !b.next_cursor || rows.length === 0) return true;
    p.cursor = b.next_cursor;
    return false;
  }
  if (p.paging === "analytics") {
    if (b.more !== true || typeof b.last !== "string" || !b.last) return true;
    p.cursor = b.last;
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------- the spec

export interface ScanContext<A> {
  args: A;
  constraints: Constraint[];
  /** Wall-clock anchor fixed when the scan started (epoch ms), stable across resumes. */
  anchorMs: number;
}

export interface FnExample {
  sql: string;
  description: string;
}

export interface PdTableSpec<A> {
  name: string;
  description: string;
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
  args?: Record<string, VgiDataType>;
  argDocs?: Record<string, string>;
  argDefaults?: Record<string, unknown>;
  argConstraints?: Record<string, Record<string, unknown>>;
  columns: Col[];
  /** The initial phases; an empty list means the scan returns no rows. */
  plan: (scan: ScanContext<A>) => Phase[];
  /** Child-phase builders for fan-out phases, by name. */
  children?: Record<string, (parent: Row, scan: ScanContext<A>) => Phase | null>;
  /** Turn one API row into several output rows (flattening nested lists). */
  explode?: (row: Row) => Row[];
  /** Drop rows (e.g. edges reported by both ends). */
  keep?: (row: Row) => boolean;
  /** Enrich a page of rows with extra calls (e.g. incident timelines). */
  expand?: (cfg: PdConfig, rows: Row[], scan: ScanContext<A>) => Promise<Row[]>;
  /**
   * An add-on API (Analytics, Audit, Status Pages): a 402/403 becomes a clear "not
   * available to this account" error pointing at `account_features`.
   */
  feature?: string;
  /** Compute every row in one go instead of paging phases (feature probes). */
  custom?: (cfg: PdConfig, scan: ScanContext<A>) => Promise<Row[]>;
  /** Primary-key column(s) of a catalog table (attested NOT NULL + PRIMARY KEY). */
  pk?: string | string[];
  /** Foreign keys of a catalog table: column → referenced table's single-column primary key. */
  fks?: { column: string; table: string; references: string }[];
  /** Text appended to doc_md describing what pushes down. */
  pushdown?: string;
  examples: FnExample[];
}

interface ScanState {
  started: boolean;
  /** JSON of the pending phases; the head is being read. */
  queue: string;
  anchorMs: number;
}

/** The Arrow output schema of a spec (column comments ride along as field metadata). */
function outputSchemaOf<A>(spec: PdTableSpec<A>) {
  return schema(
    spec.columns.map((c) =>
      c.sqlType === "JSON"
        ? field(c.name, c.type, c.nullable ?? true, new Map([["comment", c.doc], JSON_META]))
        : column(c.name, c.type, c.doc, c.nullable ?? true),
    ),
  );
}

/** Discovery tags shared by a spec's function or table. */
function tagsOf<A>(spec: PdTableSpec<A>): Record<string, string> {
  return {
    domain: "incident-management",
    ...objectTags({
      title: spec.title,
      category: spec.category,
      keywords: spec.keywords,
      docLlm: spec.docLlm,
      docMd: spec.docMd + (spec.pushdown ? `\n\n**Pushdown:** ${spec.pushdown}` : ""),
    }),
    "vgi.result_columns_schema": JSON.stringify(
      spec.columns.map((c) => ({ name: c.name, type: c.sqlType, description: c.doc })),
    ),
  };
}

/**
 * A spec with no arguments exposed as a catalog table (`SELECT … FROM pagerduty.main.services`).
 * The scan function keeps every capability (pushdown, paging, secrets) and is table-only.
 */
export function definePdCatalogTable(spec: PdTableSpec<Record<string, never>>): TableDescriptor {
  const fn = definePdTable({ ...spec, name: `${spec.name}_scan` });
  const pk = spec.pk === undefined ? [] : Array.isArray(spec.pk) ? spec.pk : [spec.pk];
  const schemaPath = ["main"];
  const base = { schemaPath, table: spec.name, enforcement: "ENFORCED", validation: "ALL_ROWS" } as const;
  const constraints = [
    ...pk.map((c) => constraintRecord({ ...base, id: `${c}_not_null`, kind: "NOT_NULL", columns: [[c]] })),
    ...(pk.length ? [constraintRecord({ ...base, id: "pk", kind: "PRIMARY_KEY", columns: pk.map((c) => [c]) })] : []),
    // PagerDuty only returns references to objects that exist in the same account.
    ...(spec.fks ?? []).map((fk) =>
      constraintRecord({
        ...base,
        id: `${fk.column}_fk`,
        kind: "FOREIGN_KEY",
        columns: [[fk.column]],
        references: { schemaPath, table: fk.table, columns: [[fk.references]], constraintId: "pk" },
      }),
    ),
  ];
  return {
    name: spec.name,
    comment: spec.description,
    columns: outputSchemaOf(spec),
    function: fn,
    constraints,
    tags: { ...tagsOf(spec), "vgi.example_queries": JSON.stringify(spec.examples) },
  };
}

export function definePdTable<A extends Record<string, unknown>>(spec: PdTableSpec<A>): VgiFunction {
  const outSchema = outputSchemaOf(spec);

  return defineTableFunction<A, ScanState>({
    name: spec.name,
    description: spec.description,
    args: spec.args ?? {},
    argDocs: spec.argDocs,
    argDefaults: spec.argDefaults,
    argConstraints: spec.argConstraints as never,
    filterPushdown: true,
    // DuckDB drops pushed predicates from its plan; the SDK re-applies them exactly,
    // so translating a predicate only ever narrows what PagerDuty returns.
    autoApplyFilters: true,
    onBind: (params) =>
      params.resolvedSecretsProvided
        ? { outputSchema: outSchema }
        : { outputSchema: outSchema, lookupSecretTypes: [SECRET_TYPE] },
    initialState: () => ({ started: false, queue: "[]", anchorMs: Date.now() }),
    process: async (params, state, out) => {
      const cfg = configFromSecrets(params.secrets);
      const scan: ScanContext<A> = {
        args: params.args,
        constraints: constraintsOf(params.pushdownFilters),
        anchorMs: state.anchorMs,
      };
      if (spec.custom) {
        if (!state.started) {
          state.started = true;
          const rows = await spec.custom(cfg, scan);
          if (rows.length) {
            const cols: Record<string, unknown[]> = {};
            for (const f of params.outputSchema.fields) {
              const c = spec.columns.find((x) => x.name === f.name);
              cols[f.name] = c ? rows.map((r) => c.get(r)) : rows.map(() => null);
            }
            out.emit(batchFromColumns(cols, params.outputSchema));
            return;
          }
        }
        out.finish();
        return;
      }
      if (!state.started) {
        log.debug(`${spec.name}: pushed filters ${formatPushedFilters(params.pushdownFilters)}`);
        state.queue = JSON.stringify(spec.plan(scan));
        state.started = true;
      }
      const queue = JSON.parse(state.queue) as Phase[];
      const rows = await nextRows(spec, cfg, scan, queue);
      state.queue = JSON.stringify(queue);
      if (rows === null) {
        out.finish();
        return;
      }
      const cols: Record<string, unknown[]> = {};
      for (const f of params.outputSchema.fields) {
        const c = spec.columns.find((x) => x.name === f.name);
        cols[f.name] = c ? rows.map((r) => c.get(r)) : rows.map(() => null);
      }
      out.emit(batchFromColumns(cols, params.outputSchema));
    },
    tags: tagsOf(spec),
    examples: spec.examples,
  });
}

/**
 * Read pages from the head of `queue` (mutating it: offsets advance, finished phases
 * drop off, split windows and fan-out children are inserted) until one yields output
 * rows. Returns those rows, or null once the queue is empty — so no empty batches go out.
 */
export async function nextRows<A>(
  spec: PdTableSpec<A>,
  cfg: PdConfig,
  scan: ScanContext<A>,
  queue: Phase[],
): Promise<Row[] | null> {
  for (;;) {
    const p = queue[0];
    if (!p) return null;
    const req = pageRequest(p);
    let body: unknown;
    try {
      body = await pdRequest(cfg, req.path, { method: req.method, query: req.query, body: req.body });
    } catch (err) {
      if (p.notFoundEmpty && err instanceof PdError && err.status === 404) {
        queue.shift();
        continue;
      }
      if (spec.feature && err instanceof PdError && (err.status === 402 || err.status === 403)) {
        throw new PdError(
          `${spec.feature} is not available to this PagerDuty account or API key (${err.message}); ` +
            "SELECT * FROM pagerduty.main.account_features shows which add-on APIs are enabled",
          err.status,
          err.code,
        );
      }
      throw err;
    }
    // A window holding more rows than offset paging reaches: halve it and retry.
    if (p.window && p.paging === "offset" && !p.offset) {
      const total = (body as { total?: unknown } | null)?.total;
      if (typeof total === "number" && total > OFFSET_CAP) {
        const halves = splitWindow(p.window);
        if (!halves) {
          throw new PdError(`${p.path}: ${total} rows in under a minute; PagerDuty cannot page past ${OFFSET_CAP}`);
        }
        log.debug(`${spec.name}: ${total} rows in ${iso(p.window.lo)}..${iso(p.window.hi)}; splitting`);
        queue.splice(0, 1, ...halves.map((w) => ({ ...p, window: w })));
        continue;
      }
    }
    const page = rowsAt(body, p.key);
    const base = p.offset ?? 0;
    if (advance(p, body, page)) queue.shift();
    if (p.child) {
      const build = spec.children?.[p.child];
      const kids = build ? page.map((r) => build(r, scan)).filter((x): x is Phase => x !== null) : [];
      // Children are read before the parent listing continues, so the queue stays one page deep.
      queue.splice(0, 0, ...kids);
      continue;
    }
    let rows: Row[] = page.map((r, i) => ({ ...r, __ctx: p.ctx, __pos: base + i }));
    if (spec.explode) rows = rows.flatMap((r) => spec.explode?.(r) ?? [r]);
    if (spec.keep) rows = rows.filter(spec.keep);
    if (spec.expand && rows.length) rows = await spec.expand(cfg, rows, scan);
    if (rows.length) return rows;
  }
}

/** Read every page of an offset-paged sub-resource (used by expansions). */
export async function getAll(cfg: PdConfig, path: string, key: string, q: Query = []): Promise<Row[]> {
  const rows: Row[] = [];
  const p: Phase = { path, q, key, paging: "offset", limit: 100 };
  for (let guard = 0; guard < 200; guard++) {
    const req = pageRequest(p);
    const body = await pdRequest(cfg, req.path, { query: req.query });
    const page = rowsAt(body, key);
    rows.push(...page);
    if (advance(p, body, page)) break;
  }
  return rows;
}
