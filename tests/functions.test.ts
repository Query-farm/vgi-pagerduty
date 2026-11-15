// Unit tests. The SQL-level tests live in test/sql and run through DuckDB against the
// strict replay server in test/mock_api (every transport).

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { Arguments, VgiClient } from "@query-farm/vgi";
import { subprocessConnect } from "@query-farm/vgi-rpc";
import {
  configFromSecrets,
  normalizeBaseUrl,
  type PdConfig,
  pdRequest,
  rateLimitDelayMs,
  requestUrl,
} from "../src/pd/client.ts";
import { type Constraint, toEpochMs } from "../src/pd/pushdown.ts";
import { DEFAULT_LOOKBACK_MS, timelineOf, windowOf } from "../src/pd/resources.ts";
import {
  advance,
  asTime,
  C,
  chunkWindow,
  nextRows,
  OFFSET_CAP,
  type PdTableSpec,
  type Phase,
  pageRequest,
  splitWindow,
} from "../src/pd/table.ts";

const ROOT = join(import.meta.dir, "..");
const DAY = 86_400_000;
const secret = (fields: Record<string, string>) => ({ s: { type: "pagerduty", ...fields } });

test("configFromSecrets: REST API key or OAuth token; US default, EU region, base_url override", () => {
  expect(() => configFromSecrets({})).toThrow(/no pagerduty secret/);
  expect(() => configFromSecrets(secret({ region: "eu" }))).toThrow(/API_TOKEN.*OAUTH_TOKEN/);
  expect(configFromSecrets(secret({ api_token: "k" }))).toEqual({
    baseUrl: "https://api.pagerduty.com",
    authorization: "Token token=k",
  });
  expect(configFromSecrets(secret({ oauth_token: "o", region: "EU" }))).toEqual({
    baseUrl: "https://api.eu.pagerduty.com",
    authorization: "Bearer o",
  });
  expect(configFromSecrets(secret({ api_token: "k", base_url: "http://127.0.0.1:9/" })).baseUrl).toBe(
    "http://127.0.0.1:9",
  );
  expect(() => configFromSecrets(secret({ api_token: "k", region: "apac" }))).toThrow(/region/);
});

test("normalizeBaseUrl: https only, except loopback fixtures", () => {
  expect(normalizeBaseUrl("https://api.pagerduty.com/")).toBe("https://api.pagerduty.com");
  expect(() => normalizeBaseUrl("http://api.pagerduty.com")).toThrow(/https/);
  expect(() => normalizeBaseUrl("nope")).toThrow(/not a URL/);
});

test("requestUrl: array parameters keep their brackets and repeat", () => {
  const cfg: PdConfig = { baseUrl: "https://api.pagerduty.com", authorization: "" };
  expect(
    requestUrl(cfg, "/incidents", [
      ["statuses[]", "triggered"],
      ["statuses[]", "acknowledged"],
      ["since", "2026-01-01T00:00:00.000Z"],
    ]),
  ).toBe(
    "https://api.pagerduty.com/incidents?statuses[]=triggered&statuses[]=acknowledged&since=2026-01-01T00%3A00%3A00.000Z",
  );
});

test("rateLimitDelayMs: ratelimit-reset, then Retry-After, then linear backoff (capped)", () => {
  expect(rateLimitDelayMs(new Headers({ "ratelimit-reset": "2" }), 1)).toBe(2100);
  expect(rateLimitDelayMs(new Headers({ "Retry-After": "3" }), 1)).toBe(3100);
  expect(rateLimitDelayMs(new Headers(), 2)).toBe(2000);
  expect(rateLimitDelayMs(new Headers({ "ratelimit-reset": "600" }), 1)).toBe(61_000);
});

test("toEpochMs: µs bigints and µs numbers (TIMESTAMPTZ arguments) and ms numbers", () => {
  const ms = Date.parse("2026-11-01T00:00:00Z");
  expect(toEpochMs(BigInt(ms) * 1000n)).toBe(ms);
  expect(toEpochMs(ms * 1000)).toBe(ms);
  expect(toEpochMs(ms)).toBe(ms);
  expect(toEpochMs("2026-11-01T00:00:00Z")).toBe(ms);
});

test("windowOf: arguments and pushed bounds intersect; defaults are the last 30 days", () => {
  const anchor = Date.parse("2026-11-15T00:00:00Z");
  const scan = (args: Record<string, unknown>, constraints: Constraint[] = []) => ({
    args,
    constraints,
    anchorMs: anchor,
  });
  expect(windowOf(scan({}), "created_at")).toEqual({ lo: anchor - DEFAULT_LOOKBACK_MS, hi: anchor });
  const lo = Date.parse("2024-01-01T00:00:00Z");
  const cs: Constraint[] = [{ kind: "cmp", column: "created_at", op: "ge", value: BigInt(lo) * 1000n }];
  expect(windowOf(scan({}, cs), "created_at")).toEqual({ lo, hi: anchor });
  // An inclusive upper bound widens by a millisecond; since := narrows a pushed bound.
  const le: Constraint[] = [
    ...cs,
    { kind: "cmp", column: "created_at", op: "le", value: BigInt(anchor - DAY) * 1000n },
  ];
  expect(windowOf(scan({ since: BigInt(lo + DAY) * 1000n }, le), "created_at")).toEqual({
    lo: lo + DAY,
    hi: anchor - DAY + 1,
  });
  expect(windowOf(scan({ since: BigInt(anchor) * 1000n, until: BigInt(anchor - DAY) * 1000n }), "created_at")).toBe(
    undefined,
  );
});

test("chunkWindow / splitWindow: PagerDuty's range limits and the offset cap", () => {
  expect(chunkWindow(0, 400, 180)).toEqual([
    [0, 180],
    [180, 360],
    [360, 400],
  ]);
  expect(chunkWindow(0, 400, 180, true)[0]).toEqual([360, 400]);
  expect(splitWindow({ lo: 0, hi: 10 * DAY, style: "query" })).toEqual([
    { lo: 0, hi: 5 * DAY, style: "query" },
    { lo: 5 * DAY, hi: 10 * DAY, style: "query" },
  ]);
  expect(splitWindow({ lo: 0, hi: 1000, style: "query" })).toBeUndefined();
});

test("pageRequest / advance: offset paging asks for total on a window's first page and stops at the cap", () => {
  const p: Phase = {
    path: "/incidents",
    key: "incidents",
    paging: "offset",
    limit: 100,
    window: { lo: 0, hi: DAY, style: "query" },
  };
  expect(pageRequest(p).query).toEqual([
    ["since", "1970-01-01T00:00:00.000Z"],
    ["until", "1970-01-02T00:00:00.000Z"],
    ["limit", "100"],
    ["offset", "0"],
    ["total", "true"],
  ]);
  const rows = Array.from({ length: 100 }, (_, i) => ({ id: String(i) }));
  expect(advance(p, { more: true }, rows)).toBe(false);
  expect(p.offset).toBe(100);
  expect(pageRequest(p).query.some(([k]) => k === "total")).toBe(false);
  expect(advance(p, { more: false }, rows)).toBe(true);
  const capped: Phase = { path: "/x", key: "x", paging: "offset", limit: 100, offset: OFFSET_CAP - 100 };
  expect(() => advance(capped, { more: true }, rows)).toThrow(/10000 rows/);
});

test("pageRequest: analytics windows travel in the body; cursors become starting_after", () => {
  const p: Phase = {
    path: "/analytics/raw/incidents",
    method: "POST",
    body: { filters: { urgency: "high" } },
    key: "data",
    paging: "analytics",
    limit: 1000,
    window: { lo: 0, hi: DAY, style: "analytics" },
    cursor: "Q1",
  };
  expect(pageRequest(p).body).toEqual({
    filters: {
      urgency: "high",
      created_at_start: "1970-01-01T00:00:00.000Z",
      created_at_end: "1970-01-02T00:00:00.000Z",
    },
    limit: 1000,
    starting_after: "Q1",
  });
  expect(advance(p, { more: true, last: "Q9" }, [{}])).toBe(false);
  expect(p.cursor).toBe("Q9");
  expect(advance(p, { more: false, last: "Q10" }, [{}])).toBe(true);
});

test("timelineOf: first acknowledgement, last resolve, escalation count", () => {
  const t = timelineOf([
    { type: "resolve_log_entry", created_at: "2026-01-01T01:00:00Z", agent: { summary: "B" } },
    { type: "trigger_log_entry", created_at: "2026-01-01T00:00:00Z" },
    { type: "acknowledge_log_entry", created_at: "2026-01-01T00:05:00Z", agent: { summary: "A" } },
    { type: "escalate_log_entry", created_at: "2026-01-01T00:03:00Z" },
    { type: "acknowledge_log_entry", created_at: "2026-01-01T00:20:00Z", agent: { summary: "C" } },
  ]);
  expect(t).toEqual({
    ack: "2026-01-01T00:05:00Z",
    ackBy: "A",
    resolve: "2026-01-01T01:00:00Z",
    resolveBy: "B",
    escalations: 1,
  });
});

test("asTime: ISO timestamps to TIMESTAMPTZ microseconds", () => {
  expect(asTime("2026-01-01T00:00:00Z")).toBe(1_767_225_600_000_000n);
  expect(asTime(null)).toBeNull();
  expect(asTime("garbage")).toBeNull();
});

describe("against a local server", () => {
  let server: ReturnType<typeof Bun.serve>;
  let hits: { url: URL; auth: string | null }[] = [];
  let mode = "ok";

  beforeAll(() => {
    server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req) {
        const url = new URL(req.url);
        hits.push({ url, auth: req.headers.get("Authorization") });
        if (mode === "429-once" && hits.length === 1) {
          return Response.json(
            { error: { message: "Rate Limit Exceeded", code: 2020 } },
            { status: 429, headers: { "ratelimit-reset": "0" } },
          );
        }
        if (mode === "404") return Response.json({ error: { message: "Not Found", code: 2100 } }, { status: 404 });
        if (mode === "dense") {
          // 200 incidents per day: a window longer than 50 days is past the 10,000 cap.
          const since = Date.parse(url.searchParams.get("since") ?? "");
          const until = Date.parse(url.searchParams.get("until") ?? "");
          const total = Math.round(((until - since) / DAY) * 200);
          return Response.json({
            incidents: [{ id: `${since}` }],
            more: false,
            total: url.searchParams.get("total") === "true" ? total : null,
          });
        }
        return Response.json({ things: [{ id: "1" }], more: false });
      },
    });
  });
  afterAll(() => server.stop(true));

  const cfg = (): PdConfig => ({ baseUrl: `http://127.0.0.1:${server.port}`, authorization: "Token token=tok" });

  test("auth and Accept headers; 429 waits for the reset, then succeeds", async () => {
    hits = [];
    mode = "429-once";
    expect(await pdRequest(cfg(), "/things")).toEqual({ things: [{ id: "1" }], more: false });
    expect(hits.length).toBe(2);
    expect(hits[1]?.auth).toBe("Token token=tok");
  });

  test("4xx is not retried and carries PagerDuty's error message and code", async () => {
    hits = [];
    mode = "404";
    await expect(pdRequest(cfg(), "/incidents/X")).rejects.toThrow(/HTTP 404: code 2100 - Not Found/);
    expect(hits.length).toBe(1);
  });

  test("a window past the offset cap is halved until every piece fits", async () => {
    hits = [];
    mode = "dense";
    const spec = { name: "t", columns: [C.str("id", "id", "id")] } as unknown as PdTableSpec<Record<string, unknown>>;
    const queue: Phase[] = [
      {
        path: "/incidents",
        key: "incidents",
        paging: "offset",
        limit: 100,
        window: { lo: 0, hi: 180 * DAY, style: "query" },
      },
    ];
    const scan = { args: {}, constraints: [], anchorMs: 0 };
    const pieces: string[] = [];
    for (;;) {
      const rows = await nextRows(spec, cfg(), scan, queue);
      if (rows === null) break;
      pieces.push(...rows.map((r) => String(r.id)));
    }
    // 180 days → 90 → 45: four 45-day windows (9,000 each), in time order.
    expect(pieces).toEqual(["0", String(45 * DAY), String(90 * DAY), String(135 * DAY)]);
    expect(hits.length).toBe(1 + 2 + 4);
  });
});

describe("the worker subprocess", () => {
  let client: VgiClient;

  beforeAll(() => {
    client = new VgiClient(subprocessConnect([process.execPath, "run", join(ROOT, "src", "worker.ts")]));
  });
  afterAll(() => client?.close());

  // Fixture-backed results are covered end to end through DuckDB in test/sql (all
  // transports); here the worker must fail clearly when no pagerduty secret is attached.
  test("a scan without a pagerduty secret explains how to create one", async () => {
    const run = async () => {
      for await (const _ of client.tableFunctionRows({
        functionName: "oncalls",
        arguments: new Arguments([], new Map()),
        int64: "bigint",
      })) {
        // no rows expected
      }
    };
    await expect(run()).rejects.toThrow(/CREATE SECRET \(TYPE pagerduty/);
  });
});
