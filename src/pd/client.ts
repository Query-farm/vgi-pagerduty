/**
 * PagerDuty REST API v2 client: authentication, regions, rate limits and errors.
 *
 * - **Auth.** A `pagerduty` DuckDB secret carries either a REST API key
 *   (`API_TOKEN`, sent as `Authorization: Token token=<key>`) or an OAuth 2.0 access
 *   token (`OAUTH_TOKEN`, sent as `Authorization: Bearer <token>`). The secret is read
 *   on every call and never enters table-function state.
 * - **Region.** `REGION 'us'` (default, https://api.pagerduty.com) or `REGION 'eu'`
 *   (https://api.eu.pagerduty.com); `BASE_URL` overrides both (https only, except a
 *   loopback fixture server).
 * - **Rate limits.** PagerDuty limits each key per minute and answers 429 with
 *   `ratelimit-reset` (seconds until the bucket refills) and/or `Retry-After`. A 429
 *   waits that long (bounded) and retries; 5xx and network errors retry with
 *   exponential backoff.
 * - **Errors.** PagerDuty error bodies are `{"error": {"message", "code", "errors"}}`;
 *   `PdError` keeps the HTTP status and code so callers can feature-detect (402/403/404
 *   on an add-on API means "not on this account").
 */

import { secretOfType } from "@query-farm/vgi";
import { log } from "../log.ts";
import { VERSION } from "../meta.ts";

export const SECRET_TYPE = "pagerduty";
export const TIMEOUT_MS = 30_000;
export const MAX_ATTEMPTS = 6;
/** Longest single wait for a rate-limit reset; PagerDuty buckets are per minute. */
export const MAX_RATE_WAIT_MS = 61_000;
export const REGIONS: Record<string, string> = {
  us: "https://api.pagerduty.com",
  eu: "https://api.eu.pagerduty.com",
};
const USER_AGENT = `vgi-pagerduty-worker/${VERSION}`;
const ACCEPT = "application/vnd.pagerduty+json;version=2";

export class PdError extends Error {
  constructor(
    message: string,
    readonly status = 0,
    readonly code?: number,
  ) {
    super(message);
  }
}

export interface PdConfig {
  baseUrl: string;
  authorization: string;
}

const LOOPBACK = new Set(["127.0.0.1", "localhost", "[::1]", "::1"]);

/** Normalize and check a base URL: https, or http only for a loopback fixture server. */
export function normalizeBaseUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new PdError(`pagerduty secret: base_url '${raw}' is not a URL (expected e.g. https://api.pagerduty.com)`);
  }
  if (url.protocol !== "https:" && !(url.protocol === "http:" && LOOPBACK.has(url.hostname))) {
    throw new PdError(
      "pagerduty secret: base_url must use https:// (http:// is accepted only for 127.0.0.1/localhost)",
    );
  }
  return url.origin + url.pathname.replace(/\/+$/, "");
}

function str(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : undefined;
}

/** Build the client config from the `pagerduty` secret DuckDB resolved for this call. */
export function configFromSecrets(secrets: Record<string, Record<string, unknown>>): PdConfig {
  const s = secretOfType(secrets as never, SECRET_TYPE) as Record<string, unknown> | undefined;
  if (!s) {
    throw new PdError(
      "no pagerduty secret: run CREATE SECRET (TYPE pagerduty, API_TOKEN '<REST API key>') " +
        "or CREATE SECRET (TYPE pagerduty, OAUTH_TOKEN '<access token>'); add REGION 'eu' for EU accounts",
    );
  }
  const apiToken = str(s.api_token);
  const oauthToken = str(s.oauth_token);
  if (!apiToken && !oauthToken) {
    throw new PdError("pagerduty secret: set API_TOKEN (a REST API key) or OAUTH_TOKEN (an OAuth access token)");
  }
  const region = (str(s.region) ?? "us").toLowerCase();
  const base = str(s.base_url) ?? REGIONS[region];
  if (!base) throw new PdError(`pagerduty secret: region must be 'us' or 'eu' (got '${region}')`);
  return {
    baseUrl: normalizeBaseUrl(base),
    authorization: apiToken ? `Token token=${apiToken}` : `Bearer ${oauthToken}`,
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** How long to wait before retrying a 429: `ratelimit-reset` / `Retry-After` seconds. */
export function rateLimitDelayMs(headers: Headers, attempt: number): number {
  for (const h of ["ratelimit-reset", "retry-after"]) {
    const v = headers.get(h);
    if (v && /^\d+(\.\d+)?$/.test(v)) return Math.min(Number(v) * 1000 + 100, MAX_RATE_WAIT_MS);
  }
  return Math.min(1000 * attempt, MAX_RATE_WAIT_MS);
}

/** Query parameters as ordered pairs, so array parameters (`statuses[]`) can repeat. */
export type Query = [string, string][];

export interface PdRequest {
  method?: "GET" | "POST";
  query?: Query;
  /** JSON body (POST only: the Analytics API takes its filters as a body). */
  body?: unknown;
}

/** Build the request URL for a path under the configured base. */
export function requestUrl(cfg: PdConfig, path: string, query?: Query): string {
  const qs = (query ?? []).map(
    ([k, v]) => `${encodeURIComponent(k).replace(/%5B%5D$/, "[]")}=${encodeURIComponent(v)}`,
  );
  return `${cfg.baseUrl}${path}${qs.length ? `?${qs.join("&")}` : ""}`;
}

/** Call one PagerDuty endpoint and return its parsed JSON body. */
export async function pdRequest(cfg: PdConfig, path: string, req: PdRequest = {}): Promise<unknown> {
  const url = requestUrl(cfg, path, req.query);
  const method = req.method ?? "GET";
  const headers: Record<string, string> = {
    Accept: ACCEPT,
    Authorization: cfg.authorization,
    "User-Agent": USER_AGENT,
  };
  if (req.body !== undefined) headers["Content-Type"] = "application/json";
  if (path.startsWith("/analytics/")) headers["X-EARLY-ACCESS"] = "analytics-v2";

  for (let attempt = 1; ; attempt++) {
    let res: Response;
    try {
      res = await fetch(url, {
        method,
        headers,
        body: req.body === undefined ? undefined : JSON.stringify(req.body),
        signal: AbortSignal.timeout(TIMEOUT_MS),
      });
    } catch (err) {
      if (attempt >= MAX_ATTEMPTS) throw new PdError(`${method} ${shortUrl(url)} failed: ${(err as Error).message}`);
      await sleep(250 * 2 ** attempt);
      continue;
    }
    if (res.ok) {
      const text = await res.text();
      return text ? JSON.parse(text) : null;
    }
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt >= MAX_ATTEMPTS) {
      const { summary, code } = await errorSummary(res);
      throw new PdError(`${method} ${shortUrl(url)} failed with HTTP ${res.status}: ${summary}`, res.status, code);
    }
    await res.body?.cancel();
    const delay = res.status === 429 ? rateLimitDelayMs(res.headers, attempt) : 250 * 2 ** attempt;
    log.debug(`pagerduty: HTTP ${res.status} for ${shortUrl(url)}; retry ${attempt} in ${delay} ms`);
    await sleep(delay);
  }
}

/** PagerDuty errors are `{error: {message, code, errors: [...]}}`; keep the message short. */
async function errorSummary(res: Response): Promise<{ summary: string; code?: number }> {
  const text = await res.text();
  try {
    const j = JSON.parse(text) as { error?: { message?: string; code?: number; errors?: string[] } | string };
    if (typeof j.error === "string") return { summary: j.error };
    const e = j.error ?? {};
    const parts = [e.code !== undefined ? `code ${e.code}` : "", e.message ?? "", ...(e.errors ?? [])].filter(Boolean);
    return { summary: parts.join(" - ") || text.replace(/\s+/g, " ").trim().slice(0, 200), code: e.code };
  } catch {
    return { summary: text.replace(/\s+/g, " ").trim().slice(0, 200) };
  }
}

/** Show the path + query of a URL in errors (credentials never appear in a URL). */
function shortUrl(url: string): string {
  const u = new URL(url);
  return decodeURIComponent(`${u.pathname}${u.search}`);
}
