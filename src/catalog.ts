/**
 * Assemble the `pagerduty` catalog: the table functions, the parameterless collections
 * as tables, and the `pagerduty` secret type.
 */

import { type CatalogDescriptor, field, schema, utf8, type VgiFunction } from "@query-farm/vgi";
import { CATALOG_COMMENT, CATALOG_NAME, CATALOG_TAGS, SCHEMA_DOC_MD, SCHEMA_TAGS, SOURCE_URL } from "./meta.ts";
import { SECRET_TYPE } from "./pd/client.ts";
import { FUNCTIONS, TABLES } from "./pd/resources.ts";

export { FUNCTIONS };

const doc = (text: string) => new Map([["vgi_doc", text]]);
const redacted = (text: string) =>
  new Map([
    ["redact", "true"],
    ["vgi_doc", text],
  ]);

/** CREATE SECRET (TYPE pagerduty, API_TOKEN '…') — or OAUTH_TOKEN; REGION 'eu' / BASE_URL. */
export const PAGERDUTY_SECRET_TYPE = {
  name: SECRET_TYPE,
  description: "A PagerDuty REST API key or OAuth access token, plus the account's region",
  schema: schema(
    [
      field(
        "api_token",
        utf8(),
        true,
        redacted(
          "REST API key (Integrations > API Access Keys), sent as 'Token token=<key>'. Use this or OAUTH_TOKEN.",
        ),
      ),
      field(
        "oauth_token",
        utf8(),
        true,
        redacted("OAuth 2.0 access token (sent as 'Bearer <token>') instead of a key."),
      ),
      field("region", utf8(), true, doc("'us' (default, api.pagerduty.com) or 'eu' (api.eu.pagerduty.com).")),
      field(
        "base_url",
        utf8(),
        true,
        doc("Overrides the region's API URL (https only; http:// is accepted for 127.0.0.1/localhost)."),
      ),
    ],
    new Map([["vgi_doc_url", "https://developer.pagerduty.com/docs/authentication"]]),
  ),
};

interface Example {
  description: string;
  sql: string;
}

/** Publish each function's examples as `vgi.example_queries`; return them all for the schema. */
function stampExampleQueries(functions: VgiFunction[]): Example[] {
  const all: Example[] = [];
  for (const fn of functions) {
    const ex = (fn.meta.examples ?? []).map((e) => ({ description: e.description, sql: e.sql }));
    fn.meta.tags = { ...fn.meta.tags, "vgi.example_queries": JSON.stringify(ex) };
    all.push(...ex);
  }
  return all;
}

const ALL_EXAMPLES = [
  ...stampExampleQueries(FUNCTIONS),
  ...TABLES.flatMap((t) => JSON.parse(t.tags?.["vgi.example_queries"] ?? "[]") as Example[]),
];

export const CATALOG: CatalogDescriptor = {
  name: CATALOG_NAME,
  comment: CATALOG_COMMENT,
  sourceUrl: SOURCE_URL,
  tags: CATALOG_TAGS,
  secretTypes: [PAGERDUTY_SECRET_TYPE],
  schemas: [
    {
      path: ["main"],
      comment: SCHEMA_DOC_MD,
      functions: FUNCTIONS,
      tables: TABLES,
      tags: { ...SCHEMA_TAGS, "vgi.example_queries": JSON.stringify(ALL_EXAMPLES.slice(0, 12)) },
    },
  ],
};
