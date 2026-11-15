/**
 * The worker entry point.
 *
 *   bin/vgi-pagerduty-worker                      # stdio: what DuckDB launches by default
 *   bin/vgi-pagerduty-worker --http --port 8000   # HTTP: for Docker / hosted deploys
 *   bin/vgi-pagerduty-worker --help
 *
 * Attach it from DuckDB:
 *
 *   ATTACH 'pagerduty' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-pagerduty-worker');
 */

import { CATALOG } from "./catalog.ts";
import { serve } from "./cli.ts";
import { CATALOG_DOC_LLM, SOURCE_URL, VERSION } from "./meta.ts";

if (import.meta.main) {
  serve(CATALOG, { version: VERSION, doc: CATALOG_DOC_LLM, repositoryUrl: SOURCE_URL });
}
