"""Self-contained GitHub Action entry point for the ARSO forecast GitHub
archive buffer (see AGENTS.md, "ARSO forecast GitHub archive buffer").

**Deliberately does NOT import anything from `weather_reliability`** --
same reasoning as `scripts/geosphere_inca_fetch_standalone.py`: this file
is deployed BY ITSELF (plus `.github/workflows/arso-forecast-buffer.yml`)
to the separate, lightweight public `pixpocker/Zevs` repository, which
does not hold this project's source tree. Everything this script needs is
duplicated here, deliberately (see that script's own module docstring for
the precedent and rationale).

**Scope is intentionally narrow**: fetch each configured location's ARSO
forecast, compute a content hash, and preserve the raw ARSO payload
verbatim. All canonical ARSO forecast parsing
(`weather_reliability.cli.parse_arso_forecast`) stays exclusively in the
local ZEVS project (`weather_reliability/arso_forecast_buffer.py`'s
`import_artifact`, which feeds each location's raw `payload` here straight
into that function, unchanged by this file).

Unlike GeoSphere INCA, ARSO's own forecast JSON does not expose a single
top-level run/reference timestamp to group by -- so this script produces
exactly ONE artifact per invocation, covering every configured location,
keyed by this script's own `fetched_at` wall-clock time. This is the exact
same "issued_at = fetch time" convention the direct `zevs collect` ARSO
forecast fetch already uses (see `cli.collect`) -- no new semantic is
introduced by archiving it this way.

Writes:
  - one artifact JSON file under `--out-dir` (default: artifact/), named
    `<artifact_name>.json`;
  - `artifacts` to `$GITHUB_OUTPUT` as a JSON array of
    `{"name": ..., "path": ...}` (a single entry) -- same output contract
    shape as the INCA fetch script, so the workflow can reuse an identical
    upload-matrix pattern even though there is only ever one entry here.

Exit code is always 0 (even when every location failed to fetch) -- same
reasoning as the INCA fetch script: a partially/fully failed run still
produces a valid, clearly-marked artifact rather than silently skipping
the upload.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# --- Duplicated constants -- keep in sync with weather_reliability/cli.py
# and weather_reliability/arso_forecast_buffer.py. See module docstring for
# why this duplication is deliberate. --------------------------------------

ARSO_LOCATION_URL = "https://vreme.arso.gov.si/api/1.0/location/"
ARSO_ATTRIBUTION = "Data source: ARSO (Slovenian Environment Agency) -- vreme.arso.gov.si"
ARTIFACT_SCHEMA_VERSION = 1

# Same 10 canonical Zevs locations as config.yml's `locations:` list --
# only `name`/`arso_location` matter here (no lat/lon needed for ARSO's
# location-name-keyed endpoint). Small and rarely changing -- kept as a
# plain hardcoded list rather than parsing config.yml (which does not
# exist in the lightweight publish repo this script is deployed to).
# Update BOTH places if a location is ever added/renamed/moved.
LOCATIONS: list[dict[str, str]] = [
    {"name": "Ljubljana", "arso_location": "Ljubljana"},
    {"name": "Maribor", "arso_location": "Maribor"},
    {"name": "Celje", "arso_location": "Celje"},
    {"name": "Kranj", "arso_location": "Kranj"},
    {"name": "Novo mesto", "arso_location": "Novo mesto"},
    {"name": "Koper", "arso_location": "Koper"},
    {"name": "Vrhnika", "arso_location": "Vrhnika"},
    {"name": "Postojna", "arso_location": "Postojna"},
    {"name": "Murska Sobota", "arso_location": "Murska Sobota"},
    {"name": "Kočevje", "arso_location": "Kočevje"},
]

DEFAULT_REQUEST_DELAY_SECONDS = 0.3


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_hex(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fetch_arso_forecast(session: requests.Session, arso_location: str, *, timeout: int) -> dict[str, Any]:
    response = session.get(ARSO_LOCATION_URL, params={"location": arso_location}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _light_validity_check(payload: dict[str, Any]) -> str | None:
    """Deliberately shallow -- NOT the real `cli.parse_arso_forecast` (that
    stays local-only). Just enough to confirm at least one forecast section
    with usable features is present."""
    if not isinstance(payload, dict):
        return "response is not a JSON object"
    for section in ("forecast1h", "forecast3h", "forecast6h", "forecast24h"):
        features = (payload.get(section) or {}).get("features")
        if isinstance(features, list) and features:
            return None
    return "no usable forecast section in response (missing/empty features)"


def build_artifact(
    session: requests.Session,
    locations: list[dict[str, str]],
    fetched_at: str,
    *,
    timeout: int = 30,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
) -> dict[str, Any]:
    import time

    entries: list[dict[str, Any]] = []
    for i, loc in enumerate(locations):
        if i > 0 and request_delay_seconds > 0:
            time.sleep(request_delay_seconds)
        entry: dict[str, Any] = {"name": loc["name"], "arso_location": loc["arso_location"]}
        try:
            payload = fetch_arso_forecast(session, loc["arso_location"], timeout=timeout)
        except requests.RequestException as exc:
            entry["status"] = "error"
            entry["error"] = f"fetch failed: {exc}"
            entries.append(entry)
            continue
        validity_error = _light_validity_check(payload)
        if validity_error is not None:
            entry["status"] = "error"
            entry["error"] = validity_error
            entries.append(entry)
            continue
        entry["status"] = "ok"
        entry["error"] = None
        entry["content_hash"] = sha256_hex(payload)
        entry["payload"] = payload
        entries.append(entry)

    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "source": {
            "provider": "ARSO",
            "dataset": "ARSO location forecast",
            "endpoint": ARSO_LOCATION_URL,
            "attribution": ARSO_ATTRIBUTION,
        },
        "fetched_at": fetched_at,
        "content_hash_algorithm": "sha256",
        "locations": entries,
    }


def artifact_name_for(fetched_at: str) -> str:
    """`arso-forecast-<fetched_at compact>`, e.g.
    `arso-forecast-20260915T1415Z` -- MUST stay `arso-forecast-`-prefixed
    (what the local importer's `arso_forecast_buffer.artifact_prefix`
    filters on)."""
    dt = datetime.fromisoformat(fetched_at)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    compact = dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")
    return f"arso-forecast-{compact}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="artifact")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)

    session = requests.Session()
    session.headers.update({"User-Agent": "zevs-arso-forecast-buffer/1"})
    fetched_at = utcnow_iso()
    artifact = build_artifact(session, LOCATIONS, fetched_at, timeout=args.timeout)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    artifact_name = artifact_name_for(fetched_at)
    out_path = out_dir / f"{artifact_name}.json"
    out_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")

    ok_count = sum(1 for e in artifact["locations"] if e.get("status") == "ok")
    total_count = len(artifact["locations"])
    print(
        f"Wrote {out_path} ({out_path.stat().st_size} bytes): "
        f"artifact_name={artifact_name} fetched_at={fetched_at} "
        f"locations_ok={ok_count}/{total_count}"
    )
    if ok_count == 0:
        print("Warning: ALL locations failed to fetch this run -- artifact still uploaded.")

    written = [{"name": artifact_name, "path": str(out_path)}]
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"artifacts={json.dumps(written)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
