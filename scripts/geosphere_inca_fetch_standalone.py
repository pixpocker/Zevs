"""Self-contained GitHub Action entry point for the GeoSphere INCA 15-min
buffer (see AGENTS.md, "GeoSphere INCA GitHub buffer").

**Deliberately does NOT import anything from `weather_reliability`.** This
file is deployed BY ITSELF (plus `.github/workflows/inca-buffer.yml`) to
the separate, lightweight public `pixpocker/Zevs` repository via the
Contents API (see `weather_reliability.cli.publish_file_to_github`) -- that
repo holds only generated/published artifacts (the two HTML reports,
`zefir_model_order.json`), never this project's source tree, and its git
history is unrelated to this development repo's. So this script cannot
assume `weather_reliability.geosphere_api`/`cli` are importable at runtime;
everything it needs is duplicated here, deliberately, in the same spirit as
this project's existing Regression v1 Python/Dart dual-implementation
(AGENTS.md: "dve implementaciji iste natancne specifikacije sta sprejemljiv
in pricakovan pristop") -- just within Python instead of across languages.
`tests/test_geosphere_inca_fetch_standalone.py`'s contract test keeps the
two sides' `ARTIFACT_SCHEMA_VERSION`/artifact-name-prefix/`timestamp_
convention` value in sync; anything else that changes on one side (fetch
URL, parameter name, location list) needs a matching manual update on the
other -- there is no shared import to keep them automatically aligned.

**Scope is intentionally narrow**: fetch, a LIGHT validity check (not full
parsing), group by the raw response's own top-level `reference_time`, and
preserve the raw GeoSphere payload verbatim. All canonical INCA parsing
(`geosphere_api.parse_geosphere_series`/`aggregate_inca_hourly`), hourly
aggregation, database writes, and scoring stay exclusively in the local
ZEVS project (`weather_reliability/geosphere_inca_buffer.py`'s
`import_artifact`, which feeds each location's raw `payload` here straight
into `cli.parse_geosphere_inca_raw`/`parse_geosphere_inca_hourly` --
unchanged by this file). This script does not need the real parser at all:
GeoSphere's raw response already carries `reference_time` as a TOP-LEVEL
field (confirmed in `geosphere_api.py`'s `parse_geosphere_series`), so
grouping by run is just `payload.get("reference_time")`, no geometry/
parameter extraction required.

Produces the exact same artifact JSON shape the local importer already
expects (`schema_version`, `source`, `reference_time`, `fetched_at`,
`timestamp_convention`, `interval_minutes`, `locations[]` with `status`/
`error`/raw `payload`) -- that contract is what must never drift, even
though the code producing it now lives in a different file/repo than the
code consuming it.

One artifact JSON FILE PER DISTINCT `reference_time` actually observed --
almost always exactly one (all locations fetched within seconds of each
other share one INCA run), but if GeoSphere's run advances MID-FETCH (a
location fetched later returns a newer `reference_time` than one fetched
earlier), locations are split by their own observed `reference_time` and
written as SEPARATE artifact files, never merged into one with mixed runs
-- see this repo's `tests/test_geosphere_inca_fetch_standalone.py::
test_build_artifacts_splits_by_reference_time_when_it_changes_mid_fetch`.
A location that failed to fetch (no reference_time to group by) is
attached to the LARGEST group (this cycle's majority/primary run, tied
broken by earliest-observed) -- harmless either way since a
`status="error"` entry carries no payload and the import side always
skips it regardless of which artifact it ends up in.

Writes:
  - one artifact JSON file per produced artifact, under `--out-dir`
    (default: artifact/), named `<artifact_name>.json`;
  - `artifacts` to `$GITHUB_OUTPUT` as a JSON array of
    `{"name": ..., "path": ...}` objects -- the workflow's upload job fans
    out over this via a matrix, since the count is only known at fetch
    time, not at workflow-authoring time.

Exit code is always 0 (even when every location failed to fetch) -- a
"some/all locations failed" run still produces a valid, clearly-marked
artifact rather than silently skipping the upload; the importer treats a
failed location as a known gap, not corruption. A non-zero exit is
reserved for something preventing any artifact from being written at all.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

# --- Duplicated constants -- keep in sync with weather_reliability/
# geosphere_api.py and weather_reliability/geosphere_inca_buffer.py. See
# module docstring for why this duplication is deliberate. ------------------

GEOSPHERE_TIMESERIES_BASE_URL = "https://dataset.api.hub.geosphere.at/v1/timeseries/forecast"
INCA_RESOURCE_ID = "nowcast-v1-15min-1km"
INCA_PRECIP_PARAMETER = "rr"
INCA_LABEL = "INCA nowcast"
INCA_INTERVAL_MINUTES = 15
# Mirrors geosphere_api.IncaTimestampConvention.END_LABELED.value -- purely
# a diagnostic field recorded in the artifact; the import side always uses
# its OWN current `INCA_RR_TIMESTAMP_CONVENTION` constant for actual
# aggregation, never this recorded value (see geosphere_api.py's docstring
# for why -- the convention is genuinely unproven for INCA specifically).
INCA_RR_TIMESTAMP_CONVENTION_VALUE = "end_labeled"

ARTIFACT_SCHEMA_VERSION = 1
GEOSPHERE_ATTRIBUTION = "Data source: GeoSphere Austria – data.hub.geosphere.at"
GEOSPHERE_LICENSE = "CC BY 4.0"

# Same 10 canonical Zevs locations as config.yml's `locations:` list.
# Small and rarely changing -- kept as a plain hardcoded list rather than
# parsing config.yml (which does not exist in the lightweight publish
# repo this script is deployed to). Update BOTH places if a location is
# ever added/renamed/moved.
LOCATIONS: list[dict[str, Any]] = [
    {"name": "Ljubljana", "latitude": 46.0655, "longitude": 14.5124},
    {"name": "Maribor", "latitude": 46.5678, "longitude": 15.6260},
    {"name": "Celje", "latitude": 46.2365, "longitude": 15.2257},
    {"name": "Kranj", "latitude": 46.2478, "longitude": 14.3647},
    {"name": "Novo mesto", "latitude": 45.8018, "longitude": 15.1773},
    {"name": "Koper", "latitude": 45.5430, "longitude": 13.7135},
    {"name": "Vrhnika", "latitude": 45.9660, "longitude": 14.2717},
    {"name": "Postojna", "latitude": 45.7722, "longitude": 14.1973},
    {"name": "Murska Sobota", "latitude": 46.6521, "longitude": 16.1913},
    {"name": "Kočevje", "latitude": 45.6458, "longitude": 14.8496},
]

# Live-observed 2026-09-15 against the real GeoSphere API (see AGENTS.md):
# ten sequential locations with no delay between them hit a 429 (Too Many
# Requests) on the 9th/10th request. A small inter-request pause plus one
# retry specifically on 429 fixed it in practice.
DEFAULT_REQUEST_DELAY_SECONDS = 0.5
DEFAULT_RATE_LIMIT_RETRY_DELAY_SECONDS = 3.0

FetchJson = Any  # Callable[[str, dict], dict] -- kept loose, no typing import needed for this alone.


def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(value: str) -> datetime:
    # GeoSphere timestamps look like "2026-09-14T16:30+00:00" -- Python's
    # fromisoformat handles the "+00:00" offset directly (3.11+; this
    # runner uses 3.13, see .github/workflows/inca-buffer.yml).
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_inca_raw(session: requests.Session, latitude: float, longitude: float, *, timeout: int) -> dict[str, Any]:
    params = {
        "parameters": INCA_PRECIP_PARAMETER,
        "lat_lon": f"{latitude},{longitude}",
        "output_format": "geojson",
    }
    response = session.get(
        f"{GEOSPHERE_TIMESERIES_BASE_URL}/{INCA_RESOURCE_ID}", params=params, timeout=timeout
    )
    response.raise_for_status()
    return response.json()


def _is_rate_limited(exc: requests.RequestException) -> bool:
    response = getattr(exc, "response", None)
    return response is not None and getattr(response, "status_code", None) == 429


def _fetch_one_location_payload(
    session: requests.Session,
    latitude: float,
    longitude: float,
    *,
    timeout: int,
    rate_limit_retry_delay_seconds: float,
) -> tuple[dict[str, Any] | None, str | None]:
    """Returns (payload, None) on success or (None, error_message) on
    failure. Retries exactly once, only on HTTP 429."""
    for attempt in range(2):
        try:
            return fetch_inca_raw(session, latitude, longitude, timeout=timeout), None
        except requests.RequestException as exc:
            if attempt == 0 and _is_rate_limited(exc):
                time.sleep(rate_limit_retry_delay_seconds)
                continue
            return None, f"fetch failed: {exc}"
    return None, "fetch failed: retries exhausted"  # unreachable


def _light_validity_check(payload: dict[str, Any]) -> str | None:
    """Returns an error string if `payload` is not usable, else None.
    Deliberately shallow -- NOT the real `parse_geosphere_series` (that
    stays local-only). Just enough to (a) confirm there is a
    `reference_time` to group by and (b) avoid archiving an obviously
    empty/out-of-domain response as if it had data."""
    if not isinstance(payload, dict):
        return "response is not a JSON object"
    if not payload.get("reference_time"):
        return "no usable INCA series in response (missing reference_time)"
    features = payload.get("features")
    if not isinstance(features, list) or not features:
        return "no usable INCA series in response (out of domain / empty)"
    parameters = ((features[0].get("properties") or {}).get("parameters")) or {}
    block = parameters.get(INCA_PRECIP_PARAMETER)
    if not isinstance(block, dict) or not block.get("data"):
        return "no usable INCA series in response (missing rr parameter data)"
    return None


def _artifact_shell(reference_time: datetime, fetched_at: str) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "source": {
            "provider": "GeoSphere Austria",
            "dataset": INCA_LABEL,
            "resource_id": INCA_RESOURCE_ID,
            "parameter": INCA_PRECIP_PARAMETER,
            "attribution": GEOSPHERE_ATTRIBUTION,
            "license": GEOSPHERE_LICENSE,
        },
        "reference_time": to_utc_iso(reference_time),
        "fetched_at": fetched_at,
        "timestamp_convention": INCA_RR_TIMESTAMP_CONVENTION_VALUE,
        "interval_minutes": INCA_INTERVAL_MINUTES,
        "locations": [],
    }


def build_artifacts(
    session: requests.Session,
    locations: list[dict[str, Any]],
    fetched_at: str,
    *,
    timeout: int = 30,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    rate_limit_retry_delay_seconds: float = DEFAULT_RATE_LIMIT_RETRY_DELAY_SECONDS,
) -> list[dict[str, Any]]:
    """One artifact dict per DISTINCT reference_time actually observed --
    see module docstring for the full contract and the mid-fetch-rollover
    case. Mirrors `weather_reliability.geosphere_inca_buffer.
    build_artifact_payload`'s algorithm exactly (grouping/tie-break/
    failed-location attachment), just against raw dicts instead of parsed
    `GeosphereSeries` objects."""
    ok_entries_by_reference_time: dict[datetime, list[dict[str, Any]]] = {}
    reference_time_order: list[datetime] = []
    failed_entries: list[dict[str, Any]] = []

    for i, loc in enumerate(locations):
        if i > 0 and request_delay_seconds > 0:
            time.sleep(request_delay_seconds)
        entry: dict[str, Any] = {
            "name": loc["name"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
        }
        payload, error = _fetch_one_location_payload(
            session, loc["latitude"], loc["longitude"],
            timeout=timeout, rate_limit_retry_delay_seconds=rate_limit_retry_delay_seconds,
        )
        if error is not None:
            entry["status"] = "error"
            entry["error"] = error
            failed_entries.append(entry)
            continue

        validity_error = _light_validity_check(payload)
        if validity_error is not None:
            entry["status"] = "error"
            entry["error"] = validity_error
            failed_entries.append(entry)
            continue

        entry["status"] = "ok"
        entry["error"] = None
        entry["payload"] = payload
        ref = parse_iso(str(payload["reference_time"]))
        if ref not in ok_entries_by_reference_time:
            ok_entries_by_reference_time[ref] = []
            reference_time_order.append(ref)
        ok_entries_by_reference_time[ref].append(entry)

    if not ok_entries_by_reference_time:
        reference_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        artifact = _artifact_shell(reference_time, fetched_at)
        artifact["locations"] = failed_entries
        artifact["all_locations_failed"] = True
        return [artifact]

    majority_ref = max(
        reference_time_order,
        key=lambda r: (len(ok_entries_by_reference_time[r]), -reference_time_order.index(r)),
    )

    artifacts: list[dict[str, Any]] = []
    for ref in reference_time_order:
        artifact = _artifact_shell(ref, fetched_at)
        entries = list(ok_entries_by_reference_time[ref])
        if ref == majority_ref:
            entries += failed_entries
        artifact["locations"] = entries
        artifacts.append(artifact)
    return artifacts


def artifact_name_for(artifact: dict[str, Any]) -> str:
    """`inca-<reference_time compact>`, e.g. `inca-20260915T1415Z` -- MUST
    stay `inca-`-prefixed (what the local importer's `geosphere_inca_
    buffer.artifact_prefix` filters on) and MUST NOT be reused as a prefix
    for anything else this workflow uploads (see the staging artifact's
    deliberately non-`inca-`-prefixed name in inca-buffer.yml)."""
    reference_time = parse_iso(str(artifact["reference_time"]))
    compact = reference_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")
    return f"inca-{compact}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="artifact")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)

    session = requests.Session()
    session.headers.update({"User-Agent": "zevs-geosphere-inca-buffer/1"})
    fetched_at = utcnow_iso()
    artifacts = build_artifacts(session, LOCATIONS, fetched_at, timeout=args.timeout)

    from pathlib import Path

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[dict[str, str]] = []
    for artifact in artifacts:
        artifact_name = artifact_name_for(artifact)
        out_path = out_dir / f"{artifact_name}.json"
        out_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")

        ok_count = sum(1 for e in artifact["locations"] if e.get("status") == "ok")
        total_count = len(artifact["locations"])
        print(
            f"Wrote {out_path} ({out_path.stat().st_size} bytes): "
            f"artifact_name={artifact_name} reference_time={artifact['reference_time']} "
            f"locations_ok={ok_count}/{total_count}"
        )
        if artifact.get("all_locations_failed"):
            print("Warning: ALL locations failed to fetch this run -- artifact still uploaded.")
        written.append({"name": artifact_name, "path": str(out_path)})

    if len(written) > 1:
        print(
            f"NOTE: GeoSphere INCA reference_time changed mid-fetch -- "
            f"{len(written)} separate artifacts produced this run "
            f"(never mixed into one): {[w['name'] for w in written]}"
        )

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"artifacts={json.dumps(written)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
