#!/usr/bin/env python3
"""
Macro Dashboard Data Collector
- Fetches 29 FRED series (Treasury yields, spreads, policy rates, liquidity, inflation, breakeven, TIPS)
- Outputs fred_data.js with deflate-raw compressed hex of JSON
- Compatible with index.html (window.FRED_DATA_HEX consumer)

Env:
  FRED_API_KEY (required) - set via GitHub Secrets

Safety:
  - RRPONTSYD is critical. If empty, abort and keep existing fred_data.js.
  - On partial failure of any other series, continue with a warning.
"""

import json
import os
import sys
import time
import zlib
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta


FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()

# 29 series — keep order consistent with historic layout
SERIES = [
    # Treasury yields (11)
    "DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5",
    "DGS7", "DGS10", "DGS20", "DGS30",
    # Spread (1)
    "T10Y3M",
    # Policy rates (2)
    "SOFR", "IORB",
    # Liquidity / balance sheet (3)
    "WDTGAL", "WRESBAL", "RRPONTSYD",
    # Inflation (6) - added 2026-05-16
    "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "PPIACO", "T5YIE",
    # Breakeven 10Y (1) - added 2026-05-16 for real rate calc (5Y already in T5YIE)
    "T10YIE",
    # TIPS real yields (5) - added 2026-05-16 for TIPS curve visualization
    "DFII5", "DFII7", "DFII10", "DFII20", "DFII30",
]

# Per-series observation limit — keeps bundle size modest while preserving
# enough history for the time-series charts (~3 years of daily data).
OBSERVATION_LIMIT = 800

# Any series in this set MUST return non-empty data or the run is aborted.
REQUIRED_SERIES = {"RRPONTSYD", "DGS10"}

OUTPUT_PATH = "fred_data.js"
KST = timezone(timedelta(hours=9))


def fetch_series(series_id, max_retries=4):
    """Return list of {date, value} for the given FRED series (desc by date).

    Retries with exponential backoff on transient errors (5xx, timeouts).
    FRED occasionally 500s under parallel load — a short wait clears it.
    """
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        "?" + urllib.parse.urlencode({
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": OBSERVATION_LIMIT,
        })
    )

    last_err = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "macro-dashboard-actions/1.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            obs = data.get("observations", []) or []
            return [{"date": o["date"], "value": o["value"]} for o in obs]
        except urllib.error.HTTPError as e:
            last_err = e
            # Retry only on 5xx (FRED transient) / 429 (rate limit)
            if e.code < 500 and e.code != 429:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = e
        # backoff: 1s, 2s, 4s, 8s
        time.sleep(2 ** attempt)
    raise last_err if last_err else RuntimeError("unknown error")


def fetch_all():
    """Fetch all series in parallel (bounded). Returns (series_dict, errors).

    max_workers=4 is a deliberate conservative choice: FRED has shown flaky
    behavior under 8+ parallel requests per key. 4 completes in ~2-3s.
    """
    result = {}
    errors = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_series, sid): sid for sid in SERIES}
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                result[sid] = fut.result()
                n = len(result[sid])
                print(f"  [ok] {sid:<10} {n} observations")
            except Exception as e:
                errors.append((sid, str(e)))
                result[sid] = []
                print(f"  [ERR] {sid:<10} {e}", file=sys.stderr)
    return result, errors


def build_payload(series_data):
    """Build the JSON payload consumed by index.html."""
    # Establish sync/prev dates from DGS10 (most reliable daily series)
    dgs10 = [o for o in series_data.get("DGS10", []) if o.get("value") not in (None, ".", "")]
    if not dgs10:
        raise RuntimeError("DGS10 returned no usable observations — aborting to protect existing data.")

    sync_date = dgs10[0]["date"]
    prev_date = dgs10[1]["date"] if len(dgs10) > 1 else sync_date

    now_kst = datetime.now(KST).strftime("%Y.%m.%d %H:%M")

    return {
        "series": series_data,
        "syncDate": sync_date,
        "prevDate": prev_date,
        "updatedAt": now_kst,
    }


def compress_hex(payload):
    """deflate-raw + hex encode, matching DecompressionStream('deflate-raw') in index.html."""
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    # wbits=-15 => raw DEFLATE (no zlib header/trailer), matches 'deflate-raw'
    compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
    compressed = compressor.compress(raw) + compressor.flush()
    return compressed.hex(), len(raw), len(compressed)


def main():
    if not FRED_API_KEY:
        print("ERROR: FRED_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(2)

    print(f"Fetching {len(SERIES)} FRED series...")
    series_data, errors = fetch_all()

    # Required-series guard: never overwrite existing data if these are missing
    for sid in REQUIRED_SERIES:
        if not series_data.get(sid):
            print(f"ERROR: Required series {sid} is empty. Aborting without overwriting {OUTPUT_PATH}.",
                  file=sys.stderr)
            sys.exit(1)

    try:
        payload = build_payload(series_data)
    except Exception as e:
        print(f"ERROR building payload: {e}", file=sys.stderr)
        sys.exit(1)

    hex_str, raw_size, zip_size = compress_hex(payload)
    ratio = zip_size / raw_size * 100 if raw_size else 0

    # Write fred_data.js in the exact format index.html expects
    js_content = f'window.FRED_DATA_HEX="{hex_str}";\n'
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\n[done] {OUTPUT_PATH}")
    print(f"  syncDate:  {payload['syncDate']}")
    print(f"  prevDate:  {payload['prevDate']}")
    print(f"  updatedAt: {payload['updatedAt']}")
    print(f"  raw JSON:  {raw_size:,} bytes")
    print(f"  compressed hex file: {len(js_content):,} bytes (ratio {ratio:.1f}%)")
    if errors:
        print(f"  warnings: {len(errors)} series failed (non-critical)")
        for sid, msg in errors:
            print(f"    - {sid}: {msg}")


if __name__ == "__main__":
    main()
