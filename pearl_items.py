"""
BDO Pearl Items Market Scraper.

Pulls every BDO Pearl Item (mainCategory 55: Apparel + Functional + Mount + Pets)
from a region's market via API_BASE, then writes a star-schema snapshot:

  data/DIM_Pearl_Items_ID.{parquet,csv}        - id <-> name/category lookup
  data/FACT_pearl_apparel_sets.{parquet,csv}   - subCategories 1, 2, 5
  data/FACT_pearl_apparel_pieces.{parquet,csv} - subCategories 3, 4
  data/FACT_pearl_functional.{parquet,csv}     - subCategory 6
  data/FACT_pearl_mount.{parquet,csv}          - subCategory 7
  data/FACT_pearl_pet.{parquet,csv}            - subCategory 8

FACT files are unique on (region, id, UTC date). Re-runs the same UTC day replace
that day's existing rows. The trailing `how_many_today` column is recomputed every
run as max(0, totalTrades_today - totalTrades_strict_yesterday).

This scraper intentionally does not call GetWorldMarketSubList; FACT writes
priceMin/priceMax/lastSoldTime as null to keep runs fast and stable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from typing import Iterable

import pandas as pd
import requests


MAIN_CATEGORY = 55
CHUNK_SIZE = 100
USER_AGENT = "bdo-pearl-items/1.0 (+https://github.com)"
RETRY_BACKOFFS = (3.0, 5.0, 13.0)
INTER_CHUNK_SLEEP_S = 1.5
INTER_REGION_SLEEP_S = 3.5
CANONICAL_REGION_ORDER = ("na", "eu", "sa", "kr")

# IDs that have been observed to break SubList batches even when other IDs in
# the same batch are healthy. The first time any of these appears in a failing
# batch during a run, it is dropped from all subsequent enrichment requests for
# the rest of that run.
BATCH_FAIL_SKIP_IDS: frozenset[int] = frozenset({601046})

SUB_TO_CATEGORY: dict[int, str] = {
    1: "apparel_sets",
    2: "apparel_sets",
    5: "apparel_sets",
    3: "apparel_pieces",
    4: "apparel_pieces",
    6: "functional",
    7: "mount",
    8: "pet",
}

# Subcategories excluded from default scrape. They remain valid in
# SUB_TO_CATEGORY so they can still be re-enabled explicitly via --include-subs.
DEFAULT_EXCLUDED_SUBS: frozenset[int] = frozenset({3, 4})

CATEGORY_TO_FILE: dict[str, str] = {
    "apparel_sets": "FACT_pearl_apparel_sets",
    "apparel_pieces": "FACT_pearl_apparel_pieces",
    "functional": "FACT_pearl_functional",
    "mount": "FACT_pearl_mount",
    "pet": "FACT_pearl_pet",
}

DIM_BASENAME = "DIM_Pearl_Items_ID"

DIM_COLUMNS = ["id", "name", "mainCategory", "subCategory", "category", "region"]
FACT_COLUMNS = [
    "pulled_at_utc",
    "pulled_at_unix",
    "region",
    "id",
    "currentStock",
    "basePrice",
    "totalTrades",
    "priceMin",
    "priceMax",
    "lastSoldTime",
    "how_many_today",
]


# ---------------------------------------------------------------------------
# R2 sync (S3-compatible)
# ---------------------------------------------------------------------------

# Credential env vars (must be non-empty). R2_PREFIX must be set but may be empty
# for bucket root (e.g. export R2_PREFIX=).
R2_ENV_VARS = ("R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")


def r2_object_key(prefix: str, filename: str) -> str:
    if not prefix:
        return filename
    return f"{prefix}/{filename}"


def require_r2_env() -> dict[str, str]:
    """Return R2 config (credentials + normalized object prefix), or raise SystemExit."""
    missing = [name for name in R2_ENV_VARS if not os.environ.get(name)]
    if "R2_PREFIX" not in os.environ:
        missing.append("R2_PREFIX")
    if missing:
        raise SystemExit(
            "--r2-sync requires the following environment variables to be set: "
            + ", ".join(missing)
            + ". Use R2_PREFIX=YourFolder for a prefix; "
            "set R2_PREFIX to empty for bucket root."
        )
    prefix = os.environ["R2_PREFIX"].strip().strip("/")
    out = {name: os.environ[name] for name in R2_ENV_VARS}
    out["R2_PREFIX"] = prefix
    return out


def require_api_base() -> str:
    """Return API base URL from env, or fail fast if missing."""
    api_base = os.environ.get("API_BASE", "").strip().rstrip("/")
    if not api_base:
        raise SystemExit(
            "API_BASE environment variable is required."
        )
    return api_base


def make_r2_client(env: dict[str, str]):
    import boto3  # local import so the script runs without boto3 when --r2-sync is off

    return boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
    )


def r2_download_if_exists(client, bucket: str, key: str, local_path: Path) -> bool:
    """Download object to local_path. Return True if downloaded, False on 404."""
    from botocore.exceptions import ClientError

    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(bucket, key, str(local_path))
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("404", "NoSuchKey") or status == 404:
            return False
        raise


def r2_upload(client, bucket: str, local_path: Path, key: str) -> None:
    client.upload_file(str(local_path), bucket, key)


def r2_pre_run_download(client, bucket: str, prefix: str, data_dir: Path) -> None:
    print("R2 sync: downloading existing parquets ...")
    for base in [DIM_BASENAME, *CATEGORY_TO_FILE.values()]:
        filename = f"{base}.parquet"
        object_key = r2_object_key(prefix, filename)
        local = data_dir / filename
        downloaded = r2_download_if_exists(client, bucket, object_key, local)
        marker = "downloaded" if downloaded else "not in bucket (will start empty)"
        print(f"  {object_key}: {marker}")
    print()


def r2_post_run_upload(client, bucket: str, prefix: str, data_dir: Path) -> None:
    print()
    print("R2 sync: uploading parquet+csv pairs ...")
    # Parquets first (source of truth); CSVs second (mirror).
    for base in [DIM_BASENAME, *CATEGORY_TO_FILE.values()]:
        for ext in ("parquet", "csv"):
            filename = f"{base}.{ext}"
            object_key = r2_object_key(prefix, filename)
            local = data_dir / filename
            if not local.exists():
                print(f"  skip {object_key} (no local file)")
                continue
            r2_upload(client, bucket, local, object_key)
            print(f"  uploaded {object_key} ({local.stat().st_size} bytes)")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def get_json(
    session: requests.Session,
    url: str,
    params: dict | None = None,
    quiet: bool = False,
) -> object:
    parsed = urlsplit(url)
    endpoint_path = parsed.path if parsed.path else ""
    endpoint = f"API_BASE{endpoint_path}" if endpoint_path else "API_BASE"
    last_status: int | None = None
    last_err_kind = "UnknownError"
    for attempt, backoff in enumerate((0.0,) + RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_status = None
            response = getattr(e, "response", None)
            if response is not None:
                last_status = getattr(response, "status_code", None)
            last_err_kind = type(e).__name__
            if not quiet:
                status_txt = f" status={last_status}" if last_status is not None else ""
                print(
                    f"  warn: GET {endpoint} attempt {attempt + 1} failed"
                    f" ({last_err_kind}{status_txt})",
                    file=sys.stderr,
                )
    status_txt = f" status={last_status}" if last_status is not None else ""
    raise RuntimeError(
        f"All retries failed for GET {endpoint} ({last_err_kind}{status_txt})"
    )


def fetch_pearl_items(
    session: requests.Session, api_base: str, region: str, lang: str
) -> list[dict]:
    url = f"{api_base}/v2/{region}/pearlItems"
    data = get_json(session, url, params={"lang": lang})
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected /pearlItems response shape: {type(data)}")
    return data


def fetch_sublist_chunk(
    session: requests.Session, api_base: str, region: str, lang: str, ids: list[int]
) -> list[dict]:
    """Single SubList call. Raises on failure; caller handles fallback.

    Flattens list[list[dict]] when the API returns one sub-array per id (sids).
    """
    url = f"{api_base}/v2/{region}/GetWorldMarketSubList"
    params = {"id": ",".join(str(i) for i in ids), "lang": lang}
    data = get_json(session, url, params=params, quiet=True)
    if isinstance(data, dict):
        return [data]
    if not isinstance(data, list):
        raise RuntimeError(
            f"Unexpected GetWorldMarketSubList response shape: {type(data)}"
        )
    out: list[dict] = []
    for entry in data:
        if isinstance(entry, dict):
            out.append(entry)
        elif isinstance(entry, list):
            out.extend(x for x in entry if isinstance(x, dict))
    return out


def fetch_sublist_resilient(
    session: requests.Session,
    api_base: str,
    region: str,
    lang: str,
    ids: list[int],
    failed_ids: list[int],
    skip_ids: set[int],
) -> list[dict]:
    """Fetch a SubList chunk; on failure, bisect and retry; record permanent failures.

    The API_BASE proxy occasionally serves Imperva-blocked 500s on multi-id calls
    even when each id is individually queryable. Bisecting works around it.

    ``skip_ids`` is a run-scoped set of ids that should not be requested again.
    Any id in :data:`BATCH_FAIL_SKIP_IDS` that appears in a failing batch is
    added to ``skip_ids`` and dropped from this and all subsequent retries.
    """
    ids = [i for i in ids if i not in skip_ids]
    if not ids:
        return []
    try:
        return fetch_sublist_chunk(session, api_base, region, lang, ids)
    except Exception as e:
        new_skips = [
            i for i in ids if i in BATCH_FAIL_SKIP_IDS and i not in skip_ids
        ]
        if new_skips:
            for sid in new_skips:
                skip_ids.add(sid)
                if sid not in failed_ids:
                    failed_ids.append(sid)
                print(
                    f"  warn: skipping id={sid} for rest of run "
                    f"after batch of {len(ids)} failed",
                    file=sys.stderr,
                )
            remaining = [i for i in ids if i not in skip_ids]
            if not remaining:
                return []
            time.sleep(INTER_CHUNK_SLEEP_S)
            return fetch_sublist_resilient(
                session, api_base, region, lang, remaining, failed_ids, skip_ids
            )
        if len(ids) == 1:
            print(f"  warn: SubList giving up on id={ids[0]}: {e}", file=sys.stderr)
            failed_ids.extend(ids)
            return []
        mid = len(ids) // 2
        left = ids[:mid]
        right = ids[mid:]
        print(
            f"  warn: SubList chunk of {len(ids)} failed; bisecting "
            f"({len(left)} + {len(right)})",
            file=sys.stderr,
        )
        time.sleep(INTER_CHUNK_SLEEP_S)
        out = fetch_sublist_resilient(
            session, api_base, region, lang, left, failed_ids, skip_ids
        )
        time.sleep(INTER_CHUNK_SLEEP_S)
        out += fetch_sublist_resilient(
            session, api_base, region, lang, right, failed_ids, skip_ids
        )
        return out


def chunked(seq: list[int], size: int) -> Iterable[list[int]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ---------------------------------------------------------------------------
# IO helpers (parquet + csv twins)
# ---------------------------------------------------------------------------


def parquet_path(data_dir: Path, basename: str) -> Path:
    return data_dir / f"{basename}.parquet"


def csv_path(data_dir: Path, basename: str) -> Path:
    return data_dir / f"{basename}.csv"


def read_parquet_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as e:
            print(f"  warn: failed to read {path} ({e}); starting empty", file=sys.stderr)
    return pd.DataFrame(columns=columns)


def write_pair(df: pd.DataFrame, data_dir: Path, basename: str) -> None:
    """Write df as {basename}.parquet and {basename}.csv atomically."""
    pq_final = parquet_path(data_dir, basename)
    csv_final = csv_path(data_dir, basename)
    pq_tmp = pq_final.with_suffix(pq_final.suffix + ".tmp")
    csv_tmp = csv_final.with_suffix(csv_final.suffix + ".tmp")

    df.to_parquet(pq_tmp, index=False)
    df.to_csv(csv_tmp, index=False)

    os.replace(pq_tmp, pq_final)
    os.replace(csv_tmp, csv_final)


# ---------------------------------------------------------------------------
# DIM
# ---------------------------------------------------------------------------


def build_dim_rows(pearl_items: list[dict], region: str) -> pd.DataFrame:
    rows = []
    for item in pearl_items:
        sub = int(item["subCategory"])
        category = SUB_TO_CATEGORY.get(sub)
        if category is None:
            continue
        rows.append(
            {
                "id": int(item["id"]),
                "name": item["name"],
                "mainCategory": int(item["mainCategory"]),
                "subCategory": sub,
                "category": category,
                "region": region,
            }
        )
    df = pd.DataFrame(rows, columns=DIM_COLUMNS)
    df = df.sort_values(["region", "id"]).reset_index(drop=True)
    return df


def upsert_dim(
    new_dim: pd.DataFrame, data_dir: Path, refresh: bool
) -> tuple[pd.DataFrame, int, int]:
    """Return (final_df, total_rows, new_rows_added)."""
    pq = parquet_path(data_dir, DIM_BASENAME)
    if refresh or not pq.exists():
        write_pair(new_dim, data_dir, DIM_BASENAME)
        return new_dim, len(new_dim), len(new_dim) if not pq.exists() else 0

    existing = read_parquet_or_empty(pq, DIM_COLUMNS)
    if existing.empty:
        write_pair(new_dim, data_dir, DIM_BASENAME)
        return new_dim, len(new_dim), len(new_dim)

    existing_keys = set(zip(existing["region"].astype(str), existing["id"].astype(int)))
    mask_new = ~new_dim.apply(
        lambda r: (str(r["region"]), int(r["id"])) in existing_keys, axis=1
    )
    additions = new_dim[mask_new]

    if additions.empty:
        return existing, len(existing), 0

    combined = pd.concat([existing, additions], ignore_index=True)
    combined = combined.sort_values(["region", "id"]).reset_index(drop=True)
    write_pair(combined, data_dir, DIM_BASENAME)
    return combined, len(combined), len(additions)


# ---------------------------------------------------------------------------
# FACT
# ---------------------------------------------------------------------------


def build_today_facts(
    pearl_items: list[dict],
    region: str,
    pulled_at_utc: str,
    pulled_at_unix: int,
) -> pd.DataFrame:
    rows = []
    for item in pearl_items:
        sub = int(item["subCategory"])
        if sub not in SUB_TO_CATEGORY:
            continue
        item_id = int(item["id"])
        rows.append(
            {
                "pulled_at_utc": pulled_at_utc,
                "pulled_at_unix": pulled_at_unix,
                "region": region,
                "id": item_id,
                "currentStock": int(item.get("currentStock", 0) or 0),
                "basePrice": int(item.get("basePrice", 0) or 0),
                "totalTrades": int(item.get("totalTrades", 0) or 0),
                # Intentionally not enriched for pearl scraper speed.
                "priceMin": None,
                "priceMax": None,
                "lastSoldTime": None,
                "how_many_today": 0,  # placeholder, overwritten in recompute
                "_subCategory": sub,
            }
        )
    df = pd.DataFrame(rows)
    return df


def _opt_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def recompute_how_many_today(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and recompute how_many_today using the strict-yesterday rule.

    Strict yesterday means the prior row's date must equal today_date - 1 day;
    a gap day or first-ever row yields 0. Negative diffs clamp to 0.
    """
    if df.empty:
        return df

    df = df.sort_values(["region", "id", "pulled_at_unix"]).reset_index(drop=True)

    today_d = pd.to_datetime(df["pulled_at_utc"].str[:10], format="%Y-%m-%d")
    expected_yday = (today_d - pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")

    g = df.groupby(["region", "id"], sort=False)
    prev_total = g["totalTrades"].shift(1)
    prev_date = g["pulled_at_utc"].shift(1).str[:10]

    diff = (df["totalTrades"].astype("Int64") - prev_total.astype("Int64")).fillna(0)
    diff = diff.clip(lower=0).astype("int64")

    matches_strict_yday = prev_date == expected_yday
    df["how_many_today"] = diff.where(matches_strict_yday, 0).astype("int64")
    return df


FACT_NULLABLE_INT_COLS = ("priceMin", "priceMax", "lastSoldTime")


def coerce_fact_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Force nullable Int64 on the API-may-omit columns and plain int64 elsewhere."""
    if df.empty:
        return df
    for col in FACT_NULLABLE_INT_COLS:
        df[col] = pd.array(df[col], dtype="Int64")
    for col in ("pulled_at_unix", "id", "currentStock", "basePrice", "totalTrades", "how_many_today"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
    return df


def write_fact_for_category(
    category: str,
    today_rows: pd.DataFrame,
    data_dir: Path,
    today_date: str,
) -> tuple[int, int]:
    """Returns (rows_for_today, replaced_same_day_rows)."""
    basename = CATEGORY_TO_FILE[category]
    pq = parquet_path(data_dir, basename)
    existing = read_parquet_or_empty(pq, FACT_COLUMNS)

    today_rows = today_rows[FACT_COLUMNS].copy()
    today_rows = coerce_fact_dtypes(today_rows)
    today_keys = set(
        zip(
            today_rows["region"].astype(str),
            today_rows["id"].astype(int),
        )
    )

    replaced = 0
    if not existing.empty:
        existing = coerce_fact_dtypes(existing)
        existing_dates = existing["pulled_at_utc"].astype(str).str[:10]
        same_day_mask = existing_dates == today_date
        existing_keys = list(
            zip(
                existing["region"].astype(str),
                existing["id"].astype(int),
            )
        )
        same_day_and_in_pull = same_day_mask & pd.Series(
            [k in today_keys for k in existing_keys], index=existing.index
        )
        replaced = int(same_day_and_in_pull.sum())
        existing = existing[~same_day_and_in_pull]
        combined = pd.concat([existing, today_rows], ignore_index=True)
    else:
        combined = today_rows

    combined = recompute_how_many_today(combined)
    combined = combined[FACT_COLUMNS]

    write_pair(combined, data_dir, basename)
    return len(today_rows), replaced


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_subs(raw: str) -> set[int]:
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            raise SystemExit(f"--include-subs: invalid integer {part!r}")
    if not out:
        raise SystemExit("--include-subs: must include at least one subCategory")
    unknown = out - set(SUB_TO_CATEGORY.keys())
    if unknown:
        raise SystemExit(
            f"--include-subs: unknown subCategory ids {sorted(unknown)}; "
            f"valid: {sorted(SUB_TO_CATEGORY)}"
        )
    return out


def parse_regions(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        region = part.strip().lower()
        if not region:
            continue
        if region in seen:
            continue
        seen.add(region)
        out.append(region)
    if not out:
        raise SystemExit("--regions: must include at least one region code")
    canonical = [r for r in CANONICAL_REGION_ORDER if r in out]
    extras = [r for r in out if r not in CANONICAL_REGION_ORDER]
    return canonical + extras


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pull BDO Pearl Item market data via API_BASE into star-schema parquet+csv files."
    )
    parser.add_argument(
        "--regions",
        default="na,eu,sa,kr",
        help="Comma-separated regions to scrape (default: na,eu,sa,kr)",
    )
    parser.add_argument("--data-dir", default="data", help="Output directory (default: data)")
    parser.add_argument("--lang", default="en", help="Language for item names (default: en)")
    parser.add_argument(
        "--include-subs",
        default=",".join(
            str(s)
            for s in sorted(SUB_TO_CATEGORY)
            if s not in DEFAULT_EXCLUDED_SUBS
        ),
        help=(
            "Comma-separated subCategory ids to include (default: all 1-8 "
            f"minus {sorted(DEFAULT_EXCLUDED_SUBS)})."
        ),
    )
    parser.add_argument(
        "--refresh-dim",
        action="store_true",
        help="Rewrite DIM_Pearl_Items_ID from scratch instead of upserting.",
    )
    parser.add_argument(
        "--r2-sync",
        action="store_true",
        help=(
            "Download existing parquets from Cloudflare R2 before scraping, then "
            "upload all parquet+csv pairs after. Requires env vars: "
            + ", ".join(R2_ENV_VARS)
            + ", and R2_PREFIX (folder inside the bucket; "
            "set R2_PREFIX empty for bucket root)."
        ),
    )
    args = parser.parse_args()

    include_subs = parse_subs(args.include_subs)
    regions = parse_regions(args.regions)
    api_base = require_api_base()
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    now_utc = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    pulled_at_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    pulled_at_unix = int(now_utc.timestamp())
    today_date = pulled_at_utc[:10]

    print(f"== BDO Pearl Items Scraper ==")
    print(f"  regions  : {regions}")
    print(f"  lang     : {args.lang}")
    print(f"  api base : {api_base}")
    print(f"  data dir : {data_dir.resolve()}")
    print(f"  pulled_at: {pulled_at_utc} ({pulled_at_unix})")
    print(f"  subs     : {sorted(include_subs)}")
    print(f"  r2 sync  : {'on' if args.r2_sync else 'off'}")

    r2_client = None
    r2_env: dict[str, str] = {}
    if args.r2_sync:
        r2_env = require_r2_env()
        r2_client = make_r2_client(r2_env)
        print(f"  r2 prefix: {r2_env['R2_PREFIX'] or '(bucket root)'}")
        r2_pre_run_download(r2_client, r2_env["R2_BUCKET"], r2_env["R2_PREFIX"], data_dir)
    print()

    session = make_session()

    total_raw = 0
    total_filtered = 0
    success_regions: list[str] = []
    failed_regions: list[str] = []
    did_refresh_dim = False
    print("Fetching /pearlItems per region ...")
    for idx, region in enumerate(regions, 1):
        try:
            print(f"  [{region}] fetching /pearlItems ...")
            raw_pearl = fetch_pearl_items(session, api_base, region, args.lang)
            pearl = [
                i
                for i in raw_pearl
                if int(i.get("mainCategory", -1)) == MAIN_CATEGORY
                and int(i.get("subCategory", -1)) in include_subs
            ]
            print(
                f"  [{region}] got {len(raw_pearl)} items, {len(pearl)} after main/sub filter"
            )
            total_raw += len(raw_pearl)
            total_filtered += len(pearl)
            if not pearl:
                print(f"  [{region}] no filtered rows; skipping writes")
                failed_regions.append(region)
                continue

            region_dim = build_dim_rows(pearl, region)
            refresh_now = args.refresh_dim and not did_refresh_dim
            _, dim_total, dim_added = upsert_dim(region_dim, data_dir, refresh=refresh_now)
            if refresh_now:
                did_refresh_dim = True
            refresh_note = " (refreshed)" if refresh_now else ""
            print(
                f"  [{region}] {DIM_BASENAME}: {dim_total} total (+{dim_added} new this run){refresh_note}"
            )

            today_facts = build_today_facts(pearl, region, pulled_at_utc, pulled_at_unix)
            today_facts["category"] = today_facts["_subCategory"].map(SUB_TO_CATEGORY)

            summary: list[tuple[str, int, int]] = []
            for category, group in today_facts.groupby("category", sort=False):
                rows_today, replaced = write_fact_for_category(
                    category, group, data_dir, today_date
                )
                summary.append((CATEGORY_TO_FILE[category], rows_today, replaced))
            for name, _ in CATEGORY_TO_FILE.items():
                if not any(s[0] == CATEGORY_TO_FILE[name] for s in summary):
                    summary.append((CATEGORY_TO_FILE[name], 0, 0))
            summary.sort(key=lambda t: list(CATEGORY_TO_FILE.values()).index(t[0]))
            name_w = max(len(s[0]) for s in summary)
            for fname, rows_today, replaced in summary:
                print(
                    f"  [{region}] {fname.ljust(name_w)} : {rows_today:>5} rows for today "
                    f"(replaced {replaced} same-day rows)"
                )

            success_regions.append(region)
        except Exception as e:
            failed_regions.append(region)
            print(f"  [{region}] ERROR: {e}", file=sys.stderr)
        if idx < len(regions):
            time.sleep(INTER_REGION_SLEEP_S)

    if not success_regions:
        print(
            "ERROR: all configured regions failed; no writes were completed.",
            file=sys.stderr,
        )
        return 1

    print("Skipping SubList enrichment (priceMin/priceMax/lastSoldTime remain null).")
    print()
    print(
        f"Region summary: succeeded={success_regions} failed={failed_regions} "
        f"(raw={total_raw}, filtered={total_filtered})"
    )

    if r2_client is not None:
        r2_post_run_upload(
            r2_client, r2_env["R2_BUCKET"], r2_env["R2_PREFIX"], data_dir
        )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())