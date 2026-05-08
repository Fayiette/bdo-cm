"""
NA Pearl Shop price / discount ingestor (v2).

Parses Pearl Shop announcement HTML:

  - div.tpl_shop_title (title + sibling fragments)
  - div.shop_item_list → div.list_item (item_name_wrap + price_side with percent_badge / del / price_orange)

Writes star-schema style outputs:

  - DIM_Pearl_Shop_Sales.{csv,parquet}     — one row per announcement (groupContentNo, title, publishedAt)
  - FACT_Pearl_Shop_Sales.{csv,parquet}    — one row per item; only postGroupContentNo (no repeated URL)
  - pearl_shop_best_discounts.{csv,parquet} — best discount per item (join-friendly on postGroupContentNo)
  - pearl_shop_catalog.json                 — dim + best discounts for the website

Optional R2 sync (same credentials as other scrapers):
  R2_PEARL_CATALOG_PREFIX  — object prefix (e.g. pearl-catalog). Secret in CI only.
  With --r2-sync and without --full-refresh, if local DIM/FACT CSVs are missing (e.g. fresh CI
  runner), they are downloaded from R2 first so the run merges new board posts into the prior
  snapshot instead of replacing history. FACT dedupes on (postGroupContentNo, normalizedName)
  keeping the row with the richest pricing; DIM dedupes one row per post.

Does not replace patch-notes-pearls.py (impact/patches digest); this is a parallel catalog.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np

USER_AGENT = "bdo-pearl-catalog-v2/1.0 (+https://github.com)"
RETRY_BACKOFFS = (3.0, 5.0, 13.0)


def json_client_safe(obj: Any) -> Any:
    """RFC 8259 friendly for browser JSON.parse: NaN/Inf → null, numpy/pandas scalars → native."""
    if isinstance(obj, dict):
        return {k: json_client_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_client_safe(v) for v in obj]
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return int(obj)
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        x = float(obj)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    if isinstance(obj, np.ndarray):
        return json_client_safe(obj.tolist())
    try:
        if obj is pd.NA:
            return None
    except (AttributeError, TypeError):
        pass
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(obj, "item") and callable(getattr(obj, "item", None)):
        try:
            return json_client_safe(obj.item())
        except Exception:
            pass
    raise TypeError(f"Not JSON-serializable for catalog: {type(obj)!r}")


BASE = "https://www.naeu.playblackdesert.com"
LIST_PATH = "/en-US/News/Notice"

PARSER_VERSION = 2

DIM_CSV = "DIM_Pearl_Shop_Sales.csv"
DIM_PARQUET = "DIM_Pearl_Shop_Sales.parquet"
FACT_CSV = "FACT_Pearl_Shop_Sales.csv"
FACT_PARQUET = "FACT_Pearl_Shop_Sales.parquet"
# Renamed outputs (read once for incremental if new files absent)
OLD_DIM_CSV = "pearl_shop_dim_post.csv"
OLD_FACT_CSV = "pearl_shop_fact_price_events.csv"
# Legacy flat file (pre–FACT/DIM); incremental may read once if fact file missing
LEGACY_EVENTS_CSV = "pearl_shop_price_events.csv"
BEST_CSV = "pearl_shop_best_discounts.csv"
BEST_PARQUET = "pearl_shop_best_discounts.parquet"
CATALOG_JSON = "pearl_shop_catalog.json"

R2_ENV_VARS = ("R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")
R2_CATALOG_PREFIX_ENV = "R2_PEARL_CATALOG_PREFIX"

# Original Pearls → Sale Pearls (allows ~~strike~~ and arrows)
PEAR_ARROW_RE = re.compile(
    r"(?:~~)?([\d,]+)\s*Pearls?\s*(?:~~)?\s*[→\-]+\s*(?:~~)?([\d,]+)\s*Pearls?",
    re.I,
)
# Title prefix like [-60%] or [-50%]
TITLE_BRACKET_PCT_RE = re.compile(r"^\s*\[\s*-?\s*(\d+)\s*%\s*\]\s*", re.I)
# "25% off ..." or "60% off"
PHRASE_PCT_OFF_RE = re.compile(
    r"\b(\d+)\s*%\s*off\b",
    re.I,
)
# Line like "60% ↓" near prices
LINE_PCT_ARROW_RE = re.compile(r"\b(\d+)\s*%\s*[↓▼]", re.I)


def require_r2_env() -> dict[str, str]:
    missing = [name for name in R2_ENV_VARS if not os.environ.get(name)]
    if R2_CATALOG_PREFIX_ENV not in os.environ:
        missing.append(R2_CATALOG_PREFIX_ENV)
    if missing:
        raise SystemExit(
            "--r2-sync requires: "
            + ", ".join(missing)
            + f". Set {R2_CATALOG_PREFIX_ENV} for R2 key prefix."
        )
    prefix = os.environ[R2_CATALOG_PREFIX_ENV].strip().strip("/")
    out = {name: os.environ[name] for name in R2_ENV_VARS}
    out[R2_CATALOG_PREFIX_ENV] = prefix
    return out


def make_r2_client(env: dict[str, str]):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
    )


def r2_object_key(prefix: str, filename: str) -> str:
    if not prefix:
        return filename
    return f"{prefix}/{filename}"


def r2_download_if_exists(client, bucket: str, key: str, local_path: Path) -> bool:
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
    extra_args: dict[str, str] = {}
    suffix = local_path.suffix.lower()
    if suffix == ".json":
        extra_args["ContentType"] = "application/json; charset=utf-8"
    elif suffix == ".csv":
        extra_args["ContentType"] = "text/csv; charset=utf-8"
    elif suffix == ".parquet":
        extra_args["ContentType"] = "application/vnd.apache.parquet"
    if extra_args:
        client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args)
    else:
        client.upload_file(str(local_path), bucket, key)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def board_html_looks_blocked(html: str) -> bool:
    """Pearl Abyss uses Imperva/Incapsula; blocked responses are tiny shell pages."""
    if len(html) < 8000 and (
        "Incapsula" in html
        or "_Incapsula_" in html
        or 'id="main-iframe"' in html
        or "Request unsuccessful" in html
    ):
        return True
    return False


def collect_board_post_anchors(soup: BeautifulSoup):
    """Prefer legacy list container; fall back to notice/detail links if markup changes."""
    ul = soup.select_one("ul.thumb_nail_list")
    if ul:
        return ul.select('a[href*="groupContentNo="]')
    anchors: list = []
    for a in soup.select('a[href*="groupContentNo="]'):
        href = a.get("href") or ""
        if "/News/" not in href and "Notice" not in href:
            continue
        anchors.append(a)
    return anchors


def get_text(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, str] | None = None,
    quiet: bool = False,
) -> str:
    parsed = urlsplit(url)
    endpoint_path = parsed.path if parsed.path else ""
    endpoint = f"GET {endpoint_path}" if endpoint_path else "GET"
    last_status: int | None = None
    last_err_kind = "UnknownError"
    for attempt, backoff in enumerate((0.0,) + RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            resp = session.get(url, params=params, timeout=45)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_status = getattr(getattr(e, "response", None), "status_code", None)
            last_err_kind = type(e).__name__
            if not quiet:
                st = f" status={last_status}" if last_status is not None else ""
                print(
                    f"  warn: {endpoint} attempt {attempt + 1} failed ({last_err_kind}{st})",
                    file=sys.stderr,
                )
    st = f" status={last_status}" if last_status is not None else ""
    raise RuntimeError(f"All retries failed for {endpoint} ({last_err_kind}{st})")


def parse_pearl_int(raw: str) -> int | None:
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def normalize_item_name(name: str) -> str:
    """
    Normalize for dedupe / analytics grouping.

    We intentionally remove discount prefixes so variants like:
      - "[-33%] Choose Your Warm Journey Box x3"
      - "15% off Naphart Campsite"
    collapse to the same base name.
    """
    n = name.strip()
    # Remove any leading bracketed campaign/tag prefixes, e.g.
    # "[10 Years] Foo", "[Exciting Spring] Foo", "[Event] Foo" → "Foo".
    n = re.sub(r"^(?:\s*\[[^\]]+\]\s*)+", "", n, count=1)
    # Back-compat for explicit percent bracket hint.
    n = TITLE_BRACKET_PCT_RE.sub("", n, count=1)
    # Category/title prefix like "25% off Ship/Horse Gear" → "Ship/Horse Gear"
    n = re.sub(
        r"^\s*-?\s*\d+(?:\.\d+)?\s*%\s*off\s+",
        "",
        n,
        flags=re.I,
        count=1,
    )
    # Treat singular quantity suffix as the same base item:
    # "Foo x1" == "Foo", while x2/x3/... remain distinct bundles.
    n = re.sub(r"\s+x\s*1\s*$", "", n, flags=re.I)
    return re.sub(r"\s+", " ", n).strip()


def stable_catalog_id(normalized_name: str) -> str:
    h = hashlib.sha256(normalized_name.encode("utf-8")).hexdigest()
    return h[:16]


def _classes_include(attr: Any, token: str) -> bool:
    if not attr:
        return False
    if isinstance(attr, str):
        return token in attr.split()
    return token in " ".join(str(x) for x in attr)


def nearest_sales_period_before_element(el) -> str:
    """Sales period from the nearest preceding div.tpl_shop_title (document order)."""
    prev = el.find_previous("div", class_=re.compile(r"\btpl_shop_title\b"))
    if prev:
        desc = prev.select_one("p.desc")
        return desc.get_text(" ", strip=True) if desc else ""
    return ""


def _value_present(v: Any) -> bool:
    """True for real scalar values; False for None / NaN (CSV round-trips)."""
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    return True


def fact_row_completeness(row: dict[str, Any]) -> float:
    o = row.get("originalPearls")
    s = row.get("salePearls")
    d = row.get("discountPercent")
    score = 0.0
    if _value_present(o) and _value_present(s):
        score += 10.0
    elif _value_present(s):
        score += 4.0
    if _value_present(d):
        score += 2.0
    ds = row.get("discountSource") or ""
    if ds == "computed_from_prices":
        score += 1.0
    if row.get("itemSource") == "shop_item_list":
        score += 0.25
    return score


def merge_fact_rows_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Keep best row per (postGroupContentNo, normalizedName) by pricing completeness."""
    if df.empty:
        return df
    work = df.copy()
    work["_completeness"] = work.apply(lambda r: fact_row_completeness(r.to_dict()), axis=1)
    work = work.sort_values("_completeness", ascending=False)
    work = work.drop_duplicates(["postGroupContentNo", "normalizedName"], keep="first")
    return work.drop(columns=["_completeness"])


def enrich_fact_cluster_original_pearls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Same normalizedName may appear with original+sale on one row and null original on another.
    Fill missing originalPearls from the cluster max MSRP so discount can be computed from prices.
    """
    if df.empty or "normalizedName" not in df.columns:
        return df
    out = df.copy()
    if "originalPearls" not in out.columns or "salePearls" not in out.columns:
        return out

    orig_num = pd.to_numeric(out["originalPearls"], errors="coerce")
    sale_num = pd.to_numeric(out["salePearls"], errors="coerce")
    cluster_max_o = orig_num.groupby(out["normalizedName"]).transform("max")

    miss_orig = orig_num.isna()
    has_sale = sale_num.notna()
    has_cluster_o = cluster_max_o.notna()
    fill_mask = miss_orig & has_sale & has_cluster_o

    if not fill_mask.any():
        return out

    out.loc[fill_mask, "originalPearls"] = cluster_max_o.loc[fill_mask]

    o2 = pd.to_numeric(out.loc[fill_mask, "originalPearls"], errors="coerce")
    s2 = pd.to_numeric(out.loc[fill_mask, "salePearls"], errors="coerce")
    pct = np.where((o2 > 0) & s2.notna(), np.round((o2 - s2) / o2 * 100.0, 2), np.nan)
    out.loc[fill_mask, "discountPercent"] = pct

    if "discountSource" in out.columns:
        disc_ok = pd.to_numeric(out["discountPercent"], errors="coerce").notna()
        out.loc[fill_mask & disc_ok, "discountSource"] = "computed_from_prices"

    tag = "cluster_original_fill"
    if "notes" in out.columns:
        for idx in out.index[fill_mask]:
            prev = out.at[idx, "notes"]
            if pd.isna(prev) or str(prev).strip() in ("", "nan"):
                out.at[idx, "notes"] = tag
            elif tag not in str(prev):
                out.at[idx, "notes"] = f"{prev}; {tag}"

    return out


def merge_dim_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """One row per post; prefer richer metadata (longer title) when duplicates exist."""
    if df.empty:
        return df
    d = df.copy()
    if "postTitle" not in d.columns:
        return d.drop_duplicates("postGroupContentNo", keep="last")
    d["_meta_rank"] = d["postTitle"].fillna("").astype(str).str.len()
    d["_date_rank"] = d["postPublishedAt"].fillna("").astype(str).str.len()
    d = d.sort_values(["_meta_rank", "_date_rank"], ascending=False)
    d = d.drop_duplicates("postGroupContentNo", keep="first")
    return d.drop(columns=["_meta_rank", "_date_rank"])


def bootstrap_catalog_csv_from_r2(
    client,
    bucket: str,
    prefix: str,
    data_dir: Path,
) -> None:
    """Download latest DIM/FACT CSV from R2 when missing locally (e.g. fresh CI runner)."""
    for fname in (FACT_CSV, DIM_CSV):
        local = data_dir / fname
        if local.exists():
            continue
        key = r2_object_key(prefix, fname)
        if r2_download_if_exists(client, bucket, key, local):
            print(f"  bootstrapped from R2 → {fname}")


def _collect_lines_from_fragment(frag) -> list[str]:
    lines: list[str] = []
    if frag is None:
        return lines
    for w in frag.select(".item_name_wrap"):
        t = w.get_text(" ", strip=True)
        if t and t not in lines:
            lines.append(t)
    for cell in frag.select(".item_info div"):
        t = cell.get_text(" ", strip=True)
        if t and len(t) < 220 and t not in lines:
            lines.append(t)
    for li in frag.select("li"):
        t = li.get_text(" ", strip=True)
        if t and len(t) < 320 and t not in lines:
            lines.append(t)
    return lines


def extract_pricing(
    raw_name: str, lines: list[str]
) -> dict[str, Any]:
    """Derive original/sale pearls and discount % from title + body lines."""
    blob = raw_name + "\n" + "\n".join(lines)
    original_pearls: int | None = None
    sale_pearls: int | None = None
    discount_pct: float | None = None
    discount_source = "none"
    notes: list[str] = []

    m = PEAR_ARROW_RE.search(blob)
    if m:
        original_pearls = parse_pearl_int(m.group(1))
        sale_pearls = parse_pearl_int(m.group(2))
        if (
            original_pearls is not None
            and sale_pearls is not None
            and original_pearls > 0
        ):
            discount_pct = round(
                (original_pearls - sale_pearls) / original_pearls * 100.0,
                2,
            )
            discount_source = "computed_from_prices"

    tb = TITLE_BRACKET_PCT_RE.match(raw_name)
    if tb:
        pct = float(tb.group(1))
        if discount_pct is None:
            discount_pct = pct
            discount_source = "title_bracket"
        notes.append("title_bracket_pct")

    if discount_pct is None:
        pm = PHRASE_PCT_OFF_RE.search(blob)
        if pm:
            discount_pct = float(pm.group(1))
            discount_source = "percent_only"

    if discount_pct is None:
        lm = LINE_PCT_ARROW_RE.search(blob)
        if lm:
            discount_pct = float(lm.group(1))
            discount_source = "line_percent_arrow"

    # Single pearl amount only (no arrow): treat as current/sale-only
    if original_pearls is None and sale_pearls is None:
        singles = re.findall(r"\b([\d,]+)\s*Pearls?\b", blob, flags=re.I)
        if singles and not PEAR_ARROW_RE.search(blob):
            # Take last plausible standalone price (often the prominent discount line)
            sale_pearls = parse_pearl_int(singles[-1])
            if sale_pearls is not None and discount_source == "none":
                discount_source = "single_price_only"

    return {
        "originalPearls": original_pearls,
        "salePearls": sale_pearls,
        "discountPercent": discount_pct,
        "discountSource": discount_source,
        "notes": "; ".join(notes) if notes else None,
    }


def parse_shop_item_list_events(
    area, post_group_no: int
) -> list[dict[str, Any]]:
    """Structured card rows: div.shop_item_list > div.list_item with price_side."""
    events: list[dict[str, Any]] = []
    for shop_list in area.select("div.shop_item_list"):
        period = nearest_sales_period_before_element(shop_list)
        for li in shop_list.find_all(
            "div", class_=lambda c: _classes_include(c, "list_item"), recursive=False
        ):
            name_el = li.select_one(".item_name_wrap")
            price_side = li.select_one(".price_side")
            if not name_el or not price_side:
                continue
            raw_name = name_el.get_text(" ", strip=True)
            if not raw_name:
                continue
            price_text = price_side.get_text(" ", strip=True)
            badge = price_side.select_one(".percent_badge")
            badge_text = badge.get_text(" ", strip=True) if badge else ""
            lines: list[str] = [price_text]
            if badge_text and badge_text not in price_text:
                lines.append(badge_text)

            pricing = extract_pricing(raw_name, lines)
            norm = normalize_item_name(raw_name)
            if not norm:
                continue

            events.append(
                {
                    "catalogItemId": stable_catalog_id(norm),
                    "normalizedName": norm,
                    "itemRawName": raw_name,
                    "salesPeriodText": period.strip(),
                    "postGroupContentNo": post_group_no,
                    "originalPearls": pricing["originalPearls"],
                    "salePearls": pricing["salePearls"],
                    "discountPercent": pricing["discountPercent"],
                    "discountSource": pricing["discountSource"],
                    "notes": pricing["notes"],
                    "itemSource": "shop_item_list",
                    "parserVersion": PARSER_VERSION,
                }
            )
    return events


def parse_published_sort_key(raw: str | None) -> tuple[int, str]:
    if not raw:
        return (0, "")
    m = re.match(r"^([A-Za-z]+)\s+(\d+),\s+(\d{4})", raw.strip())
    if not m:
        return (0, raw)
    mon_s, day_s, year_s = m.group(1), m.group(2), m.group(3)
    try:
        dt = datetime.strptime(f"{mon_s} {int(day_s)}, {year_s}", "%b %d, %Y")
        return (int(dt.timestamp()), raw)
    except ValueError:
        return (0, raw)


def parse_detail_catalog(html: str, group_no: int, source_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one(".title_area strong.title")
    date_el = soup.select_one(".title_area span.date")
    post_title = title_el.get_text(" ", strip=True) if title_el else ""
    published = date_el.get_text(" ", strip=True) if date_el else ""

    dim = {
        "postGroupContentNo": group_no,
        "postTitle": post_title,
        "postPublishedAt": published,
    }

    events: list[dict[str, Any]] = []
    area = soup.select_one(".contents_area.editor_area")
    if not area:
        return {
            "dim": dim,
            "events": [],
            "parserVersion": PARSER_VERSION,
        }

    for title_block in area.select("div.tpl_shop_title"):
        name_el = title_block.select_one("p.title")
        desc_el = title_block.select_one("p.desc")
        if not name_el:
            continue
        raw_name = name_el.get_text(" ", strip=True)
        if not raw_name:
            continue
        period = desc_el.get_text(" ", strip=True) if desc_el else ""

        lines: list[str] = []
        sib = title_block.next_sibling
        while sib is not None:
            el_name = getattr(sib, "name", None) if hasattr(sib, "name") else None
            if el_name == "div":
                classes = " ".join(sib.get("class", []))
                if "tpl_shop_title" in classes or "tpl_title_bullet" in classes:
                    break
                for line in _collect_lines_from_fragment(sib):
                    if line not in lines:
                        lines.append(line)
            sib = sib.next_sibling

        pricing = extract_pricing(raw_name, lines)
        norm = normalize_item_name(raw_name)
        if not norm:
            continue

        events.append(
            {
                "catalogItemId": stable_catalog_id(norm),
                "normalizedName": norm,
                "itemRawName": raw_name,
                "salesPeriodText": period.strip(),
                "postGroupContentNo": group_no,
                "originalPearls": pricing["originalPearls"],
                "salePearls": pricing["salePearls"],
                "discountPercent": pricing["discountPercent"],
                "discountSource": pricing["discountSource"],
                "notes": pricing["notes"],
                "itemSource": "tpl_shop_title",
                "parserVersion": PARSER_VERSION,
            }
        )

    events.extend(parse_shop_item_list_events(area, group_no))

    merged_df = merge_fact_rows_duplicates(pd.DataFrame(events)) if events else pd.DataFrame()
    merged_df = enrich_fact_cluster_original_pearls(merged_df)
    events_out = merged_df.to_dict("records") if not merged_df.empty else []

    return {
        "dim": dim,
        "events": events_out,
        "parserVersion": PARSER_VERSION,
    }


def listing_urls(session: requests.Session, limit: int) -> list[tuple[int, str]]:
    """
    Crawl Pearl Shop board pages (`page=N`) and collect unique detail URLs.
    """
    url = f"{BASE}{LIST_PATH}"
    seen: set[int] = set()
    out: list[tuple[int, str]] = []

    # Hard safety ceiling; usually we stop much earlier.
    max_pages = max(1, (limit // 20) + 12)
    for page in range(1, max_pages + 1):
        params = {"boardType": "5", "countryType": "en-US"}
        if page > 1:
            params["page"] = str(page)

        html = get_text(session, url, params=params)
        if board_html_looks_blocked(html):
            raise RuntimeError(
                "Pearl Shop board HTML looks like a bot-protection page (e.g. Incapsula), "
                "not the real notice list — no ul.thumb_nail_list. "
                "Try again from a residential IP, use browser cookies in the session, "
                "or run locally after passing the challenge once."
            )
        soup = BeautifulSoup(html, "html.parser")
        link_anchors = collect_board_post_anchors(soup)
        if not link_anchors:
            if page == 1:
                raise RuntimeError(
                    "Could not find Pearl Shop post links on the board page "
                    "(no ul.thumb_nail_list and no a[href*=groupContentNo] in /News/). "
                    "The site markup may have changed, or the response was incomplete."
                )
            break

        before = len(seen)
        for a in link_anchors:
            href = a.get("href") or ""
            m = re.search(r"groupContentNo=(\d+)", href)
            if not m:
                continue
            gid = int(m.group(1))
            if gid in seen:
                continue
            seen.add(gid)
            if href.startswith("/"):
                full = BASE + href
            elif href.startswith("http"):
                full = href
            else:
                full = BASE + "/" + href.lstrip("/")
            if "countryType=" not in full:
                full += ("&" if "?" in full else "?") + "countryType=en-US"
            out.append((gid, full))
            if len(out) >= limit:
                return out

        # If a page yields no new IDs, pagination is exhausted.
        if len(seen) == before:
            break
    return out


def build_best_discounts_df(fact_df: pd.DataFrame, dim_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame()
    ev = fact_df.copy()
    if not dim_df.empty and "postGroupContentNo" in dim_df.columns:
        d = dim_df[["postGroupContentNo", "postPublishedAt"]].drop_duplicates(
            "postGroupContentNo", keep="last"
        )
        ev = ev.merge(d, on="postGroupContentNo", how="left")
    elif "postPublishedAt" not in ev.columns:
        ev["postPublishedAt"] = pd.NA

    ev["_sort_ts"] = ev["postPublishedAt"].map(
        lambda x: parse_published_sort_key(str(x) if pd.notna(x) else "")[0]
    )
    valid = ev[ev["discountPercent"].notna()].copy()
    if valid.empty:
        return pd.DataFrame()
    valid = valid.sort_values(
        ["normalizedName", "discountPercent", "salePearls", "_sort_ts"],
        ascending=[True, False, True, False],
    )
    best = valid.groupby("normalizedName", as_index=False).first()
    best = best.drop(columns=["_sort_ts"], errors="ignore")
    out_cols = [
        "catalogItemId",
        "normalizedName",
        "itemRawName",
        "originalPearls",
        "salePearls",
        "discountPercent",
        "discountSource",
        "postPublishedAt",
        "postGroupContentNo",
        "salesPeriodText",
        "itemSource",
    ]
    present = [c for c in out_cols if c in best.columns]
    return best[present]


def _legacy_flat_to_fact_dim(legacy_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split pre–FACT/DIM combined CSV into fact + dim frames."""
    df = legacy_df.copy()
    dim_cols = ["postGroupContentNo", "postTitle", "postPublishedAt"]
    have_dim = all(c in df.columns for c in dim_cols)
    if have_dim:
        dim = df[dim_cols].drop_duplicates("postGroupContentNo", keep="last")
    else:
        dim = pd.DataFrame(columns=dim_cols)
    drop = [c for c in ("postTitle", "postPublishedAt", "postSourceUrl") if c in df.columns]
    fact = df.drop(columns=drop, errors="ignore")
    return fact, dim


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Pearl Shop price/discount catalog v2 (CSV/Parquet/JSON)."
    )
    ap.add_argument("--data-dir", default="data", help="Output directory")
    ap.add_argument("--max-posts", type=int, default=4240, help="Max Pearl Shop posts to fetch")
    ap.add_argument(
        "--r2-sync",
        action="store_true",
        help=f"Sync outputs to R2 using {R2_CATALOG_PREFIX_ENV}.",
    )
    ap.add_argument("--full-refresh", action="store_true")
    ap.add_argument(
        "--incremental-stop-after",
        type=int,
        default=0,
        metavar="N",
        help="Early exit after N consecutive known post IDs (0 = off; recommended for daily/CI)",
    )
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "dim_csv": data_dir / DIM_CSV,
        "dim_parquet": data_dir / DIM_PARQUET,
        "fact_csv": data_dir / FACT_CSV,
        "fact_parquet": data_dir / FACT_PARQUET,
        "best_csv": data_dir / BEST_CSV,
        "best_parquet": data_dir / BEST_PARQUET,
        "catalog_json": data_dir / CATALOG_JSON,
    }

    now = datetime.now(timezone.utc).replace(microsecond=0)
    generated = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    r2_client = None
    r2_env: dict[str, str] = {}
    if args.r2_sync:
        r2_env = require_r2_env()
        r2_client = make_r2_client(r2_env)
        pfx = r2_env[R2_CATALOG_PREFIX_ENV]
        print(f"  r2 catalog prefix ({R2_CATALOG_PREFIX_ENV}): {pfx or '(bucket root)'}")
        if not args.full_refresh:
            bootstrap_catalog_csv_from_r2(
                r2_client, r2_env["R2_BUCKET"], pfx, data_dir
            )

    existing_facts: list[dict[str, Any]] = []
    existing_dims: list[dict[str, Any]] = []
    if not args.full_refresh:
        try:
            if paths["fact_csv"].exists():
                existing_facts = pd.read_csv(paths["fact_csv"]).to_dict("records")
                if paths["dim_csv"].exists():
                    existing_dims = pd.read_csv(paths["dim_csv"]).to_dict("records")
            elif (data_dir / OLD_FACT_CSV).exists():
                existing_facts = pd.read_csv(data_dir / OLD_FACT_CSV).to_dict("records")
                if (data_dir / OLD_DIM_CSV).exists():
                    existing_dims = pd.read_csv(data_dir / OLD_DIM_CSV).to_dict("records")
                print(
                    f"  note: loaded incremental state from {OLD_FACT_CSV} / {OLD_DIM_CSV}; "
                    f"next run will use {FACT_CSV} / {DIM_CSV}",
                    file=sys.stderr,
                )
            elif (data_dir / LEGACY_EVENTS_CSV).exists():
                leg = pd.read_csv(data_dir / LEGACY_EVENTS_CSV)
                f_df, d_df = _legacy_flat_to_fact_dim(leg)
                existing_facts = f_df.to_dict("records")
                existing_dims = d_df.to_dict("records")
                print(
                    f"  note: migrated incremental state from {LEGACY_EVENTS_CSV}",
                    file=sys.stderr,
                )
        except Exception as e:
            print(f"  warn: could not read existing CSV ({e}); starting fresh", file=sys.stderr)

    known_posts: set[int] = set()
    for row in existing_facts:
        try:
            known_posts.add(int(row.get("postGroupContentNo", -1)))
        except (TypeError, ValueError):
            continue

    session = make_session()
    incremental = not args.full_refresh
    stop_after = max(0, args.incremental_stop_after)

    entries = listing_urls(session, args.max_posts)
    print(f"Listing Pearl Shop board (max {args.max_posts}), found {len(entries)} posts")

    fetch_count = 0
    skip_count = 0
    consecutive_known = 0
    early_exit = False
    new_facts: list[dict[str, Any]] = []
    new_dims: list[dict[str, Any]] = []

    for gid, detail_url in entries:
        if incremental and gid in known_posts:
            skip_count += 1
            consecutive_known += 1
            print(f"  [{gid}] skip (already in fact store)")
            if stop_after and consecutive_known >= stop_after:
                early_exit = True
                print(f"  early exit after {stop_after} consecutive known")
                break
            continue
        consecutive_known = 0
        try:
            print(f"  [{gid}] fetch …")
            html = get_text(session, detail_url, quiet=True)
            parsed = parse_detail_catalog(html, gid, detail_url)
            new_dims.append(parsed["dim"])
            new_facts.extend(parsed.get("events") or [])
            fetch_count += 1
            time.sleep(0.35)
        except Exception as e:
            print(f"  [{gid}] ERROR: {e}", file=sys.stderr)

    all_facts = existing_facts + new_facts
    df_fact = merge_fact_rows_duplicates(pd.DataFrame(all_facts)) if all_facts else pd.DataFrame()
    df_fact = enrich_fact_cluster_original_pearls(df_fact)

    all_dim_rows = existing_dims + new_dims
    df_dim = merge_dim_duplicates(pd.DataFrame(all_dim_rows)) if all_dim_rows else pd.DataFrame()

    df_dim.to_csv(paths["dim_csv"], index=False)
    if not df_dim.empty:
        df_dim.to_parquet(paths["dim_parquet"], index=False)

    df_fact.to_csv(paths["fact_csv"], index=False)
    if not df_fact.empty:
        df_fact.to_parquet(paths["fact_parquet"], index=False)

    best_df = build_best_discounts_df(df_fact, df_dim)
    best_df.to_csv(paths["best_csv"], index=False)
    if not best_df.empty:
        best_df.to_parquet(paths["best_parquet"], index=False)

    catalog_doc = {
        "parserVersion": PARSER_VERSION,
        "region": "na",
        "generatedAtUtc": generated,
        "schema": "pearl_shop_catalog_v2",
        "dimPosts": df_dim.to_dict("records") if not df_dim.empty else [],
        "bestDiscounts": best_df.to_dict("records") if not best_df.empty else [],
        "factRowCount": int(len(df_fact)),
        "dimPostCount": int(len(df_dim)),
        "bestRowCount": int(len(best_df)),
    }
    safe_doc = json_client_safe(catalog_doc)
    paths["catalog_json"].write_text(
        json.dumps(safe_doc, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print(
        f"Wrote dim {paths['dim_csv']} ({len(df_dim)}), "
        f"fact {paths['fact_csv']} ({len(df_fact)}), "
        f"{paths['best_csv']} ({len(best_df)}), {paths['catalog_json']}"
    )
    print(
        f"  summary: fetches={fetch_count}, skipped={skip_count}, early_exit={int(early_exit)}"
    )

    if r2_client is not None:
        pfx = r2_env[R2_CATALOG_PREFIX_ENV]
        bucket = r2_env["R2_BUCKET"]
        for _label, path in paths.items():
            key = r2_object_key(pfx, path.name)
            print(f"R2 upload {key} …")
            r2_upload(r2_client, bucket, path, key)
        print("Done (R2).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
