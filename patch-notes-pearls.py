"""
BDO NA Pearl Shop patch ingestor.

Fetches the Pearl Shop announcement board, parses each post's HTML, classifies
packs/outfits for Pearl Outfit market relevance, writes normalized JSON, and
optional R2 sync (same credentials as other scrapers; separate object prefix from Pearl).

Incremental runs (default): skip detail fetch for groupContentNo already
present in the merged JSON; stop walking the board after N consecutive known
rows (see --incremental-stop-after). Use --full-refresh to re-parse all.

Output (default): data/patches_normalized.json
R2 object key:    {R2_PATCHES_PREFIX}/patches_normalized.json
(separate from pearl_items.py which uses R2_PREFIX.)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

USER_AGENT = "bdo-pearl-patches/1.0 (+https://github.com)"
RETRY_BACKOFFS = (3.0, 5.0, 13.0)

BASE = "https://www.naeu.playblackdesert.com"
LIST_PATH = "/en-US/News/Notice"
DETAIL_PATH = "/en-US/News/Notice/Detail"

PARSER_VERSION = 1
JSON_FILENAME = "patches_normalized.json"

R2_ENV_VARS = ("R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")
R2_PATCHES_PREFIX_ENV = "R2_PATCHES_PREFIX"

# Reference: https://www.naeu.playblackdesert.com/en-US/Wiki?wikiNo=425
CRON_HEAVY_PHRASES: tuple[str, ...] = (
    "blacksmith's shiny box",
    "transcended premium enhancement box",
    "ultimate premium enhancement box",
    "radiant premium enhancement box",
    "sweet premium enhancement box",
)

# Broader gamble /loot style boxes (wiki + common pearl shop wording)
GAMBLE_BOX_PHRASES: tuple[str, ...] = CRON_HEAVY_PHRASES + (
    "premium enhancement box",
    "shiny box",
    "choose your combat support box",
    "choose your 7-day box",
    "choose your premium value box",
    "reform stone box",
    "forgotten ancient treasure chest",
)

OUTFIT_BOX_PHRASES: tuple[str, ...] = (
    "choose your premium outfit box",
    "premium outfit box",
)

PEARL_BOX_PHRASES: tuple[str, ...] = (
    "pearl box",
    "special pearl box",
    "coupon and pearl box",
)

NON_CM_PHRASES: tuple[str, ...] = (
    "cannot be registered on the central market",
    "cannot be registered on central market",
)


def require_r2_env() -> dict[str, str]:
    """Credentials + patches object prefix (not the same as Pearl's R2_PREFIX)."""
    missing = [name for name in R2_ENV_VARS if not os.environ.get(name)]
    if R2_PATCHES_PREFIX_ENV not in os.environ:
        missing.append(R2_PATCHES_PREFIX_ENV)
    if missing:
        raise SystemExit(
            "--r2-sync requires environment variables: "
            + ", ".join(missing)
            + f". Use {R2_PATCHES_PREFIX_ENV}=YourFolder for R2 key prefix "
            "(empty = bucket root for this file only; independent of pearl_items R2_PREFIX)."
        )
    prefix = os.environ[R2_PATCHES_PREFIX_ENV].strip().strip("/")
    out = {name: os.environ[name] for name in R2_ENV_VARS}
    out[R2_PATCHES_PREFIX_ENV] = prefix
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
    if local_path.suffix.lower() == ".json":
        extra_args["ContentType"] = "application/json; charset=utf-8"
    if extra_args:
        client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args)
    else:
        client.upload_file(str(local_path), bucket, key)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,*/*"})
    return s


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


def compact_alnum(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def text_blob(name: str, lines: list[str]) -> str:
    return compact_alnum(name + " " + " ".join(lines))


def has_any_phrase(blob: str, phrases: tuple[str, ...]) -> bool:
    for p in phrases:
        if compact_alnum(p) in blob:
            return True
    return False


def stable_item_id(group_no: int, pack_name: str) -> str:
    h = hashlib.sha256(f"{group_no}:{pack_name.strip()}".encode("utf-8")).hexdigest()
    return h[:20]


def classify_product(
    group_no: int,
    pack_name: str,
    sales_period: str,
    content_lines: list[str],
    preceding_text: str = "",
) -> dict[str, Any]:
    blob = text_blob(preceding_text + " " + pack_name, content_lines)
    non_cm = has_any_phrase(blob, NON_CM_PHRASES)
    has_outfit = has_any_phrase(blob, OUTFIT_BOX_PHRASES)
    cron_heavy = has_any_phrase(blob, CRON_HEAVY_PHRASES)
    gamble = has_any_phrase(blob, GAMBLE_BOX_PHRASES) or has_any_phrase(
        blob, PEARL_BOX_PHRASES
    )
    pearl_box = has_any_phrase(blob, PEARL_BOX_PHRASES)

    # Single outfit / cosmetic without CM registration (footers mention non-CM)
    if non_cm and "outfit" in blob and not has_outfit and not pearl_box and not cron_heavy:
        category = "outfit"
        impact = "none"
        market_tags = ["no_cm_listing"]
        reason = "Non-tradeable outfit; no Central Market supply."
    elif pearl_box:
        category = "pearl_box"
        impact = "high"
        market_tags = ["pearl_liquidity", "indirect_outfit_demand"]
        reason = "Pearl purchase bundle; strong effect on pearl economy / outfit buys."
    elif has_outfit and cron_heavy:
        category = "gamble_box"
        impact = "high"
        market_tags = ["cron_pressure", "pearl_outfit_supply"]
        reason = "Contains premium outfit choice plus Cron-heavy enhancement boxes."
    elif has_outfit and gamble:
        category = "gamble_box"
        impact = "medium"
        market_tags = ["pearl_outfit_supply"]
        reason = "Gamble-style pack with sellable outfit selection."
    elif has_outfit:
        category = "gamble_box"
        impact = "medium"
        market_tags = ["pearl_outfit_supply"]
        reason = "Contains premium outfit selection; watch for stacked discounts/coupons."
    elif gamble:
        category = "gamble_box"
        impact = "low"
        market_tags = ["indirect_market"]
        reason = "RNG / value box; limited direct outfit supply."
    elif "coupon" in pack_name.lower() or "coupon" in blob:
        category = "coupon"
        impact = "low"
        market_tags = ["discounts"]
        reason = "Coupon-oriented; indirect pearl spend effects."
    else:
        category = "other"
        impact = "low"
        market_tags = []
        reason = "General Pearl Shop listing."

    flags = {
        "nonTradeableOutfit": non_cm,
        "hasOutfitBox": has_outfit,
        "hasCronHeavy": cron_heavy,
        "hasPearlBoxWording": pearl_box,
    }

    return {
        "stableId": stable_item_id(group_no, pack_name),
        "rawName": pack_name.strip(),
        "salesPeriodText": sales_period.strip(),
        "contentLines": content_lines,
        "category": category,
        "impact": impact,
        "marketTags": market_tags,
        "reason": reason,
        "flags": flags,
    }


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


def _preceding_disclaimer_text(title_block) -> str:
    """Only footnotes directly above a pack (e.g. non-CM outfit). Stops at <ul> so we
    do not pull wiki link lists that belong to the previous product block."""
    parts: list[str] = []
    prev = title_block.previous_sibling
    hops = 0
    while prev is not None and hops < 25:
        hops += 1
        name = getattr(prev, "name", None)
        if name == "ul":
            break
        if name == "div":
            classes = " ".join(prev.get("class", []))
            if "tpl_shop_title" in classes or "tpl_title_bullet" in classes:
                break
            t = prev.get_text(" ", strip=True)
            if t and len(t) < 4500:
                parts.insert(0, t)
        prev = prev.previous_sibling
    raw = " ".join(parts)
    if "cannot be registered" not in raw.lower():
        return ""
    return raw


def parse_detail(html: str, group_no: int, source_url: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one(".title_area strong.title")
    date_el = soup.select_one(".title_area span.date")
    title = title_el.get_text(" ", strip=True) if title_el else ""
    published = date_el.get_text(" ", strip=True) if date_el else ""

    area = soup.select_one(".contents_area.editor_area")
    if not area:
        return {
            "groupContentNo": group_no,
            "title": title,
            "publishedAt": published,
            "sourceUrl": source_url,
            "items": [],
            "parserVersion": PARSER_VERSION,
        }

    items: list[dict[str, Any]] = []
    for title_block in area.select("div.tpl_shop_title"):
        name_el = title_block.select_one("p.title")
        desc_el = title_block.select_one("p.desc")
        if not name_el:
            continue
        pack_name = name_el.get_text(" ", strip=True)
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

        pre = _preceding_disclaimer_text(title_block)
        items.append(
            classify_product(group_no, pack_name, period, lines, preceding_text=pre)
        )

    return {
        "groupContentNo": group_no,
        "title": title,
        "publishedAt": published,
        "sourceUrl": source_url,
        "items": items,
        "parserVersion": PARSER_VERSION,
    }


def listing_urls(session: requests.Session, limit: int) -> list[tuple[int, str]]:
    url = f"{BASE}{LIST_PATH}"
    params = {"boardType": "5", "countryType": "en-US"}
    html = get_text(session, url, params=params)
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.select_one("ul.thumb_nail_list")
    if not ul:
        raise RuntimeError("Could not find thumb_nail_list on Pearl Shop board page.")

    seen: set[int] = set()
    out: list[tuple[int, str]] = []
    for a in ul.select('a[href*="groupContentNo="]'):
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
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Ingest NA Pearl Shop HTML posts into patches_normalized.json."
    )
    ap.add_argument("--data-dir", default="data", help="Output directory (default: data)")
    ap.add_argument(
        "--max-posts",
        type=int,
        default=40,
        help="Max posts to fetch from board listing (default: 40)",
    )
    ap.add_argument(
        "--r2-sync",
        action="store_true",
        help=(
            "Download existing JSON from R2 before run, upload after. "
            "Requires R2_* credentials plus R2_PATCHES_PREFIX (not R2_PREFIX)."
        ),
    )
    ap.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-fetch every post in the listing window (ignore incremental skip/early exit).",
    )
    ap.add_argument(
        "--incremental-stop-after",
        type=int,
        default=5,
        metavar="N",
        help=(
            "After N consecutive board rows already in JSON, stop listing processing "
            "(default: 5). Set to 0 to disable early exit but still skip known IDs."
        ),
    )
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / JSON_FILENAME

    now = datetime.now(timezone.utc).replace(microsecond=0)
    generated = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    r2_client = None
    r2_env: dict[str, str] = {}
    if args.r2_sync:
        r2_env = require_r2_env()
        r2_client = make_r2_client(r2_env)
        pfx = r2_env[R2_PATCHES_PREFIX_ENV]
        print(
            f"  r2 patches prefix ({R2_PATCHES_PREFIX_ENV}): "
            f"{pfx or '(bucket root — patches JSON only)'}"
        )
        key = r2_object_key(pfx, JSON_FILENAME)
        print(f"R2: downloading {key if key else JSON_FILENAME} ...")
        r2_download_if_exists(
            r2_client, r2_env["R2_BUCKET"], key, out_path
        )

    existing_doc: dict[str, Any] = {"patches": [], "region": "na"}
    if out_path.exists():
        try:
            existing_doc = json.loads(out_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"  warn: corrupt existing JSON ({e}); starting fresh", file=sys.stderr)
            existing_doc = {"patches": [], "region": "na"}

    by_id: dict[int, dict[str, Any]] = {}
    for p in existing_doc.get("patches", []):
        try:
            gid = int(p["groupContentNo"])
            by_id[gid] = p
        except (KeyError, TypeError, ValueError):
            continue

    session = make_session()
    incremental = not args.full_refresh
    stop_after = max(0, args.incremental_stop_after)
    print(f"Listing Pearl Shop board (max {args.max_posts}) ...")
    if incremental:
        print(
            (
                f"  incremental: on (skip known ids; early exit after {stop_after} consecutive known)"
                if stop_after
                else "  incremental: on (skip known ids; early exit off)"
            )
        )
    else:
        print("  incremental: off (--full-refresh)")

    entries = listing_urls(session, args.max_posts)
    print(f"  found {len(entries)} posts")

    fetch_count = 0
    skip_count = 0
    early_exit = False
    consecutive_known = 0

    for gid, detail_url in entries:
        if incremental and gid in by_id:
            skip_count += 1
            consecutive_known += 1
            print(f"  [{gid}] skip (already in store)")
            if stop_after and consecutive_known >= stop_after:
                early_exit = True
                print(
                    f"  early exit: {stop_after} consecutive known rows "
                    f"({skip_count} skipped this run so far)"
                )
                break
            continue

        consecutive_known = 0
        try:
            print(f"  [{gid}] fetch ...")
            html = get_text(session, detail_url, quiet=True)
            parsed = parse_detail(html, gid, detail_url)
            by_id[gid] = parsed
            fetch_count += 1
            time.sleep(0.35)
        except Exception as e:
            print(f"  [{gid}] ERROR: {e}", file=sys.stderr)

    print(
        f"  summary: detail fetches={fetch_count}, skipped={skip_count}"
        + (", early_exit=1" if early_exit else ", early_exit=0")
    )

    patches = sorted(
        by_id.values(),
        key=lambda p: parse_published_sort_key(str(p.get("publishedAt") or ""))[0],
        reverse=True,
    )

    doc = {
        "parserVersion": PARSER_VERSION,
        "region": "na",
        "generatedAtUtc": generated,
        "patches": patches,
    }
    out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} ({len(patches)} patches)")

    if r2_client is not None:
        key = r2_object_key(r2_env[R2_PATCHES_PREFIX_ENV], JSON_FILENAME)
        print(f"R2: uploading {key} ...")
        r2_upload(r2_client, r2_env["R2_BUCKET"], out_path, key)
        print("Done (R2 upload).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
