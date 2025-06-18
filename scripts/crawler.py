#!/usr/bin/env python3
"""
crawler.py — Incremental SRU crawler for Dutch legislation (Basis-wettenbestand).

مثال تشغيل:
    python crawler.py \
        --sru_url https://zoekservice.overheid.nl/sru/Search \
        --cql_query "modified<=2025-02-13" \
        --sru_version 1.2 \
        --batch_size 100
"""

from __future__ import annotations
import argparse, os, json, time, random, sys
from pathlib import Path
from typing import List
from dotenv import load_dotenv          # type: ignore
import requests                         # type: ignore
from lxml import etree                  # type: ignore
from requests.exceptions import HTTPError, ConnectionError

# ─────────────────────── إعداد عام ──────────────────────── #
load_dotenv()                           # يقرأ متغيّرات .env إن وُجدت
ENC  = sys.getfilesystemencoding()      # لتجنّب UnicodeEncodeError على Windows
PROG = Path(__file__).with_suffix('').name
PROGRESS_FILE = Path(__file__).with_name("sru_progress.json")
MAX_RETRIES   = 5
BACKOFF_BASE  = 2        # ثوانٍ
REQUEST_TIMEOUT = 60     # ثوانٍ
DEFAULT_SLEEP  = 3       # مهلة أدبية بين الدُفعات

# ─────────────────── دوال مساعدة صغيرة ─────────────────── #

def _safe_print(msg: str):
    """طباعة بدون كسر الترميز في PowerShell."""
    print(msg.encode(ENC, "ignore").decode(ENC, "ignore"))

def load_progress() -> int:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8")).get("start", 1)
        except Exception:
            pass
    return 1

def save_progress(start: int):
    PROGRESS_FILE.write_text(json.dumps({"start": start}), encoding="utf-8")

def parse_records(xml_bytes: bytes) -> List[etree._Element]:
    root = etree.fromstring(xml_bytes)
    # يعمل لكل من SRU 1.2 و SRU 2.0
    return root.findall(".//{*}recordData")

# ───────────────────── طلب السجلات مع إعادة محاولة ───────────────────── #

def fetch_batch(url: str,
                params: dict,
                retries: int = MAX_RETRIES) -> bytes:
    """
    يرجع المحتوى XML لسجلات SRU، مع إعادة محاولة تلقائية عند فشل الشبكة.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            _safe_print(f" URL (start={params['startRecord']}): {resp.url}")
            resp.raise_for_status()
            return resp.content

        except (HTTPError, ConnectionError) as err:
            if attempt == retries:
                raise                         # بعد آخر محاولة… ارفع الخطأ
            wait = BACKOFF_BASE * 2 ** (attempt - 1) + random.uniform(0, 1)
            _safe_print(f"{err} — retry {attempt}/{retries} after {wait:.1f}s")
            time.sleep(wait)

# ─────────────────────────ــ الدالة الرئيسة crawl ────────────────────────── #

def crawl(url: str,
          cql: str,
          out_dir: Path,
          batch: int,
          version: str,
          connection: str,
          polite_sleep: int = DEFAULT_SLEEP):
    out_dir.mkdir(parents=True, exist_ok=True)
    start = load_progress()

    while True:
        params = {
            "version"       : version,
            "operation"     : "searchRetrieve",
            "x-connection"  : connection,
            "query"         : cql,
            "startRecord"   : start,
            "maximumRecords": batch,
        }

        xml_bytes = fetch_batch(url, params)
        records   = parse_records(xml_bytes)
        if not records:
            _safe_print(" لا توجد سجلات جديدة — انتهى الزحف.")
            break

        for rec in records:
            ident = rec.find(".//{*}identifier")
            ident_text = (ident.text.strip() if ident is not None and ident.text else
                          f"{int(time.time()*1000)}_{start}")
            outfile = out_dir / f"{ident_text}.xml"
            outfile.write_bytes(
                etree.tostring(rec, encoding="utf-8", pretty_print=True))
        _safe_print(f" حُفِظت دفعة: {len(records)} ملف (حتى start={start})")

        start += len(records)
        save_progress(start)
        time.sleep(polite_sleep)

# ───────────────────────────── CLI ───────────────────────────── #

def main():
    ap = argparse.ArgumentParser(
        prog=PROG,
        description="Incremental SRU crawler for Basis-wettenbestand")
    ap.add_argument("--sru_url",      default=os.getenv("SRU_URL", "https://zoekservice.overheid.nl/sru/Search"))
    ap.add_argument("--cql_query",    default=os.getenv("CQL_QUERY", "modified<=2025-02-13"))
    ap.add_argument("--sru_version",  default="1.2", choices=["1.2", "2.0"])
    ap.add_argument("--connection",   default="BWB")
    ap.add_argument("--out_dir",      default="../data")
    ap.add_argument("--batch_size",   type=int, default=100)
    ap.add_argument("--sleep",        type=int, default=DEFAULT_SLEEP,
                    help="Seconds to sleep between batches (politeness)")
    args = ap.parse_args()

    try:
        crawl(
            url         = args.sru_url,
            cql         = args.cql_query,
            out_dir     = Path(args.out_dir),
            batch       = args.batch_size,
            version     = args.sru_version,
            connection  = args.connection,
            polite_sleep= args.sleep,
        )
    except Exception as ex:
        _safe_print(f" فشل الزحف نهائيًا: {ex}")

if __name__ == "__main__":
    main()
