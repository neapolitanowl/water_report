#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import time
import sqlite3
import argparse
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import pdfplumber

PDF_BASE = "https://water-quality-api.prod.p.webapp.thameswater.co.uk/water-quality-api/Zone/"
TW_REFERER = "https://www.thameswater.co.uk/help/water-and-waste-help/water-quality/check-your-water-quality"
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# --- classification sets (normalize names for matching) ---
def norm(s: str) -> str:
    s = s.lower().strip()
    # drop " as XXX", units in parens, and extra spaces/punctuation
    s = re.sub(r"\s+as\s+[a-z0-9\(\)]+", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z0-9\+\-\. ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

HEAVY_METALS = {
    "aluminium", "antimony", "arsenic", "cadmium", "chromium",
    "copper", "iron", "lead", "manganese", "mercury", "nickel", "selenium"
}

PESTICIDES = {
    "atrazine", "bentazone", "bromoxynil", "carbendazim", "carbetamide",
    "chlortoluron", "clopyralid", "dicamba", "dichlorprop", "diuron",
    "flufenacet", "fluroxypyr", "isoproturon", "linuron", "mcpa",
    "mecoprop", "metaldehyde", "metazachlor", "monuron", "pentachlorophenol",
    "picloram", "propyzamide", "quinmerac", "simazine", "triclopyr",
}

# everything else that isn't a heavy metal or pesticide, we treat as "chemicals";
# add common synonyms to normalize
CHEM_SYNONYMS = {
    "12 dichloroethane": "1,2-dichloroethane",
    "chlorine residual": "chlorine",
    "nitrate nitrite calculation": "nitrate/nitrite calculation",
    "total organic carbon as c": "total organic carbon",
    "tetra  trichloroethene calc": "tetra- & trichloroethene calc",
    "hydrogen ion": "ph",
}

# --- DB schema ---
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS zones (
  zone_code TEXT PRIMARY KEY,
  zone_title TEXT,
  population INTEGER,
  period_start TEXT,
  period_end TEXT,
  pdf_path TEXT
);
CREATE TABLE IF NOT EXISTS postcodes (
  postcode TEXT PRIMARY KEY,
  zone_code TEXT,
  FOREIGN KEY(zone_code) REFERENCES zones(zone_code)
);
CREATE TABLE IF NOT EXISTS measurements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  zone_code TEXT,
  parameter TEXT,
  parameter_norm TEXT,
  category TEXT, -- 'heavy_metal' | 'chemical' | 'pesticide'
  units TEXT,
  regulatory_limit TEXT,
  min_val TEXT,
  mean_val TEXT,
  max_val TEXT,
  samples_total INTEGER,
  samples_contrav INTEGER,
  pct_contrav TEXT,
  FOREIGN KEY(zone_code) REFERENCES zones(zone_code)
);
CREATE INDEX IF NOT EXISTS idx_meas_zone ON measurements(zone_code);
"""

def ensure_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.executescript(SCHEMA)
    return conn

def parse_float(val: str) -> Optional[float]:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("", "n/a", "na", "-"):
        return None
    v = v.replace("µ", "")  # µg/l -> g/l unit symbol ignored here
    v = v.replace(",", "")  # strip thousands
    if v.startswith("<"):   # treat "<x" as zero (below detect)
        return 0.0
    try:
        return float(re.findall(r"-?\d+(?:\.\d+)?", v)[0])
    except Exception:
        return None

def classify_parameter(pname: str) -> str:
    n = norm(pname)
    if n in CHEM_SYNONYMS: n = CHEM_SYNONYMS[n]
    base = n
    if base in HEAVY_METALS:
        return "heavy_metal"
    if base in PESTICIDES:
        return "pesticide"
    return "chemical"

HEADER_KEYS = ("parameter", "units", "regulatory limit", "min", "mean", "max", "total", "contravening", "contravening")

def extract_zone_meta(text: str) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
    """
    From header text like:
    'Water Supply Zone: 0062 BROMLEY TOWN Population: 65,484
     Time Period: 1 Jan 2024 to 31 Dec 2024'
    """
    title = None; pop = None; pstart = None; pend = None
    m = re.search(r"Water Supply Zone:\s*([^\n\r]+?)\s+Population:\s*([\d,]+)", text, re.I)
    if m:
        title = m.group(1).strip()
        pop = int(m.group(2).replace(",", ""))
    m2 = re.search(r"Time Period:\s*([0-9A-Za-z\s]+?)\s+to\s+([0-9A-Za-z\s]+)", text, re.I)
    if m2:
        pstart = m2.group(1).strip()
        pend = m2.group(2).strip()
    return title, pop, pstart, pend

def rows_from_pdf(pdf_path: Path) -> Tuple[List[Dict], Dict]:
    """
    Returns (rows, meta). rows contain parsed table records.
    meta: {'zone_title','population','period_start','period_end'}
    """
    rows: List[Dict] = []
    meta = {"zone_title": None, "population": None, "period_start": None, "period_end": None}

    with pdfplumber.open(str(pdf_path)) as pdf:
        all_text = []
        for page in pdf.pages:
            try:
                all_text.append(page.extract_text() or "")
            except Exception:
                pass
        big_text = "\n".join(all_text)

        # zone meta
        zt, pop, pstart, pend = extract_zone_meta(big_text)
        meta.update({"zone_title": zt, "population": pop, "period_start": pstart, "period_end": pend})

        # try tables first
        for page in pdf.pages:
            try:
                tbls = page.extract_tables() or []
            except Exception:
                tbls = []
            for t in tbls:
                # find header row index
                hdr_idx = None
                for i, row in enumerate(t):
                    line = " ".join([c or "" for c in row]).lower()
                    if "parameter" in line and "units" in line and "min" in line and "mean" in line:
                        hdr_idx = i
                        break
                if hdr_idx is None:
                    continue

                headers = [ (c or "").strip().lower() for c in t[hdr_idx] ]
                # normalize header positions by fuzzy matching
                def pick(cols, key):
                    for idx, h in enumerate(cols):
                        if key in h:
                            return idx
                    return None
                idx_param = pick(headers, "parameter")
                idx_units = pick(headers, "unit")
                idx_rl    = pick(headers, "regulatory")
                idx_min   = pick(headers, "min")
                idx_mean  = pick(headers, "mean")
                idx_max   = pick(headers, "max")
                idx_total = pick(headers, "total")
                idx_cont  = pick(headers, "contraven")
                # data rows follow until blank
                for r in t[hdr_idx+1:]:
                    if not any(r):  # blank row
                        continue
                    def cell(i): return (r[i].strip() if (i is not None and i < len(r) and r[i]) else "")
                    pname = cell(idx_param)
                    if not pname:
                        continue
                    rows.append({
                        "parameter": pname,
                        "units": cell(idx_units),
                        "regulatory_limit": cell(idx_rl),
                        "min": cell(idx_min),
                        "mean": cell(idx_mean),
                        "max": cell(idx_max),
                        "total": cell(idx_total),
                        "contravening": cell(idx_cont),
                        "pct_contrav": "",  # not always present in table
                    })

        # if table extraction failed, fallback to text parsing
        if not rows:
            # heuristic: lines after a header containing "Parameter Units Regulatory"
            lines = [ln.strip() for ln in big_text.splitlines()]
            try:
                start = next(i for i,ln in enumerate(lines) if re.search(r"parameter\s+units\s+regulatory", ln, re.I))
            except StopIteration:
                start = None
            if start is not None:
                for ln in lines[start+1:]:
                    if not ln or ln.lower().startswith("water supply zone"):
                        break
                    # split by 2+ spaces
                    parts = re.split(r"\s{2,}", ln)
                    if len(parts) >= 6:
                        pname = parts[0].strip()
                        units = parts[1].strip() if len(parts) > 1 else ""
                        rl    = parts[2].strip() if len(parts) > 2 else ""
                        mn    = parts[3].strip() if len(parts) > 3 else ""
                        me    = parts[4].strip() if len(parts) > 4 else ""
                        mx    = parts[5].strip() if len(parts) > 5 else ""
                        tot   = parts[6].strip() if len(parts) > 6 else ""
                        con   = parts[7].strip() if len(parts) > 7 else ""
                        pct   = parts[8].strip() if len(parts) > 8 else ""
                        rows.append({
                            "parameter": pname,
                            "units": units,
                            "regulatory_limit": rl,
                            "min": mn, "mean": me, "max": mx,
                            "total": tot, "contravening": con, "pct_contrav": pct
                        })

    return rows, meta

def hardness_label_from_rows(rows: List[Dict]) -> Optional[str]:
    # look for "Hardness (Total) as CaCO3"
    for r in rows:
        if "hardness" in r["parameter"].lower():
            mean = parse_float(r.get("mean"))
            if mean is None:
                continue
            if mean <= 100: return "Soft"
            if mean <= 200: return "Moderately hard"
            return "Hard"
    return None

# ------------- Hardened PDF downloader with browser fallback -----------------

def _zone_variants(zone: str):
    """Try safe variants like SLE02 / SLE002 if SLE2 fails."""
    zone = zone.strip().upper()
    m = re.match(r"^([A-Z]+)(\d+)$", zone)
    if not m:
        return [zone]
    letters, digits = m.group(1), m.group(2).lstrip("0") or "0"
    variants = [f"{letters}{digits}"]
    if len(digits) == 1:
        variants.append(f"{letters}0{digits}")
    if len(digits) <= 2:
        variants.append(f"{letters}{digits.zfill(3)}")
    return list(dict.fromkeys(variants))  # dedupe preserve order

def _requests_try(session: requests.Session, url: str, debug=False):
    hdrs = {
        "User-Agent": BROWSER_UA,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Referer": TW_REFERER,
        "Origin": "https://www.thameswater.co.uk",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = session.get(url, headers=hdrs, timeout=45, allow_redirects=True)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "3"))
        if debug: print(f"[pdf] 429 backoff {wait}s")
        time.sleep(wait)
        r = session.get(url, headers=hdrs, timeout=45, allow_redirects=True)
    return r

async def _browser_fetch(zone: str, out_path: Path, headed=False, debug=False):
    """
    Fetch via real browser to satisfy WAF (origin/referer/cookies). Requires playwright.
    """
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        if debug: print(f"[pdf] browser fallback unavailable: {e}")
        return False

    url = PDF_BASE + zone
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not headed)
        ctx = await browser.new_context(
            user_agent=BROWSER_UA,
            accept_downloads=True,
            locale="en-GB",
        )
        page = await ctx.new_page()
        try:
            # warm up cookies/referrer by visiting the TW page once
            await page.goto(TW_REFERER, wait_until="domcontentloaded", timeout=30000)
            # request the PDF via APIRequest
            resp = await page.request.get(url)
            if resp.ok:
                body = await resp.body()
                out_path.write_bytes(body)
                if debug: print(f"[pdf] browser-fetched {zone} ({len(body)} bytes)")
                return True
            # fallback to download event if server forces attachment
            try:
                download_fut = page.wait_for_event("download", timeout=8000)
                await page.goto(url, wait_until="commit", timeout=20000)
                dl = await download_fut
                await dl.save_as(str(out_path))
                if debug: print(f"[pdf] browser-downloaded {zone} via download event")
                return True
            except Exception:
                pass
            if debug:
                txt = await resp.text()
                print(f"[pdf] browser fetch failed: {resp.status} {txt[:120]!r}")
            return False
        finally:
            await ctx.close()
            await browser.close()

def download_pdf(zone: str, out_dir: Path, session: requests.Session, debug=False, headed=False) -> Optional[Path]:
    """
    Try requests with browser-like headers; on 401/403/406 fallback to Playwright.
    Also tries zero-padded zone variants (e.g. SLE02).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for candidate in _zone_variants(zone):
        out_path = out_dir / f"{candidate}.pdf"
        if out_path.exists() and out_path.stat().st_size > 1000:
            if debug: print(f"[pdf] exists {out_path.name}")
            return out_path

        url = PDF_BASE + candidate
        if debug: print(f"[pdf] GET {url}")
        try:
            r = _requests_try(session, url, debug=debug)
            ct = (r.headers.get("content-type") or "").lower()
            if r.ok and ("pdf" in ct or r.content[:4] == b"%PDF"):
                out_path.write_bytes(r.content)
                if debug: print(f"[pdf] saved {out_path.name} ({len(r.content)} bytes)")
                return out_path
            elif r.status_code in (401, 403, 406):
                if debug: print(f"[pdf] {r.status_code} → switching to browser fallback for {candidate}")
                ok = asyncio.run(_browser_fetch(candidate, out_path, headed=headed, debug=debug))
                if ok:
                    return out_path
            elif r.status_code == 404:
                if debug: print(f"[pdf] 404 for {candidate}, trying next variant…")
                continue
            else:
                if debug: print(f"[pdf] unexpected {r.status_code} ct={ct}")
        except requests.RequestException as e:
            if debug: print(f"[pdf] error for {candidate}: {e}")
        time.sleep(0.6)  # polite pause between variants

    if debug: print(f"[pdf] failed for zone {zone} (all variants tried)")
    return None

# ---------------------------- DB upserts -------------------------------------

def upsert_zone(conn, zone_code: str, meta: Dict, pdf_path: Path):
    conn.execute("""
        INSERT INTO zones(zone_code, zone_title, population, period_start, period_end, pdf_path)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(zone_code) DO UPDATE SET
          zone_title=excluded.zone_title,
          population=excluded.population,
          period_start=excluded.period_start,
          period_end=excluded.period_end,
          pdf_path=excluded.pdf_path
    """, (zone_code, meta.get("zone_title"), meta.get("population"),
          meta.get("period_start"), meta.get("period_end"), str(pdf_path)))

def upsert_postcode(conn, postcode: str, zone_code: str):
    conn.execute("""
        INSERT INTO postcodes(postcode, zone_code)
        VALUES (?,?)
        ON CONFLICT(postcode) DO UPDATE SET zone_code=excluded.zone_code
    """, (postcode.upper().strip(), zone_code))

def insert_measurements(conn, zone_code: str, rows: List[Dict]):
    conn.execute("DELETE FROM measurements WHERE zone_code = ?", (zone_code,))
    for r in rows:
        pnorm = norm(r["parameter"])
        if pnorm in CHEM_SYNONYMS: pnorm = CHEM_SYNONYMS[pnorm]
        cat = classify_parameter(r["parameter"])
        conn.execute("""
            INSERT INTO measurements(zone_code, parameter, parameter_norm, category, units,
              regulatory_limit, min_val, mean_val, max_val, samples_total, samples_contrav, pct_contrav)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (zone_code, r["parameter"], pnorm, cat, r.get("units",""),
              r.get("regulatory_limit",""), r.get("min",""), r.get("mean",""), r.get("max",""),
              int(re.findall(r"\d+", r.get("total","0"))[0]) if re.findall(r"\d+", r.get("total","0")) else None,
              int(re.findall(r"\d+", r.get("contravening","0"))[0]) if re.findall(r"\d+", r.get("contravening","0")) else None,
              r.get("pct_contrav","")))

# ---------------------------- CSV reader -------------------------------------

def read_csv(input_csv: Path) -> List[Tuple[str, Optional[str]]]:
    """
    Robust reader for 'POSTCODE, AREA CODE' (or similar).
    - Strips BOMs and header whitespace
    - Finds columns by meaning (e.g., 'postcode', 'area code' or 'zone')
    - Falls back to first two columns
    """
    out: List[Tuple[str, Optional[str]]] = []
    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        try:
            headers = next(r)
        except StopIteration:
            return out

        # normalise headers
        normh = [h.strip().lower() for h in headers]
        idx_pc = None
        idx_area = None

        for i, h in enumerate(normh):
            if ("post" in h and "code" in h) or h == "postcode":
                idx_pc = i
            if (("area" in h and "code" in h) or "zone" in h or h == "area code"):
                idx_area = i

        if idx_pc is None:
            idx_pc = 0
        if idx_area is None and len(headers) >= 2:
            idx_area = 1

        for row in r:
            if not row:
                continue
            need = max(idx_pc, idx_area if idx_area is not None else 0) + 1
            if len(row) < need:
                row = row + [""] * (need - len(row))

            pc = (row[idx_pc] or "").strip()
            ac = (row[idx_area] or "").strip() if idx_area is not None else ""

            if pc:
                out.append((pc, ac if ac else None))
    return out

# ------------------------------- main ----------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Ingest Thames Water PDFs into SQLite by postcode")
    ap.add_argument("-i", "--input", required=True, help="CSV with columns POSTCODE, AREA CODE")
    ap.add_argument("-o", "--outdir", default="water_reports", help="Folder for downloaded PDFs")
    ap.add_argument("-d", "--db", default="water_reports.db", help="SQLite DB path")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--sleep-ms", type=int, default=200, help="polite pause between downloads")
    ap.add_argument("--headed-fallback", action="store_true",
                    help="Use a headed Chromium window for the browser fallback")
    args = ap.parse_args()

    input_csv = Path(args.input)
    out_dir   = Path(args.outdir)
    db_path   = Path(args.db)

    conn = ensure_db(db_path)
    sess = requests.Session()

    rows = read_csv(input_csv)
    seen_zones = set()

    for idx, (pc, zone) in enumerate(rows, 1):
        pc_disp = pc or "(missing)"
        if not zone:
            if args.debug: print(f"[{idx}] {pc_disp}: no area code, skip")
            continue

        if args.debug: print(f"[{idx}] {pc} -> {zone}")

        pdf_path = download_pdf(zone, out_dir, sess, debug=args.debug, headed=args.headed_fallback)
        if not pdf_path:
            continue

        if zone in seen_zones:
            upsert_postcode(conn, pc, zone)
            conn.commit()
            continue

        # parse and insert for this (new) zone
        try:
            table_rows, meta = rows_from_pdf(pdf_path)
            upsert_zone(conn, zone, meta, pdf_path)
            upsert_postcode(conn, pc, zone)
            insert_measurements(conn, zone, table_rows)
            conn.commit()
            seen_zones.add(zone)

            if args.debug:
                hard = hardness_label_from_rows(table_rows)
                print(f"   zone: {meta.get('zone_title')} pop={meta.get('population')} "
                      f"period={meta.get('period_start')}→{meta.get('period_end')} hardness={hard}")
                print(f"   inserted {len(table_rows)} measurement rows for {zone}")

        except Exception as e:
            if args.debug: print(f"[parse-error] {zone}: {e}")

        time.sleep(max(0, args.sleep_ms) / 1000.0)

    conn.close()
    if args.debug: print("[done]")

if __name__ == "__main__":
    main()