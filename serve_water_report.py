#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sqlite3
from typing import List, Tuple, Set
from flask import Flask, request, render_template_string, jsonify, url_for

DB_PATH = "water_reports.db"

BRAND_NAME = "Keepnetics"
BRAND_PRIMARY = "#009ecf"

# --- Solid dark-blue theme + Mont DEMO fonts (OTF from /static/fonts) ---
BRAND_CSS = f"""
@font-face {{
  font-family: 'MontDemo';
  src: url('/static/fonts/Mont-ExtraLightDEMO.otf') format('opentype');
  font-weight: 200; font-style: normal; font-display: swap;
}}
@font-face {{
  font-family: 'MontDemo';
  src: url('/static/fonts/Mont-HeavyDEMO.otf') format('opentype');
  font-weight: 800; font-style: normal; font-display: swap;
}}

:root{{
  --bg-solid: #072d44;
  --panel: rgba(255,255,255,0.06);
  --panel-border: rgba(255,255,255,0.12);
  --ink: #ecf7ff;
  --ink-weak: #cde6f3;
  --muted: #a6c8d8;
  --line: rgba(255,255,255,0.15);
  --accent: {BRAND_PRIMARY};

  --badge-metal-bg: rgba(255,255,255,0.08);
  --badge-metal-fg: #eaf6fb;
  --badge-chem-bg: rgba(0,158,207,0.18);
  --badge-chem-fg: #c9f3ff;
  --badge-pest-bg: rgba(0,207,136,0.18);
  --badge-pest-fg: #ccffea;

  --danger-bg: rgba(220,38,38,0.18);
  --danger-fg: #ffdcdc;
  --ok-bg: rgba(16,185,129,0.18);
  --ok-fg: #d9ffe9;
}}

*{{box-sizing:border-box}}
html,body{{
  margin:0; padding:0;
  background: var(--bg-solid);
  color: var(--ink);
  font-family: 'MontDemo', 'Montserrat', system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
  font-weight: 200;  /* defaults to ExtraLight; headings/buttons override to 800 */
}}
a{{color:inherit;text-decoration:none}}

.container{{max-width:1120px;margin:0 auto;padding:20px}}
.header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;gap:16px}}

.brand{{display:flex;align-items:center;gap:12px}}
.brand .logo{{display:flex;align-items:center;justify-content:center;width:40px;height:46px}}
.brand .name{{font-weight:800;letter-spacing:.2px;font-size:18px;color:#e8fbff}}

.btn{{
  padding:10px 14px;border-radius:12px;border:1px solid var(--panel-border);
  background: rgba(255,255,255,0.10); color:#e9fbff; font-weight:800; cursor:pointer
}}
.btn:hover{{background: rgba(255,255,255,0.16)}}
.btn.primary{{border-color: {BRAND_PRIMARY}; background:{BRAND_PRIMARY}; color:#00161c}}
.btn.primary:hover{{filter: brightness(1.05)}}

.sm{{font-size:13px;color:var(--muted)}}
.mono{{font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono','Courier New', monospace}}

.hero{{
  border:1px solid var(--panel-border);
  border-radius:18px;
  background: var(--panel);
}}
.hero-inner{{padding:22px}}

.headline{{text-align:center;margin:6px 0 10px}}
.headline h1{{font-size:32px;line-height:1.2;margin:0;font-weight:800;letter-spacing:.2px}}
.headline h1 strong{{color:#e8fbff}}
.subline{{text-align:center;font-size:13px;color:var(--ink-weak);margin-bottom:10px}}

.form{{display:flex;gap:10px;justify-content:center;margin-top:8px;flex-wrap:wrap}}
.input{{
  flex:1;min-width:260px;max-width:520px;padding:12px 14px;
  border:1px solid var(--panel-border);border-radius:12px;font-size:16px;
  background: rgba(0,0,0,0.25); color: var(--ink); font-weight:200;
}}
.input::placeholder{{color:#b7dbe7}}

.grid{{display:grid;gap:12px;margin:12px 0}}
.grid.cols-3{{grid-template-columns: repeat(3, 1fr)}}
.card{{
  background: var(--panel);
  border:1px solid var(--panel-border);
  border-radius:16px;padding:16px;
  box-shadow:0 8px 30px rgba(0,0,0,.20)
}}
.card h2{{font-size:16px;margin:0 0 6px;color:#eafcff;font-weight:800}}

.kv{{display:flex;gap:8px;flex-wrap:wrap}}
.pill{{
  display:inline-block;padding:6px 10px;
  border:1px solid var(--panel-border);
  border-radius:999px;background:rgba(255,255,255,0.04);color:#e7faff; font-weight:200
}}

.badge{{display:inline-block;padding:3px 9px;border-radius:999px;font-weight:800;font-size:12px;border:1px solid transparent}}
.badge.metal{{background:var(--badge-metal-bg);color:var(--badge-metal-fg);border-color:rgba(255,255,255,0.18)}}
.badge.chem{{background:var(--badge-chem-bg);color:var(--badge-chem-fg);border-color:rgba(0,158,207,0.35)}}
.badge.pest{{background:var(--badge-pest-bg);color:var(--badge-pest-fg);border-color:rgba(0,207,136,0.35)}}

.table-wrap{{border:1px solid var(--panel-border);border-radius:16px;overflow:hidden;background: var(--panel);margin-top:12px}}
.table{{width:100%;border-collapse:separate;border-spacing:0}}
.table thead th{{
  background:rgba(255,255,255,0.06);
  border-bottom:1px solid var(--panel-border);
  font-size:12px;letter-spacing:.04em;text-transform:uppercase;
  color:var(--ink-weak);padding:10px 12px;font-weight:800
}}
.table tbody td{{padding:10px 12px;border-bottom:1px solid var(--panel-border);font-size:14px;color:#e9fbff}}
.table tbody tr:nth-child(odd){{background:rgba(255,255,255,0.02)}}
.table tbody tr:nth-child(even){{background:rgba(255,255,255,0.03)}}

.flag{{
  font-size:12px;padding:3px 8px;border-radius:10px;border:1px solid var(--panel-border);
  background:rgba(255,255,255,0.06);color:#e9fbff;font-weight:800
}}
.flag.ok{{background:var(--ok-bg);color:var(--ok-fg);border-color:rgba(16,185,129,0.45)}}
.flag.bad{{background:var(--danger-bg);color:var(--danger-fg);border-color:rgba(220,38,38,0.45)}}

.row-actions{{display:flex;gap:10px;align-items:center;margin:10px 0;flex-wrap:wrap}}
.checkbox label, .row-actions label{{cursor:pointer}}

@media (max-width: 760px){{
  .container{{padding:14px}}
  .brand .name{{font-size:16px}}
  .headline h1{{font-size:24px}}
  .grid.cols-3{{grid-template-columns:1fr}}
  .form{{flex-direction:column}}
  .btn{{width:100%}}
}}
"""

KEEPNETICS_SVG = """
<svg width="40" height="46" viewBox="0 0 56 64" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <path fill-rule="evenodd" clip-rule="evenodd" d="M27.64 11L36.23 23.99L44.6 36.65L38.58 40.04L27.64 46.2L16.86 39.93L11 36.52L19.1 24.1L27.64 11Z" fill="white"/>
</svg>
"""

TPL = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>{{ brand_name }} · Water Report</title>
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <meta name="color-scheme" content="dark">
    <style>{{ css }}</style>
  </head>
  <body>
    <div class="container">
      <div class="header">
        <div class="brand">
          <span class="logo">{{ svg_logo|safe }}</span>
          <div class="name">{{ brand_name }}</div>
        </div>
        <a class="btn" href="/">New search</a>
      </div>

      {% if not report %}
        <div class="hero">
          <div class="hero-inner">
            <div class="headline">
              <h1>Check your water quality by postcode</h1>
              <div class="subline">Enter a full UK postcode from your database (e.g., <span class="mono">N19 5SJ</span>).</div>
            </div>
            <form method="get" action="/" class="form">
              <input class="input" type="text" placeholder="Enter full UK postcode" name="p" value="{{ q or '' }}" autofocus>
              <button class="btn primary" type="submit">Search</button>
            </form>
          </div>
        </div>
      {% else %}
        <div class="hero">
          <div class="hero-inner">
            <div class="subline">
              Postcode <strong class="mono">{{ report.postcode }}</strong> ·
              Zone <strong class="mono">{{ report.zone_code }}</strong>{% if report.zone_title %} · {{ report.zone_title }}{% endif %} ·
              Period: {{ report.period_start or '—' }} → {{ report.period_end or '—' }}{% if report.population %} · Population: {{ report.population }}{% endif %}
            </div>
            <div class="headline">
              <h1>Your tap water is classified as <strong>{{ report.hardness or '—' }}</strong>{% if report.total_found is not none %} and has <strong>{{ report.total_found }}</strong> detected parameters{% endif %}.</h1>
            </div>

            <div class="grid cols-3">
              <div class="card">
                <h2>{{ report.count_heavy }} Heavy Metals</h2>
                {% if report.list_heavy %}
                  <div class="kv">
                    {% for n in report.list_heavy %}<span class="pill">{{ n }}</span>{% endfor %}
                  </div>
                {% else %}<div class="sm">No heavy metals detected above reporting thresholds.</div>{% endif %}
              </div>
              <div class="card">
                <h2>{{ report.count_chem }} Chemicals</h2>
                {% if report.list_chem %}
                  <div class="kv">
                    {% for n in report.list_chem %}<span class="pill">{{ n }}</span>{% endfor %}
                  </div>
                {% else %}<div class="sm">No chemicals detected above reporting thresholds.</div>{% endif %}
              </div>
              <div class="card">
                <h2>{{ report.count_pest }} Pesticides/Herbicides</h2>
                {% if report.list_pest %}
                  <div class="kv">
                    {% for n in report.list_pest %}<span class="pill">{{ n }}</span>{% endfor %}
                  </div>
                {% else %}<div class="sm">No pesticides/herbicides detected above reporting thresholds.</div>{% endif %}
              </div>
            </div>
          </div>
        </div>

        <div class="row-actions">
          <form method="get" action="/" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
            <input type="hidden" name="p" value="{{ report.postcode }}">
            <label class="sm" style="display:flex;gap:8px;align-items:center">
              <input type="checkbox" name="only_detected" value="1" {% if only_detected %}checked{% endif %} onchange="this.form.submit()">
              Show only detected parameters
            </label>
            <a class="btn" href="{{ url_for('api', pc=report.postcode) }}">View JSON</a>
          </form>
        </div>

        <div class="table-wrap">
          <table class="table">
            <thead>
              <tr>
                <th>Parameter</th>
                <th>Category</th>
                <th>Units</th>
                <th>Reg. Limit</th>
                <th>Min</th>
                <th>Mean</th>
                <th>Max</th>
                <th>Samples</th>
                <th>Contrav.</th>
                <th>Detected</th>
              </tr>
            </thead>
            <tbody>
            {% for r in table %}
              <tr>
                <td class="mono">{{ r['parameter'] }}</td>
                <td>
                  {% if r['category'] == 'heavy_metal' %}
                    <span class="badge metal">Heavy metal</span>
                  {% elif r['category'] == 'pesticide' %}
                    <span class="badge pest">Pesticide/Herbicide</span>
                  {% else %}
                    <span class="badge chem">Chemical</span>
                  {% endif %}
                </td>
                <td>{{ r['units'] }}</td>
                <td>{{ r['regulatory_limit'] }}</td>
                <td>{{ r['min_val'] }}</td>
                <td>{{ r['mean_val'] }}</td>
                <td>{{ r['max_val'] }}</td>
                <td>{{ r['samples_total'] or '' }}</td>
                <td>
                  {% if r['samples_contrav'] and r['samples_contrav']|int > 0 %}
                    <span class="flag bad">{{ r['samples_contrav'] }}</span>
                  {% else %}
                    <span class="flag ok">{{ r['samples_contrav'] or 0 }}</span>
                  {% endif %}
                </td>
                <td>
                  {% if r['detected'] %}
                    <span class="flag ok">Yes</span>
                  {% else %}
                    <span class="flag">No</span>
                  {% endif %}
                </td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
        </div>
      {% endif %}
    </div>
  </body>
</html>
"""

# -------------------- App & data helpers --------------------

app = Flask(__name__, static_folder="static", static_url_path="/static")

def db():
    return sqlite3.connect(DB_PATH)

def fetch_zone_for_postcode(conn, postcode: str):
    cur = conn.execute("SELECT zone_code FROM postcodes WHERE UPPER(postcode)=UPPER(?)", (postcode.strip(),))
    row = cur.fetchone()
    return row[0] if row else None

def fetch_zone_meta(conn, zone_code: str):
    cur = conn.execute("SELECT zone_title, population, period_start, period_end FROM zones WHERE zone_code=?", (zone_code,))
    r = cur.fetchone()
    return {"zone_title": r[0], "population": r[1], "period_start": r[2], "period_end": r[3]} if r else {}

def fetch_table(conn, zone_code: str) -> List[dict]:
    cur = conn.execute("""
      SELECT parameter, parameter_norm, category, units, regulatory_limit, min_val, mean_val, max_val, samples_total, samples_contrav
      FROM measurements WHERE zone_code=? ORDER BY parameter COLLATE NOCASE
    """, (zone_code,))
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def parse_float(val: str):
    if val is None: return None
    s = str(val).lower().strip().replace(",", "")
    if s.startswith("<"): return 0.0
    m = re.findall(r"-?\d+(?:\.\d+)?", s)
    if not m: return None
    try: return float(m[0])
    except Exception: return None

def mark_detected(table: List[dict]) -> None:
    for r in table:
        mx = parse_float(r.get("max_val"))
        me = parse_float(r.get("mean_val"))
        r["detected"] = ((mx is not None and mx > 0) or (me is not None and me > 0))

def hardness_from_table(table: List[dict]) -> str:
    for r in table:
        if "hardness" in r["parameter"].lower():
            m = parse_float(r.get("mean_val"))
            if m is None: continue
            if m <= 100: return "Soft"
            if m <= 200: return "Moderately hard"
            return "Hard"
    return "—"

def summarize(table: List[dict]) -> Tuple[Set[str], Set[str], Set[str]]:
    heavy, chem, pest = set(), set(), set()
    for r in table:
        if not r.get("detected"):
            continue
        p = r["parameter"]
        cat = r.get("category", "")
        if cat == "heavy_metal":
            heavy.add(p)
        elif cat == "pesticide":
            pest.add(p)
        else:
            chem.add(p)
    return heavy, chem, pest

@app.route("/api/postcode/<pc>")
def api(pc):
    with db() as conn:
        zone = fetch_zone_for_postcode(conn, pc)
        if not zone:
            return jsonify({"postcode": pc, "found": False}), 404
        meta = fetch_zone_meta(conn, zone)
        table = fetch_table(conn, zone)
        mark_detected(table)
        heavy, chem, pest = summarize(table)
        return jsonify({
            "postcode": pc,
            "zone_code": zone,
            **meta,
            "hardness": hardness_from_table(table),
            "counts": {
                "heavy_metals": len(heavy),
                "chemicals": len(chem),
                "pesticides": len(pest),
                "total": len(heavy | chem | pest),
            },
            "lists": {
                "heavy_metals": sorted(heavy),
                "chemicals": sorted(chem),
                "pesticides": sorted(pest),
            },
            "table": table
        })

@app.route("/", methods=["GET"])
def home():
    q = request.args.get("p", "").strip()
    only_detected = request.args.get("only_detected") in ("1", "true", "on")
    base_ctx = dict(css=BRAND_CSS, brand_name=BRAND_NAME, svg_logo=KEEPNETICS_SVG)

    if not q:
        return render_template_string(TPL, **base_ctx, report=None, q="")

    with db() as conn:
        zone = fetch_zone_for_postcode(conn, q)
        if not zone:
            return render_template_string(TPL, **base_ctx, report=None, q=q)

        meta  = fetch_zone_meta(conn, zone)
        table = fetch_table(conn, zone)
        mark_detected(table)
        if only_detected:
            table = [r for r in table if r.get("detected")]

        heavy, chem, pest = summarize(table)
        report = {
            "postcode": q,
            "zone_code": zone,
            "zone_title": meta.get("zone_title"),
            "population": meta.get("population"),
            "period_start": meta.get("period_start"),
            "period_end": meta.get("period_end"),
            "hardness": hardness_from_table(table),
            "count_heavy": len(heavy),
            "count_chem": len(chem),
            "count_pest": len(pest),
            "total_found": len(heavy | chem | pest),
            "list_heavy": sorted(heavy),
            "list_chem": sorted(chem),
            "list_pest": sorted(pest),
        }
        return render_template_string(
            TPL,
            **base_ctx,
            report=report,
            table=table,
            q=q,
            only_detected=only_detected
        )

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)