#!/usr/bin/env python3
"""
Render a Grontia property report (HTML, English) from:
  - the evaluation JSON written by `python -m evaluation`
  - a narrative JSON written by Claude (headline, the "why" bullets, notes)

    python render_report.py evaluation.json narrative.json out.html

Everything numeric (rent band, growth scenarios, comparisons, links) is derived
here so every report is computed the same way. See ../reference/methodology.md.
"""
from __future__ import annotations

import html
import json
import statistics
import sys
from urllib.parse import quote

# Gross yield band used to turn a sale value into a market-rent estimate when no
# real rent comparable is given (annual rent / value). See methodology.md.
YIELD_LOW, YIELD_BASE, YIELD_HIGH = 0.035, 0.040, 0.045
PROJECTION_YEARS = 5

e = html.escape


def eur(v, digits=0) -> str:
    if v is None:
        return "–"
    return f"€{v:,.{digits}f}"


def pct(v, digits=1, sign=True) -> str:
    if v is None:
        return "–"
    return f"{v:+.{digits}f}%" if sign else f"{v:.{digits}f}%"


# ---------------------------------------------------------------------------
# Derived numbers
# ---------------------------------------------------------------------------

def rent_estimate(ev: dict, nar: dict) -> dict:
    sale = ev["purchase"]["range"]["central"]
    m2 = ev["dwelling"]["floor_area_m2"]
    comps = nar.get("rent", {}).get("comparables") or []      # [{"rent":1299,"m2":88,"label":"..."}]
    if comps:
        per_m2 = statistics.median(c["rent"] / c["m2"] for c in comps)
        central = per_m2 * m2
        low, high = central * 0.95, central * 1.05
        basis = "rent comparables"
    else:
        low, central, high = (sale * y / 12 for y in (YIELD_LOW, YIELD_BASE, YIELD_HIGH))
        basis = "sale value × gross yield"
    wws = ev["rent"]["scenarios"]
    return {
        "market_low": round(low, -1), "market": round(central, -1), "market_high": round(high, -1),
        "basis": basis, "per_m2": central / m2,
        "legal": {k: wws[k] for k in ("low", "typical", "high")},
        "gross_yield_pct": 100 * central * 12 / sale,
    }


def projection(ev: dict) -> dict:
    a = ev["purchase"]["appreciation"]
    quarters = [q["price_index_yoy_pct"] for q in a["province_index_recent_quarters"][:4]
                if q.get("price_index_yoy_pct") is not None]
    base = statistics.mean(quarters) if quarters else 3.0
    high = a.get("municipality_avg_sale_price_cagr_5y_pct") or base * 1.5
    high = max(high, base)
    low = base / 2
    start = ev["purchase"]["range"]["central"]
    years = list(range(PROJECTION_YEARS + 1))
    path = lambda r: [start * (1 + r / 100) ** y for y in years]
    return {"low": low, "base": base, "high": high, "years": years,
            "paths": {"low": path(low), "base": path(base), "high": path(high)}}


def similar_links(ev: dict) -> list[dict]:
    """Search pages for the user to open. They are built, never fetched."""
    place = ev["address"]["place"].lower()
    m2 = ev["dwelling"]["floor_area_m2"]
    lo, hi = int(m2 * 0.85), int(m2 * 1.15)
    area = quote(json.dumps([place]))
    size = quote(f'"{lo}-{hi}"')
    kind = "apartments" if ev["dwelling"]["is_apartment"] else "houses"
    return [
        {"label": f"For sale in {ev['address']['place']}, {lo}–{hi} m²", "site": "Funda",
         "url": f"https://www.funda.nl/zoeken/koop?selected_area={area}&floor_area={size}"},
        {"label": f"For rent in {ev['address']['place']}, {lo}–{hi} m²", "site": "Funda",
         "url": f"https://www.funda.nl/zoeken/huur?selected_area={area}&floor_area={size}"},
        {"label": f"Rental {kind} in {ev['address']['place']}", "site": "Pararius",
         "url": f"https://www.pararius.com/{kind}/{quote(place)}"},
        {"label": "WOZ values of this and neighbouring addresses", "site": "WOZ-waardeloket",
         "url": "https://www.wozwaardeloket.nl/"},
        {"label": "Registered energy label", "site": "EP-Online", "url": "https://www.ep-online.nl/"},
    ]


# ---------------------------------------------------------------------------
# SVG helpers (single hue; text uses theme tokens)
# ---------------------------------------------------------------------------

def bars(items: list[tuple[str, float, bool]], fmt, width=520) -> str:
    """Horizontal bars; the highlighted item uses the accent, others are muted."""
    if not items:
        return ""
    vmax = max(v for _, v, _ in items) or 1
    label_w, row_h, bar_h = 150, 34, 14
    plot_w = width - label_w - 70
    h = row_h * len(items) + 6
    out = [f'<svg viewBox="0 0 {width} {h}" class="chart" role="img">']
    for i, (label, v, hi) in enumerate(items):
        y = i * row_h + 10
        w = max(4, plot_w * v / vmax)
        cls = "bar hi" if hi else "bar"
        out.append(
            f'<g><title>{e(label)}: {e(fmt(v))}</title>'
            f'<text x="0" y="{y + bar_h - 2}" class="lbl">{e(label)}</text>'
            f'<rect x="{label_w}" y="{y}" width="{w:.1f}" height="{bar_h}" rx="4" class="{cls}"/>'
            f'<text x="{label_w + w + 8:.1f}" y="{y + bar_h - 2}" class="val">{e(fmt(v))}</text></g>'
        )
    out.append("</svg>")
    return "".join(out)


def line(points: list[tuple[str, float]], fmt, width=520, height=170, band=None) -> str:
    """Single-series line with endpoint label; optional (low, high) band per point."""
    if len(points) < 2:
        return ""
    vals = [v for _, v in points] + ([v for b in band for v in b] if band else [])
    vmin, vmax = min(vals), max(vals)
    pad = (vmax - vmin) * 0.12 or 1
    vmin, vmax = vmin - pad, vmax + pad
    left, right, top, bottom = 8, 84, 12, 26
    pw, ph = width - left - right, height - top - bottom
    X = lambda i: left + pw * i / (len(points) - 1)
    Y = lambda v: top + ph * (1 - (v - vmin) / (vmax - vmin))
    out = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img">']
    for t in (0, 0.5, 1):
        gy = top + ph * t
        out.append(f'<line x1="{left}" x2="{left + pw}" y1="{gy:.1f}" y2="{gy:.1f}" class="grid"/>')
    if band:
        up = " ".join(f"{X(i):.1f},{Y(b[1]):.1f}" for i, b in enumerate(band))
        dn = " ".join(f"{X(i):.1f},{Y(b[0]):.1f}" for i, b in reversed(list(enumerate(band))))
        out.append(f'<polygon points="{up} {dn}" class="band"/>')
    pts = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, (_, v) in enumerate(points))
    out.append(f'<polyline points="{pts}" class="ln"/>')
    for i, (lab, v) in enumerate(points):
        tip = f"{lab}: {fmt(v)}" + (f" (range {fmt(band[i][0])} – {fmt(band[i][1])})" if band else "")
        out.append(f'<g><title>{e(tip)}</title><circle cx="{X(i):.1f}" cy="{Y(v):.1f}" r="9" class="hit"/>'
                   f'<circle cx="{X(i):.1f}" cy="{Y(v):.1f}" r="{4.5 if i == len(points) - 1 else 3}" class="dot"/></g>')
    step = max(1, len(points) // 6)
    for i, (lab, _) in enumerate(points):
        if i % step == 0 or i == len(points) - 1:
            out.append(f'<text x="{X(i):.1f}" y="{height - 6}" class="axis" text-anchor="middle">{e(lab)}</text>')
    lx, (llab, lv) = X(len(points) - 1), points[-1]
    out.append(f'<text x="{lx + 10:.1f}" y="{Y(lv) + 4:.1f}" class="val">{e(fmt(lv))}</text>')
    out.append("</svg>")
    return "".join(out)


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

CSS = """
:root{--bg:#f5f6f4;--surface:#fff;--ink:#17201d;--muted:#5d6a65;--line:#dde3df;--accent:#1f6f5c;--accent-soft:#cfe5dd;
--muted-bar:#c3ccc8;--good:#2c7a43;--warn:#9a6812;--bad:#a63d3d;--chip:#eef2ef;
--display:"Sora","Segoe UI",system-ui,sans-serif;--body:"Source Sans 3","Segoe UI",system-ui,sans-serif;--mono:"IBM Plex Mono",ui-monospace,Consolas,monospace}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#101513;--surface:#171e1b;--ink:#e4ebe7;--muted:#97a59f;--line:#28322e;
--accent:#5fc3a6;--accent-soft:#1d3a31;--muted-bar:#3c4843;--good:#6cc58a;--warn:#e2b155;--bad:#ea8a8a;--chip:#1f2824;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#101513;--surface:#171e1b;--ink:#e4ebe7;--muted:#97a59f;--line:#28322e;
--accent:#5fc3a6;--accent-soft:#1d3a31;--muted-bar:#3c4843;--good:#6cc58a;--warn:#e2b155;--bad:#ea8a8a;--chip:#1f2824;color-scheme:dark}
body{background:var(--bg);color:var(--ink);font-family:var(--body);font-size:1.02rem;line-height:1.55}
.wrap{max-width:70rem;margin:0 auto;padding-inline:16px;padding-block:2rem 4rem;display:grid;gap:2.6rem}
h1,h2,h3{font-family:var(--display);margin:0;line-height:1.15;text-wrap:balance}
h1{font-size:clamp(1.8rem,4.4vw,2.7rem);font-weight:600;letter-spacing:-.015em}
h2{font-size:1.35rem;font-weight:600}h3{font-size:1rem;font-weight:600}
p{margin:0}ul{margin:0;padding-left:1.1rem;display:grid;gap:.35rem}
a{color:var(--accent)}
.eyebrow{font-family:var(--mono);font-size:.72rem;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}
.lede{color:var(--muted);max-width:46rem;font-size:1.08rem}
section{display:grid;gap:1rem;min-width:0}
.head{display:grid;gap:.6rem}
.facts{display:flex;flex-wrap:wrap;gap:.5rem}
.chip{font-family:var(--mono);font-size:.78rem;background:var(--chip);border-radius:999px;padding:.2rem .65rem;color:var(--ink)}
.verdicts{display:grid;grid-template-columns:repeat(auto-fit,minmax(15rem,1fr));gap:1rem}
.card{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:1.1rem 1.2rem;display:grid;gap:.45rem;align-content:start;min-width:0}
.card .big{font-family:var(--display);font-size:2rem;font-weight:600;font-variant-numeric:tabular-nums;letter-spacing:-.01em}
.card .range{font-family:var(--mono);font-size:.82rem;color:var(--muted)}
.card .note{font-size:.92rem;color:var(--muted)}
.pill{display:inline-flex;align-items:center;gap:.35rem;font-family:var(--mono);font-size:.74rem;padding:.12rem .55rem;border-radius:999px;border:1px solid currentColor;width:max-content}
.pill.good{color:var(--good)}.pill.warn{color:var(--warn)}.pill.bad{color:var(--bad)}.pill.neutral{color:var(--muted)}
.two{display:grid;grid-template-columns:repeat(auto-fit,minmax(20rem,1fr));gap:1.2rem}
.panel{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:1.1rem 1.2rem;display:grid;gap:.8rem;min-width:0}
.why li::marker{color:var(--accent)}
.chart{width:100%;height:auto;overflow:visible;font-family:var(--body)}
.chart .lbl,.chart .val,.chart .axis{fill:var(--muted);font-size:12px}.chart .val{fill:var(--ink);font-variant-numeric:tabular-nums}
.chart .bar{fill:var(--muted-bar)}.chart .bar.hi{fill:var(--accent)}
.chart .grid{stroke:var(--line);stroke-width:1}.chart .ln{fill:none;stroke:var(--accent);stroke-width:2}
.chart .dot{fill:var(--accent);stroke:var(--surface);stroke-width:2}.chart .hit{fill:transparent}
.chart .band{fill:var(--accent-soft)}
table{border-collapse:collapse;width:100%;font-size:.92rem}
.tw{overflow-x:auto}
th,td{text-align:left;padding:.45rem .6rem;border-bottom:1px solid var(--line);vertical-align:top}
th{font-family:var(--mono);font-size:.7rem;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);font-weight:500}
td.num{font-variant-numeric:tabular-nums;white-space:nowrap}
.kv{display:grid;grid-template-columns:auto 1fr;gap:.35rem 1rem;font-size:.95rem}
.kv dt{color:var(--muted)}.kv dd{margin:0;font-variant-numeric:tabular-nums}
.news li{display:grid;gap:.1rem}.news .meta{font-family:var(--mono);font-size:.74rem;color:var(--muted)}
.links{display:grid;grid-template-columns:repeat(auto-fit,minmax(16rem,1fr));gap:.7rem}
.links a{display:grid;gap:.15rem;background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:.8rem 1rem;text-decoration:none;color:var(--ink)}
.links a:hover,.links a:focus-visible{border-color:var(--accent);outline:none}
.links .site{font-family:var(--mono);font-size:.72rem;color:var(--accent);text-transform:uppercase;letter-spacing:.06em}
.foot{color:var(--muted);font-size:.85rem;border-top:1px solid var(--line);padding-top:1rem;display:grid;gap:.4rem}
"""


def li(items) -> str:
    return "".join(f"<li>{e(i)}</li>" for i in items or [])


def render(ev: dict, nar: dict) -> str:
    a, d, pur = ev["address"], ev["dwelling"], ev["purchase"]
    nb = ev["neighbourhood"]
    rng = pur["range"]
    rent = rent_estimate(ev, nar)
    proj = projection(ev)
    kf = nb.get("key_figures") or {}
    bu, gm, nl = kf.get(a["buurt_code"], {}), kf.get(a["municipality_code"], {}), kf.get("NL00", {})

    # Sale verdict pill
    check = pur.get("asking_price_check")
    sale_pill = ""
    if check:
        kind = {"below_range": ("good", "Below estimate"), "in_range": ("neutral", "In line"),
                "above_range": ("bad", "Above estimate")}[check["verdict"]]
        sale_pill = f'<span class="pill {kind[0]}">{kind[1]} · asking {eur(check["asking_price"])}</span>'

    # Rent verdict pill (contract context is decided in the narrative)
    rent_pill = ""
    rc = nar.get("rent", {}).get("verdict")
    if rc:
        rent_pill = f'<span class="pill {e(rc.get("tone", "neutral"))}">{e(rc["label"])}</span>'

    legal = rent["legal"]["typical"]
    seg_name = {"social": "social (regulated)", "middle": "middle (regulated)", "free": "free sector"}

    # Livability
    liv = (nb.get("livability") or {}).get(a["buurt_code"]) or []
    liv_latest = liv[-1] if liv else None
    liv_chart = line([(str(r["year"]), r["national_percentile"]) for r in liv if r["national_percentile"] is not None],
                     lambda v: f"P{v:.0f}") if liv else ""
    dims = []
    if liv_latest:
        names = {"safety": "Safety", "physical_environment": "Physical environment", "amenities": "Amenities",
                 "housing_stock": "Housing stock", "social_cohesion": "Social cohesion"}
        for k, n in names.items():
            v = liv_latest.get(k)
            if v is not None:
                tone = "good" if v > 0.01 else "bad" if v < -0.01 else "neutral"
                word = "above" if v > 0.01 else "below" if v < -0.01 else "around"
                dims.append(f'<tr><td>{n}</td><td><span class="pill {tone}">{word} national average</span></td>'
                            f'<td class="num">{v:+.3f}</td></tr>')

    # Crime
    crime = nb.get("crime") or {}
    crime_html = '<p class="note">Crime data was unavailable for this run.</p>'
    if "by_region" in crime:
        br = crime["by_region"]
        items = []
        for code, label, hi in ((a["buurt_code"], a["buurt_name"], True),
                                (a["municipality_code"], a["municipality_name"], False),
                                ("NL00", "Netherlands", False)):
            v = (br.get(code) or {}).get("per_1000_residents_last_12m")
            if v is not None:
                items.append((label, v, hi))
        bstats = br.get(a["buurt_code"], {})
        types = "".join(f'<tr><td>{e(t["type"].split(" ", 1)[-1])}</td><td class="num">{t["count"]}</td></tr>'
                        for t in crime.get("top_types_last_12m", [])[:6])
        crime_html = (
            f'{bars(items, lambda v: f"{v:.1f} per 1,000")}'
            f'<p class="note">Last 12 months to {e(crime.get("latest_period", "").replace("MM", "-"))}: '
            f'{bstats.get("last_12m", "–")} registered crimes in the neighbourhood, '
            f'{pct(bstats.get("change_pct"))} vs the previous 12 months.</p>'
            f'<div class="tw"><table><thead><tr><th>Most common</th><th>Count</th></tr></thead><tbody>{types}</tbody></table></div>'
        )

    # Infrastructure
    infra_rows = []
    for key, label in (("km_to_supermarket", "Large supermarket"), ("km_to_gp", "GP practice"),
                       ("km_to_school", "Primary school"), ("km_to_daycare", "Daycare")):
        if bu.get(key) is not None:
            infra_rows.append(f'<tr><td>{label}</td><td class="num">{bu[key]:.1f} km</td>'
                              f'<td class="num">{(gm.get(key) or 0):.1f} km</td><td class="num">{(nl.get(key) or 0):.1f} km</td></tr>')

    profile_rows = []
    for key, label, f in (("population", "Residents", lambda v: f"{v:,.0f}"),
                          ("avg_woz_value_eur", "Average WOZ value", eur),
                          ("avg_income_per_resident_eur", "Average income per resident", eur),
                          ("pct_apartments", "Apartments", lambda v: f"{v:.0f}%"),
                          ("pct_owner_occupied", "Owner-occupied", lambda v: f"{v:.0f}%"),
                          ("pct_social_housing", "Social housing", lambda v: f"{v:.0f}%")):
        if bu.get(key) is not None:
            profile_rows.append(f'<tr><td>{label}</td><td class="num">{f(bu[key])}</td>'
                                f'<td class="num">{f(gm[key]) if gm.get(key) is not None else "–"}</td>'
                                f'<td class="num">{f(nl[key]) if nl.get(key) is not None else "–"}</td></tr>')

    # Price history and projection
    app = pur["appreciation"]
    latest_q = (app["province_index_recent_quarters"] or [{}])[0]
    hist = line([(str(r["year"]), r["avg_sale_price_eur"]) for r in app["municipality_avg_sale_price"]],
                lambda v: f"€{v / 1000:.0f}k")
    pp = proj["paths"]
    now_year = int(ev["generated_on"][:4])
    proj_chart = line([(str(now_year + y), pp["base"][y]) for y in proj["years"]], lambda v: f"€{v / 1000:.0f}k",
                      band=[(pp["low"][y], pp["high"][y]) for y in proj["years"]])

    # Estimates table
    names = {"neighbourhood_price_per_m2": "Neighbourhood price per m²", "home_type_ratio": "Home-type adjustment",
             "woz_indexed": "WOZ value, indexed to today", "comparable_sale": "Comparable sale, indexed"}
    est_rows = "".join(f'<tr><td>{names.get(x["method"], x["method"])}</td><td class="num">{eur(x["value"])}</td></tr>'
                       for x in pur["estimates"])

    news = ev.get("news", {})
    notices = news.get("official_notices") if isinstance(news.get("official_notices"), list) else []
    heads = news.get("headlines") if isinstance(news.get("headlines"), list) else []
    news_html = "".join(
        f'<li><a href="{e(h["url"])}">{e(h["title"])}</a><span class="meta">{e(h["source"])} · {e(h["date"] or "")}</span></li>'
        for h in heads[:8])
    notice_html = "".join(
        f'<li><a href="{e(n["url"] or "#")}">{e(n["title"])}</a><span class="meta">{"on this street" if n["scope"] == "street" else "postcode area"} · {e(n["date"] or "")}</span></li>'
        for n in notices[:8])

    links = nar.get("similar_links") or similar_links(ev)
    links_html = "".join(f'<a href="{e(l["url"])}"><span class="site">{e(l["site"])}</span>{e(l["label"])}</a>' for l in links)
    comp_list = nar.get("comparables_note")

    title = nar.get("title") or f"{a['street']} {ev['user_inputs'].get('number', '')}"
    return f"""<title>{e(title)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600&family=Source+Sans+3:ital,wght@0,400;0,600;1,400&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>{CSS}</style>
<div class="wrap">

<header class="head">
  <span class="eyebrow">Grontia property report · {e(ev["generated_on"])}</span>
  <h1>{e(a["display_name"])}</h1>
  <p class="lede">{e(nar.get("headline", ""))}</p>
  <div class="facts">
    <span class="chip">{"Apartment" if d["is_apartment"] else "House"}</span>
    <span class="chip">{d["floor_area_m2"]} m²</span>
    <span class="chip">Built {d["build_year"]}</span>
    {f'<span class="chip">{d["units_in_building"]} units in building</span>' if d["is_apartment"] else ""}
    {f'<span class="chip">Energy label {e(ev["user_inputs"]["energy_label"])}</span>' if ev["user_inputs"].get("energy_label") else ""}
    <span class="chip">{e(a["buurt_name"])}, {e(a["municipality_name"])}</span>
  </div>
</header>

<section class="verdicts">
  <div class="card">
    <span class="eyebrow">Estimated sale value</span>
    <span class="big">{eur(rng["central"])}</span>
    <span class="range">range {eur(rng["low"])} – {eur(rng["high"])} · {eur(pur["price_per_m2_central"])}/m²</span>
    {sale_pill}
  </div>
  <div class="card">
    <span class="eyebrow">Estimated market rent</span>
    <span class="big">{eur(rent["market"])}<small style="font-size:1rem;font-weight:500">/mo</small></span>
    <span class="range">range {eur(rent["market_low"])} – {eur(rent["market_high"])} · {eur(rent["per_m2"], 2)}/m²</span>
    {rent_pill}
  </div>
  <div class="card">
    <span class="eyebrow">Legal rent check (WWS)</span>
    <span class="big">{legal["total_points"]} pts</span>
    <span class="range">{seg_name[legal["segment"]]} · max {eur(legal["max_rent_eur"], 2)} for a new contract</span>
    <span class="note">Range {rent["legal"]["low"]["total_points"]}–{rent["legal"]["high"]["total_points"]} points depending on unknown details.</span>
  </div>
  <div class="card">
    <span class="eyebrow">Neighbourhood livability</span>
    <span class="big">{f'P{liv_latest["national_percentile"]:.0f}' if liv_latest else "–"}</span>
    <span class="range">{f'national percentile, {liv_latest["year"]}' if liv_latest else "not available"}</span>
    <span class="note">Higher is better: share of Dutch neighbourhoods scoring lower.</span>
  </div>
</section>

<section class="two">
  <div class="panel">
    <h2>Why this sale value</h2>
    <ul class="why">{li(nar.get("sale", {}).get("why"))}</ul>
    <div class="tw"><table><thead><tr><th>Method</th><th>Estimate</th></tr></thead><tbody>{est_rows}</tbody></table></div>
  </div>
  <div class="panel">
    <h2>Why this rent</h2>
    <ul class="why">{li(nar.get("rent", {}).get("why"))}</ul>
    <dl class="kv">
      <dt>Market basis</dt><dd>{e(rent["basis"])}</dd>
      <dt>Gross yield</dt><dd>{rent["gross_yield_pct"]:.1f}% of the sale estimate</dd>
      <dt>Legal max, low / typical / high</dt><dd>{eur(rent["legal"]["low"]["max_rent_eur"])} / {eur(legal["max_rent_eur"])} / {eur(rent["legal"]["high"]["max_rent_eur"])}</dd>
    </dl>
  </div>
</section>

<section class="two">
  <div class="panel">
    <h2>Growth outlook</h2>
    <p>{e(nar.get("growth", {}).get("summary", ""))}</p>
    {proj_chart}
    <dl class="kv">
      <dt>Scenarios per year</dt><dd>{pct(proj["low"], sign=False)} · <strong>{pct(proj["base"], sign=False)}</strong> · {pct(proj["high"], sign=False)}</dd>
      <dt>Value in {now_year + PROJECTION_YEARS}</dt><dd>{eur(pp["low"][-1], 0)} – {eur(pp["high"][-1], 0)}</dd>
    </dl>
    <p class="note">Scenarios, not a forecast. Base = average yearly change of the regional price index over the last four quarters; high = the municipality's 5-year growth rate; low = half the base.</p>
  </div>
  <div class="panel">
    <h2>Price history, {e(a["municipality_name"])}</h2>
    {hist}
    <dl class="kv">
      <dt>5-year growth</dt><dd>{pct(app.get("municipality_avg_sale_price_cagr_5y_pct"))} per year</dd>
      <dt>Regional index, latest quarter</dt><dd>{pct(latest_q.get("price_index_yoy_pct"))} year on year</dd>
    </dl>
  </div>
</section>

<section>
  <div class="head"><span class="eyebrow">The neighbourhood</span><h2>{e(a["buurt_name"])}</h2>
  <p class="lede">{e(nar.get("neighbourhood", {}).get("summary", ""))}</p></div>
  <div class="two">
    <div class="panel"><h3>Highlights</h3><ul class="why">{li(nar.get("neighbourhood", {}).get("highlights"))}</ul></div>
    <div class="panel"><h3>Watch-outs</h3><ul class="why">{li(nar.get("neighbourhood", {}).get("watch_outs"))}</ul></div>
  </div>
  <div class="two">
    <div class="panel"><h3>Livability over time</h3>{liv_chart or '<p class="note">Not available.</p>'}
      <div class="tw"><table><tbody>{"".join(dims)}</tbody></table></div></div>
    <div class="panel"><h3>Safety: registered crime per 1,000 residents</h3>{crime_html}</div>
  </div>
  <div class="two">
    <div class="panel"><h3>Daily needs: distance to nearest</h3>
      <div class="tw"><table><thead><tr><th></th><th>Here</th><th>Municipality</th><th>NL</th></tr></thead><tbody>{"".join(infra_rows)}</tbody></table></div></div>
    <div class="panel"><h3>Who lives here</h3>
      <div class="tw"><table><thead><tr><th></th><th>Here</th><th>Municipality</th><th>NL</th></tr></thead><tbody>{"".join(profile_rows)}</tbody></table></div></div>
  </div>
</section>

<section class="two">
  <div class="panel"><h2>Local news</h2><ul class="news">{news_html or "<li>No headlines found.</li>"}</ul></div>
  <div class="panel"><h2>Official notices nearby</h2><ul class="news">{notice_html or "<li>No permits or notices in the last six months.</li>"}</ul></div>
</section>

<section>
  <div class="head"><span class="eyebrow">Look further</span><h2>Similar homes</h2>
  <p class="lede">{e(comp_list or "Search pages filtered to this area and size. Grontia builds these links but never reads listing sites.")}</p></div>
  <div class="links">{links_html}</div>
</section>

<footer class="foot">
  <strong>Assumptions and limits</strong>
  <ul>{li(nar.get("caveats"))}{li(ev["rent"]["assumptions"])}</ul>
  <p>Sources: CBS StatLine, PDOK (BAG, Locatieserver, CBS wijken en buurten), Politie open data, Leefbaarometer 3.0 (BZK), overheid.nl official publications, Huurcommissie WWS rules valid from {e(ev["rent"]["rules_valid_from"])}. Estimates only; the Huurcommissie gives binding rent rulings.</p>
</footer>
</div>
"""


def main() -> None:
    ev_path, nar_path, out_path = sys.argv[1:4]
    ev = json.load(open(ev_path, encoding="utf-8"))
    nar = json.load(open(nar_path, encoding="utf-8"))
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(render(ev, nar))
    print(out_path)


if __name__ == "__main__":
    main()
