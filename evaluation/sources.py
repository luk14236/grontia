"""
On-demand lookups for one address, all from open government sources.

Listing portals (Funda, Pararius, ...) are never fetched: the listing's facts
come from the user. News headlines are kept as title + link + date only.
"""
from __future__ import annotations

import logging
import re
import statistics
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests

logger = logging.getLogger(__name__)

LOCATIESERVER  = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
BAG            = "https://api.pdok.nl/kadaster/bag/ogc/v2/collections"
WIJKEN_BUURTEN = "https://api.pdok.nl/cbs/wijken-en-buurten-2024/ogc/v1/collections"
POLITIE        = "https://dataderden.cbs.nl/ODataApi/odata/47022NED"
BEKENDMAKINGEN = "https://repository.overheid.nl/sru"
NEWS_RSS       = "https://news.google.com/rss/search"

TIMEOUT = 60


def _get(url: str, **params) -> requests.Response:
    r = requests.get(url, params=params or None, timeout=TIMEOUT)
    r.raise_for_status()
    return r


# ---------------------------------------------------------------------------
# Address and dwelling (PDOK Locatieserver + BAG)
# ---------------------------------------------------------------------------

def lookup_address(postcode: str, number: int, suffix: str | None = None) -> dict:
    postcode = postcode.replace(" ", "").upper()
    q = f"postcode:{postcode} and huisnummer:{number}"
    docs = _get(LOCATIESERVER, q=q, fq="type:adres", rows=10).json()["response"]["docs"]
    if suffix:
        s = suffix.upper()
        docs = [d for d in docs if s in (str(d.get("huisletter", "")) + str(d.get("huisnummertoevoeging", ""))).upper()]
    if not docs:
        raise ValueError(f"Address not found: {postcode} {number}{suffix or ''}")
    d = docs[0]
    lon, lat = map(float, re.findall(r"[-\d.]+", d["centroide_ll"]))
    return {
        "display_name": d["weergavenaam"],
        "postcode": d["postcode"],
        "street": d["straatnaam"],
        "place": d["woonplaatsnaam"],
        "dwelling_id": d["adresseerbaarobject_id"],
        "buurt_code": d["buurtcode"],
        "buurt_name": d["buurtnaam"],
        "wijk_code": d["wijkcode"],
        "wijk_name": d.get("wijknaam"),
        "municipality_code": "GM" + d["gemeentecode"],
        "municipality_name": d["gemeentenaam"],
        "province_code": d["provinciecode"],
        "province_name": d["provincienaam"],
        "lon": lon,
        "lat": lat,
        "linked_apartment_rights": d.get("gekoppeld_appartement", []),
    }


def bag_dwelling(dwelling_id: str) -> dict:
    vbo = _get(f"{BAG}/verblijfsobject/items", f="json", identificatie=dwelling_id).json()["features"][0]["properties"]
    pand_url = vbo["pand.href"][0] if isinstance(vbo.get("pand.href"), list) else vbo.get("pand.href")
    pand = _get(pand_url, f="json").json()["properties"]
    units = pand.get("aantal_verblijfsobjecten")
    return {
        "floor_area_m2": vbo["oppervlakte"],
        "use": vbo["gebruiksdoel"],
        "status": vbo["status"],
        "build_year": pand["bouwjaar"],
        "units_in_building": units,
        # BAG has no dwelling type; more than one unit in the building means an apartment
        "is_apartment": bool(units and units > 1),
    }


# ---------------------------------------------------------------------------
# Neighbourhood polygon and dwelling sizes
# ---------------------------------------------------------------------------

def buurt_area(buurt_code: str) -> dict:
    f = _get(f"{WIJKEN_BUURTEN}/buurten/items", f="json", buurtcode=buurt_code).json()["features"][0]
    return {"geometry": f["geometry"], "properties": f["properties"]}


def _point_in_ring(x: float, y: float, ring: list) -> bool:
    inside = False
    for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1]):
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
            inside = not inside
    return inside


def _point_in_geometry(x: float, y: float, geometry: dict) -> bool:
    polygons = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [geometry["coordinates"]]
    for rings in polygons:
        if _point_in_ring(x, y, rings[0]) and not any(_point_in_ring(x, y, h) for h in rings[1:]):
            return True
    return False


def _bbox(geometry: dict) -> tuple[float, float, float, float]:
    polygons = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [geometry["coordinates"]]
    xs = [p[0] for rings in polygons for p in rings[0]]
    ys = [p[1] for rings in polygons for p in rings[0]]
    return min(xs), min(ys), max(xs), max(ys)


def buurt_dwelling_sizes(geometry: dict) -> dict:
    """Floor area of every residential BAG object inside the neighbourhood."""
    minx, miny, maxx, maxy = _bbox(geometry)
    url = f"{BAG}/verblijfsobject/items"
    params = {"f": "json", "bbox": f"{minx},{miny},{maxx},{maxy}", "limit": 1000}
    sizes: list[int] = []
    while url:
        page = _get(url, **params).json() if params else _get(url).json()
        for f in page["features"]:
            p = f["properties"]
            if "woonfunctie" not in (p.get("gebruiksdoel") or ""):
                continue
            if not p.get("status", "").startswith("Verblijfsobject in gebruik"):
                continue
            x, y = f["geometry"]["coordinates"][:2]
            if _point_in_geometry(x, y, geometry) and p.get("oppervlakte"):
                sizes.append(p["oppervlakte"])
        url = next((l["href"] for l in page.get("links", []) if l.get("rel") == "next"), None)
        params = None
    if not sizes:
        return {"count": 0}
    return {
        "count": len(sizes),
        "mean_m2": round(statistics.mean(sizes), 1),
        "median_m2": statistics.median(sizes),
    }


# ---------------------------------------------------------------------------
# Crime (Politie, registered crimes per month, neighbourhood level)
# ---------------------------------------------------------------------------

def _politie(filter_: str) -> list[dict]:
    rows, url = [], f"{POLITIE}/TypedDataSet"
    params = {"$filter": filter_, "$format": "json"}
    while url:
        page = _get(url, **params).json() if params else _get(url).json()
        rows.extend(page["value"])
        url, params = page.get("odata.nextLink"), None
    return rows


def crime(region_codes: list[str], buurt_code: str, months: int = 12) -> dict:
    """Monthly totals for each region and, for the neighbourhood, the crime types."""
    # Region keys are padded with spaces to 10 characters ('GM1948    ', 'NL00      ')
    codes = " or ".join(f"WijkenEnBuurten eq '{c.ljust(10)}'" for c in region_codes)
    totals = _politie(f"SoortMisdrijf eq '0.0.0 ' and ({codes})")
    monthly: dict[str, dict[str, int]] = {}
    for r in totals:
        if "MM" in r["Perioden"] and r["GeregistreerdeMisdrijven_1"] is not None:
            monthly.setdefault(r["WijkenEnBuurten"].strip(), {})[r["Perioden"]] = r["GeregistreerdeMisdrijven_1"]

    periods = sorted(monthly.get(buurt_code, {}))
    last = periods[-months:]
    prev = periods[-2 * months:-months]
    summary = {
        "latest_period": last[-1] if last else None,
        "window_months": months,
        "by_region": {
            code: {
                "last_12m": sum(series.get(p, 0) for p in last),
                "previous_12m": sum(series.get(p, 0) for p in prev),
                "yearly": {
                    y: sum(v for p, v in series.items() if p.startswith(y))
                    for y in sorted({p[:4] for p in series})
                },
            }
            for code, series in monthly.items()
        },
    }

    # Crime types in the neighbourhood over the last window
    types = {t["Key"]: t["Title"] for t in _get(f"{POLITIE}/SoortMisdrijf", **{"$format": "json"}).json()["value"]}
    period_filter = " or ".join(f"Perioden eq '{p}'" for p in last)
    detail = _politie(f"WijkenEnBuurten eq '{buurt_code}' and ({period_filter})")
    by_type: dict[str, int] = {}
    for r in detail:
        key = r["SoortMisdrijf"]
        # Only leaf categories (x.y.z with z != 0) to avoid double counting
        if key.strip() == "0.0.0" or key.strip().endswith(".0"):
            continue
        by_type[types.get(key, key).strip()] = by_type.get(types.get(key, key).strip(), 0) + (r["GeregistreerdeMisdrijven_1"] or 0)
    summary["top_types_last_12m"] = sorted(
        ({"type": k, "count": v} for k, v in by_type.items() if v), key=lambda x: -x["count"]
    )[:8]
    return summary


# ---------------------------------------------------------------------------
# Local news: official notices (overheid.nl) and press headlines
# ---------------------------------------------------------------------------

def official_notices(municipality_name: str, postcode: str, street: str, since: str) -> list[dict]:
    """Permits, demolition, events... published by the municipality near the address."""
    query = (
        f'c.product-area==officielepublicaties AND dt.creator=="{municipality_name}" '
        f"AND dt.modified>={since}"
    )
    records, start = [], 1
    while True:
        text = _get(BEKENDMAKINGEN, operation="searchRetrieve", version="2.0",
                    maximumRecords=1000, startRecord=start, query=query).text
        page = re.findall(r"<sru:record>.*?</sru:record>", text, flags=re.S)
        records.extend(page)
        total = int((re.findall(r"numberOfRecords>(\d+)", text) or [0])[0])
        start += len(page)
        if not page or start > total:
            break

    pc4, pc6 = postcode[:4], postcode
    notices = []
    for rec in records:
        title = re.search(r"<dcterms:title>(.*?)</dcterms:title>", rec, flags=re.S)
        if not title:
            continue
        title = re.sub(r"\s+", " ", title.group(1)).strip()
        date = re.search(r"<dcterms:modified>([\d-]+)", rec)
        link = re.search(r"<gzd:preferredUrl>(.*?)</gzd:preferredUrl>", rec)
        if street.lower() in title.lower() or pc6 in title.replace(" ", ""):
            scope = "street"
        elif pc4 in title:
            scope = "postcode_area"
        else:
            continue
        notices.append({
            "title": title, "date": date.group(1) if date else None,
            "url": link.group(1) if link else None, "scope": scope,
        })
    # Notices on the street itself first, then the postcode area; newest first in each
    notices.sort(key=lambda n: n["date"] or "", reverse=True)
    notices.sort(key=lambda n: n["scope"] != "street")
    return notices


def news_headlines(place: str, limit: int = 15) -> list[dict]:
    """Recent press headlines mentioning the place (title, source, date, link only)."""
    xml = _get(NEWS_RSS, q=f'"{place}"', hl="nl", gl="NL", ceid="NL:nl").text
    items = []
    for item in ET.fromstring(xml).iter("item"):
        title = item.findtext("title") or ""
        source = item.findtext("source") or ""
        pub = item.findtext("pubDate")
        items.append({
            "title": title.rsplit(" - ", 1)[0] if source and title.endswith(source) else title,
            "source": source,
            "date": parsedate_to_datetime(pub).date().isoformat() if pub else None,
            "url": item.findtext("link"),
        })
    items.sort(key=lambda i: i["date"] or "", reverse=True)
    return items[:limit]


def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")
