"""
Evaluate one address: purchase price check, rent check and neighbourhood context.

    python -m evaluation --postcode 5461AB --number 1 [--asking-price 350000]
        [--rent 1200] [--deposit 2400] [--woz 310000 --woz-year 2026]
        [--energy-label A] [--comparable-price 415000 --comparable-date 2026-06]
        [--out result.json]

The result holds the user's own address: it is written only where --out points
(by default the warehouse volume), never to the repository.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path

from . import sources, valuation, warehouse as w
from .wws import DwellingFacts, assess

logger = logging.getLogger("evaluation")
OUT_DIR = Path(os.getenv("GRONTIA_EVALUATIONS_DIR", "/opt/grontia/warehouse/evaluations"))


def neighbourhood_profile(con, address: dict) -> dict:
    codes = [address["buurt_code"], address["wijk_code"], address["municipality_code"], "NL00"]
    found = w.rows(con, f"""
        select * exclude (ingestion_date, ingestion_timestamp)
        from {w.STAGING}.stg_cbs_neighbourhood_key_figures
        where region_code in ({','.join('?' * len(codes))})
    """, *codes)
    return {r["region_code"]: r for r in found}


def livability(con, address: dict) -> dict | None:
    try:
        series = w.rows(con, f"""
            select region_code, region_level, year, livability_score, national_percentile,
                   physical_environment, safety, social_cohesion, amenities, housing_stock,
                   change_since_previous, change_since_2014
            from {w.MARTS}.mart_livability
            where region_code in (?, ?, ?) order by region_code, year
        """, address["buurt_code"], address["wijk_code"], address["municipality_code"])
    except Exception as e:  # mart not built yet
        logger.warning("Livability not available: %s", e)
        return None
    out: dict = {}
    for r in series:
        out.setdefault(r["region_code"], []).append(r)
    return out


def crime_context(address: dict, profile: dict) -> dict:
    codes = [address["buurt_code"], address["municipality_code"], "NL00"]
    c = sources.crime(codes, address["buurt_code"])
    for code, stats in c["by_region"].items():
        pop = (profile.get(code) or {}).get("population")
        if pop:
            stats["per_1000_residents_last_12m"] = round(1000 * stats["last_12m"] / pop, 1)
            stats["population"] = pop
        if stats["previous_12m"]:
            stats["change_pct"] = round(100 * (stats["last_12m"] / stats["previous_12m"] - 1), 1)
    return c


def woz_range_from_valuation(con, address: dict, value: dict) -> tuple[float, float, float] | None:
    """Bring the market value estimate back to the WOZ value date (1 Jan 2025)."""
    then = valuation._index(con, address["province_code"], "2024KW04")
    now = value.get("index_now")
    if not then or not now:
        return None
    f = then["price_index_2020"] / now["price_index_2020"]
    r = value["range"]
    return tuple(round(r[k] * f, -3) for k in ("low", "central", "high"))


def _optional(name: str, fn, *args):
    """Context sources must not sink the whole evaluation when one is down."""
    try:
        return fn(*args)
    except Exception as e:
        logger.warning("%s unavailable: %s", name, e)
        return {"error": f"{type(e).__name__}: {e}"}


def run(args: argparse.Namespace) -> dict:
    address = sources.lookup_address(args.postcode, args.number, args.suffix)
    dwelling = sources.bag_dwelling(address["dwelling_id"])
    area = sources.buurt_area(address["buurt_code"])
    sizes = sources.buurt_dwelling_sizes(area["geometry"])

    con = w.connect()
    profile = neighbourhood_profile(con, address)
    comparable = ({"price": args.comparable_price, "date": args.comparable_date or date.today().isoformat(),
                   "floor_area_m2": args.comparable_m2} if args.comparable_price else None)
    value = valuation.estimate(con, address, dwelling, sizes, args.asking_price, args.woz, args.woz_year, comparable)

    facts = DwellingFacts(
        floor_area_m2=dwelling["floor_area_m2"], build_year=dwelling["build_year"],
        is_apartment=dwelling["is_apartment"], energy_label=args.energy_label,
        woz_value=args.woz,
    )
    if args.woz and args.woz_year and args.woz_year != 2026:
        facts.assumptions.append(f"woz_value: decision year {args.woz_year}, rules expect the 2026 decision")
    rent = assess(facts, asked_rent=args.rent, deposit=args.deposit,
                  woz_range=None if args.woz else woz_range_from_valuation(con, address, value))

    since = (date.today() - timedelta(days=180)).isoformat()
    result = {
        "generated_on": date.today().isoformat(),
        "address": address,
        "dwelling": dwelling,
        "user_inputs": {k: v for k, v in vars(args).items() if k not in ("out",) and v is not None},
        "purchase": {**value, "appreciation": valuation.appreciation(con, address)},
        "rent": rent,
        "neighbourhood": {
            "area_properties": {k: area["properties"].get(k) for k in (
                "aantal_inwoners", "aantal_huishoudens", "gemiddelde_huishoudsgrootte",
                "omgevingsadressendichtheid", "stedelijkheid_adressen_per_km2")},
            "dwelling_sizes": sizes,
            "key_figures": profile,
            "livability": livability(con, address),
            "crime": _optional("crime", crime_context, address, profile),
        },
        "news": {
            "official_notices": _optional(
                "official notices", sources.official_notices,
                address["municipality_name"], address["postcode"], address["street"], since),
            "headlines": _optional("headlines", sources.news_headlines, address["place"]),
        },
    }
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m evaluation")
    p.add_argument("--postcode", required=True)
    p.add_argument("--number", type=int, required=True)
    p.add_argument("--suffix")
    p.add_argument("--asking-price", type=float)
    p.add_argument("--rent", type=float, help="asked base rent (kale huur) per month")
    p.add_argument("--deposit", type=float)
    p.add_argument("--woz", type=float, help="WOZ value from the latest WOZ decision")
    p.add_argument("--woz-year", type=int, help="year of that WOZ decision")
    p.add_argument("--energy-label")
    p.add_argument("--comparable-price", type=float, help="known sale price of a similar dwelling nearby")
    p.add_argument("--comparable-date", help="sale date of that dwelling, YYYY-MM (default: today)")
    p.add_argument("--comparable-m2", type=float, help="its floor area (default: same as this dwelling)")
    p.add_argument("--out", help="output JSON path (default: warehouse evaluations dir)")
    args = p.parse_args()

    result = run(args)
    out = Path(args.out) if args.out else OUT_DIR / f"{result['address']['postcode']}-{args.number}{args.suffix or ''}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    logger.info("Written %s", out)


if __name__ == "__main__":
    main()
