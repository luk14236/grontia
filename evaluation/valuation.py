"""
Purchase question: is the asking price in line with what open data suggests?

Up to three estimates, then a range whose width says how much the centre knows:
  A. neighbourhood model (always): the neighbourhood's average WOZ value (CBS,
     every home in the stock) brought to today with the price index, split into
     an apartment and a single-family level with the national price ratio
     between the two (CBS 85791NED) and the neighbourhood's mix (BAG), then
     scaled from the typical size of that home type in the neighbourhood to
     this home's floor area with a size elasticity;
  B. WOZ (only when the user gives it): this home's own WOZ value brought from
     its value date to today with the same index;
  C. comparable sale (only when the user gives it): a known sale of a similar
     home nearby, brought to today with the index and scaled by floor area.

The centre is the most specific evidence available (C, then B, then A) and the
other estimates are shown as cross-checks. The model sees only area, type and
size: not condition, energy label, layout, garden, parking or floor level, so
on its own it gets the widest range.

Why WOZ and not the average sale price as the area level: sale prices describe
the homes that sold (larger, more often owner-occupied), while the BAG floor
areas the model divides by describe every home. The average WOZ covers the
same stock as the BAG, so price and size refer to the same homes. Using the
sale average put the level 9-14% too high in the neighbourhoods tested.
"""
from __future__ import annotations

import statistics

from . import warehouse as w

# A home twice the size of its neighbours costs about 1.7x, not 2x. 0.75 sits in
# the 0.6-0.9 range Dutch hedonic price studies report; it is an assumption, not
# fitted on transactions (Grontia has no open per-sale data to fit it on).
SIZE_ELASTICITY = 0.75
# Below this many homes of the same type the neighbourhood split is too thin
MIN_PEERS = 15
# Half-width of the value range by what the centre rests on, and how sure it is
BANDS = {"comparable_sale": 0.07, "woz_indexed": 0.10, "neighbourhood_model": 0.20}
CONFIDENCE = {"comparable_sale": "high", "woz_indexed": "medium", "neighbourhood_model": "low"}
# A cross-check further than this from the centre is worth flagging
DISAGREE = 0.25
# Below this share of the neighbourhood's homes, the home's type is unusual there
RARE_TYPE_SHARE = 0.15


def _latest_type_ratio(con, home_type: str) -> dict | None:
    r = w.one(con, f"""
        with y as (
            select year, home_type, avg_sale_price_eur
            from {w.STAGING}.stg_cbs_sale_prices_by_home_type
            where period_type = 'year' and avg_sale_price_eur is not null
        )
        select t.year, t.avg_sale_price_eur as type_price, a.avg_sale_price_eur as all_price
        from y t join y a on a.year = t.year and a.home_type = 'total'
        where t.home_type = ?
        order by t.year desc limit 1
    """, home_type)
    if r:
        r["ratio"] = r["type_price"] / r["all_price"]
    return r


def _index(con, region: str, period_like: str) -> dict | None:
    return w.one(con, f"""
        select period_code, price_index_2020 from {w.STAGING}.stg_cbs_sale_price_index
        where region_code = ? and period_code like ? and price_index_2020 is not null
        order by period_code desc limit 1
    """, region, period_like)


def _quarter(period: str) -> str:
    """'2026-05' or '2026-05-14' -> '2026KW02'."""
    year, month = int(period[:4]), int(period[5:7])
    return f"{year}KW{(month - 1) // 3 + 1:02d}"


def _woz_value_date_index(con, region: str, woz_year: int) -> dict | None:
    """Index at the WOZ value date: 1 January of the year before the tax year,
    i.e. the last quarter two years before it (WOZ 2024 -> 2022KW04)."""
    return _index(con, region, f"{woz_year - 2}KW04")


def _area_level(con, ref: dict, index_now: dict) -> dict | None:
    """Today's value of the neighbourhood's average home, from its average WOZ."""
    kf = w.one(con, f"""
        select period_code from {w.STAGING}.stg_cbs_neighbourhood_key_figures where region_code = ?
    """, ref["region_code"])
    if not kf or not ref.get("avg_woz_value_eur"):
        return None
    woz_year = int(kf["period_code"][:4])
    then = _woz_value_date_index(con, ref["index_region_code"], woz_year)
    if not then:
        return None
    factor = index_now["price_index_2020"] / then["price_index_2020"]
    return {"avg_woz_value_eur": ref["avg_woz_value_eur"], "woz_year": woz_year,
            "woz_value_date": f"{woz_year - 1}-01-01", "index_then": then, "index_now": index_now,
            "factor": round(factor, 3), "level_eur": round(ref["avg_woz_value_eur"] * factor, -3)}


def _neighbourhood_model(con, level: dict, home_type: str, m2: float, sizes: dict) -> dict | None:
    peers = (sizes.get("by_type") or {}).get(home_type) or {}
    value_level, inputs = level["level_eur"], {}
    if peers.get("count", 0) >= MIN_PEERS:
        # average = share_apt * V_apt + share_house * V_house, with V_apt = r * V_house
        apt, house = _latest_type_ratio(con, "apartment"), _latest_type_ratio(con, "single_family")
        n_apt = sizes["by_type"]["apartment"]["count"]
        share_apt = n_apt / (n_apt + sizes["by_type"]["single_family"]["count"])
        if apt and house:
            r = apt["ratio"] / house["ratio"]
            v_house = value_level / (share_apt * r + (1 - share_apt))
            value_level = r * v_house if home_type == "apartment" else v_house
            inputs = {"apartment_share": round(share_apt, 3), "apartment_to_house_ratio": round(r, 3),
                      "type_ratio_year": apt["year"]}
        peer_m2 = peers["mean_m2"]
    elif sizes.get("mean_m2"):
        peer_m2 = sizes["mean_m2"]
    else:
        return None
    size_factor = (m2 / peer_m2) ** SIZE_ELASTICITY
    return {
        "method": "neighbourhood_model",
        "value": round(value_level * size_factor, -3),
        "inputs": {"area_level": level["level_eur"], "home_type": home_type,
                   "type_level": round(value_level, -3), **inputs,
                   "peer_mean_m2": peer_m2, "peer_count": peers.get("count") or sizes.get("count"),
                   "floor_area_m2": m2, "size_factor": round(size_factor, 3),
                   "size_elasticity": SIZE_ELASTICITY},
    }


def estimate(con, address: dict, dwelling: dict, buurt_sizes: dict,
             asking_price: float | None = None, woz_value: float | None = None,
             woz_year: int | None = None, comparable: dict | None = None) -> dict:
    ref = w.one(con, f"select * from {w.MARTS}.mart_neighbourhood_reference where region_code = ?",
                address["buurt_code"])
    if not ref:
        raise ValueError(f"No neighbourhood reference for {address['buurt_code']}")

    m2 = dwelling["floor_area_m2"]
    home_type = "apartment" if dwelling["is_apartment"] else "single_family"
    # the four largest cities have their own index; elsewhere the province's
    index_region = ref["index_region_code"]
    index_now = _index(con, index_region, "%KW%")
    if not index_now:
        raise ValueError(f"No price index for {index_region}")
    estimates: dict[str, dict] = {}

    level = _area_level(con, ref, index_now)
    model = _neighbourhood_model(con, level, home_type, m2, buurt_sizes) if level else None
    if model:
        estimates["neighbourhood_model"] = model

    if woz_value:
        # WOZ value date is 1 January of the year before the decision year
        tax_year = woz_year or int(index_now["period_code"][:4])
        index_then = _woz_value_date_index(con, index_region, tax_year)
        if index_then:
            factor = index_now["price_index_2020"] / index_then["price_index_2020"]
            estimates["woz_indexed"] = {
                "method": "woz_indexed",
                "value": round(woz_value * factor, -3),
                "inputs": {"woz_value": woz_value, "woz_value_date": f"{tax_year - 1}-01-01",
                           "index_then": index_then, "index_now": index_now,
                           "factor": round(factor, 3)},
            }

    if comparable:
        # Use the sale quarter, or the latest published one if the sale is more recent
        sale_q = _quarter(comparable["date"])
        index_sale = _index(con, index_region, sale_q) if sale_q <= index_now["period_code"] else None
        index_sale = index_sale or index_now
        factor = index_now["price_index_2020"] / index_sale["price_index_2020"]
        comp_m2 = comparable.get("floor_area_m2") or m2
        size = (m2 / comp_m2) ** SIZE_ELASTICITY
        estimates["comparable_sale"] = {
            "method": "comparable_sale",
            "value": round(comparable["price"] * factor * size, -3),
            "inputs": {"sale_price": comparable["price"], "sale_date": comparable["date"],
                       "comparable_m2": comp_m2, "index_sale": index_sale, "index_now": index_now,
                       "factor": round(factor, 3), "size_factor": round(size, 3),
                       "price_per_m2": round(comparable["price"] / comp_m2)},
        }

    if not estimates:
        raise ValueError(f"Not enough data to value a home in {address['buurt_code']}")
    basis = next(k for k in ("comparable_sale", "woz_indexed", "neighbourhood_model") if k in estimates)
    central = estimates[basis]["value"]
    band = BANDS[basis]
    low, high = round(central * (1 - band), -3), round(central * (1 + band), -3)

    cross_checks = {k: round(100 * (e["value"] / central - 1), 1) for k, e in estimates.items() if k != basis}
    warnings = []
    if any(abs(v) > 100 * DISAGREE for v in cross_checks.values()):
        warnings.append("estimates_disagree")
    share = (model or {}).get("inputs", {}).get("apartment_share")
    if share is not None and (share if home_type == "apartment" else 1 - share) < RARE_TYPE_SHARE:
        # few homes of this type here: the type level leans on the national ratio
        warnings.append("rare_type")
    if dwelling.get("use") and dwelling["use"] != "woonfunctie":
        # the BAG area of a mixed-use unit can include the shop or office
        warnings.append("mixed_use")

    verdict = None
    if asking_price:
        diff = asking_price / central - 1
        if asking_price < low:
            verdict = "below_range"
        elif asking_price > high:
            verdict = "above_range"
        else:
            verdict = "in_range"
        verdict = {"asking_price": asking_price, "vs_central_pct": round(100 * diff, 1), "verdict": verdict}

    peers = (buurt_sizes.get("by_type") or {}).get(home_type) or {}
    return {
        "estimates": list(estimates.values()),
        "range": {"low": low, "central": central, "high": high},
        "price_per_m2_central": round(central / m2),
        "central_basis": basis,
        "confidence": CONFIDENCE[basis],
        "band_pct": round(100 * band),
        "cross_checks_pct": cross_checks,
        "warnings": warnings,
        "asking_price_check": verdict,
        "area_level": level,
        "size_vs_peers": {"home_type": home_type, "floor_area_m2": m2,
                          **{k: peers.get(k) for k in ("count", "median_m2", "p25_m2", "p75_m2")}},
        "neighbourhood_reference": {
            k: ref[k] for k in (
                "avg_woz_value_eur", "municipality_avg_woz_eur", "woz_ratio_to_municipality",
                "municipality_est_current_avg_sale_price_eur", "est_current_avg_sale_price_eur",
                "index_latest_period", "price_change_since_base_year", "price_base_year")
        },
        "index_region_code": index_region,
        "index_now": index_now,
    }


def appreciation(con, address: dict) -> dict:
    """How prices moved around the dwelling: municipality, province and home type."""
    gm, pv = address["municipality_code"], address["province_code"]
    sale = w.rows(con, f"""
        select year, avg_sale_price_eur from {w.STAGING}.stg_cbs_sale_prices_annual
        where region_code = ? and avg_sale_price_eur is not null order by year
    """, gm)
    woz = w.rows(con, f"""
        select cast(left(period_code, 4) as integer) as year, avg_woz_value_eur
        from {w.MARTS}.mart_housing_market
        where region_code = ? and avg_woz_value_eur is not null order by year
    """, gm)
    index = w.rows(con, f"""
        select period_code, price_index_2020, price_index_yoy_pct from {w.STAGING}.stg_cbs_sale_price_index
        where region_code = ? and period_type = 'quarter' order by period_code desc limit 8
    """, pv)
    by_type = w.rows(con, f"""
        select year, home_type, avg_sale_price_eur from {w.STAGING}.stg_cbs_sale_prices_by_home_type
        where period_type = 'year' and home_type in ('total', 'apartment') and year >= 2015
        order by year, home_type
    """)

    def cagr(series: list[dict], key: str, years: int) -> float | None:
        if len(series) <= years:
            return None
        end, start = series[-1][key], series[-1 - years][key]
        return round(100 * ((end / start) ** (1 / years) - 1), 1)

    return {
        "municipality_avg_sale_price": sale[-12:],
        "municipality_avg_sale_price_cagr_5y_pct": cagr(sale, "avg_sale_price_eur", 5),
        "municipality_avg_sale_price_cagr_10y_pct": cagr(sale, "avg_sale_price_eur", 10),
        "municipality_avg_woz": woz,
        "province_index_recent_quarters": index,
        "national_by_home_type": by_type,
    }
