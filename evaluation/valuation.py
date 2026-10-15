"""
Purchase question: is the asking price in line with what open data suggests?

Three independent estimates, each from open data, then a range:
  A. neighbourhood price per m²: the neighbourhood's estimated current average
     sale price divided by the mean floor area of its dwellings (BAG), times
     this dwelling's floor area;
  B. home type: the neighbourhood's average price scaled by the national price
     ratio between this home type and all homes (CBS 85791NED);
  C. WOZ (only when the user gives it): the dwelling's WOZ value brought from
     its reference date to today with the provincial price index;
  D. comparable sale (only when the user gives it): a known sale of a similar
     dwelling nearby, brought to today with the provincial index and scaled by
     floor area. When present it is the strongest evidence and sets the centre.

The neighbourhood-average methods (A, B) miss premiums for newer buildings and
good energy labels in mixed neighbourhoods; a comparable sale corrects that.
"""
from __future__ import annotations

import statistics

from . import warehouse as w

# Asking price vs central estimate: within ±BAND is "in line"
BAND = 0.07


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


def estimate(con, address: dict, dwelling: dict, buurt_sizes: dict,
             asking_price: float | None = None, woz_value: float | None = None,
             woz_year: int | None = None, comparable: dict | None = None) -> dict:
    ref = w.one(con, f"select * from {w.MARTS}.mart_neighbourhood_reference where region_code = ?",
                address["buurt_code"])
    if not ref:
        raise ValueError(f"No neighbourhood reference for {address['buurt_code']}")

    m2 = dwelling["floor_area_m2"]
    estimates = []

    if buurt_sizes.get("mean_m2"):
        per_m2 = ref["est_current_avg_sale_price_eur"] / buurt_sizes["mean_m2"]
        estimates.append({
            "method": "neighbourhood_price_per_m2",
            "value": round(per_m2 * m2, -3),
            "inputs": {"neighbourhood_avg_price": ref["est_current_avg_sale_price_eur"],
                       "neighbourhood_mean_m2": buurt_sizes["mean_m2"],
                       "price_per_m2": round(per_m2), "floor_area_m2": m2},
        })

    home_type = "apartment" if dwelling["is_apartment"] else "single_family"
    ratio = _latest_type_ratio(con, home_type)
    if ratio:
        estimates.append({
            "method": "home_type_ratio",
            "value": round(ref["est_current_avg_sale_price_eur"] * ratio["ratio"], -3),
            "inputs": {"neighbourhood_avg_price": ref["est_current_avg_sale_price_eur"],
                       "home_type": home_type, "year": ratio["year"],
                       "national_type_to_all_ratio": round(ratio["ratio"], 3)},
        })

    index_now = _index(con, address["province_code"], "%KW%")
    if woz_value and index_now:
        # WOZ value date is 1 January of the year before the decision year
        ref_year = (woz_year or int(index_now["period_code"][:4])) - 1
        index_then = _index(con, address["province_code"], f"{ref_year - 1}KW04")
        if index_then:
            factor = index_now["price_index_2020"] / index_then["price_index_2020"]
            estimates.append({
                "method": "woz_indexed",
                "value": round(woz_value * factor, -3),
                "inputs": {"woz_value": woz_value, "woz_value_date": f"{ref_year}-01-01",
                           "index_then": index_then, "index_now": index_now,
                           "factor": round(factor, 3)},
            })

    comparable_estimate = None
    if comparable and index_now:
        # Use the sale quarter, or the latest published one if the sale is more recent
        sale_q = _quarter(comparable["date"])
        index_sale = _index(con, address["province_code"], sale_q) if sale_q <= index_now["period_code"] else None
        index_sale = index_sale or index_now
        factor = index_now["price_index_2020"] / index_sale["price_index_2020"]
        size = m2 / (comparable.get("floor_area_m2") or m2)
        comparable_estimate = {
            "method": "comparable_sale",
            "value": round(comparable["price"] * factor * size, -3),
            "inputs": {"sale_price": comparable["price"], "sale_date": comparable["date"],
                       "comparable_m2": comparable.get("floor_area_m2") or m2,
                       "index_sale": index_sale, "index_now": index_now,
                       "factor": round(factor, 3), "price_per_m2": round(comparable["price"] / (comparable.get("floor_area_m2") or m2))},
        }
        estimates.append(comparable_estimate)

    values = [e["value"] for e in estimates]
    central = round(comparable_estimate["value"] if comparable_estimate else statistics.median(values), -3)
    low = round(min(min(values), central * (1 - BAND)), -3)
    high = round(max(max(values), central * (1 + BAND)), -3)

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

    return {
        "estimates": estimates,
        "range": {"low": low, "central": central, "high": high},
        "price_per_m2_central": round(central / m2),
        "central_basis": "comparable_sale" if comparable_estimate else "median_of_estimates",
        "model_vs_comparable_pct": (
            round(100 * (statistics.median([e["value"] for e in estimates if e is not comparable_estimate])
                         / comparable_estimate["value"] - 1), 1)
            if comparable_estimate and len(estimates) > 1 else None),
        "asking_price_check": verdict,
        "neighbourhood_reference": {
            k: ref[k] for k in (
                "avg_woz_value_eur", "municipality_avg_woz_eur", "woz_ratio_to_municipality",
                "municipality_est_current_avg_sale_price_eur", "est_current_avg_sale_price_eur",
                "index_latest_period", "price_change_since_base_year", "price_base_year")
        },
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
