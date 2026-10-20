"""
Five-year outlook for one home: value growth scenarios, market rent, gross
yield and what buying it could return, as owner-occupier or as landlord.

Shared by the CLI/web result (`run()` adds it as "outlook") and by the
property-report skill, so both show the same numbers. Scenarios, never a
forecast; the method is in .claude/skills/property-report/reference/methodology.md.
Pure functions on the evaluation result: no I/O besides the rules file.
"""
from __future__ import annotations

import statistics
from pathlib import Path

import yaml

YEARS = 5
# Gross yield band (annual rent / value) that turns a sale value into a
# market-rent estimate when no real rent comparable is known.
YIELD_LOW, YIELD_BASE, YIELD_HIGH = 0.035, 0.040, 0.045
# Rent is regulated for these WWS segments on contracts from 1 July 2024
REGULATED_SEGMENTS = ("social", "middle")

COSTS = yaml.safe_load((Path(__file__).parent / "rules" / "costs_2026.yml").read_text())


def growth_scenarios(appreciation: dict) -> dict:
    """Yearly % growth: base = mean YoY change of the provincial price index over
    the last four quarters; high = the municipality's 5-year CAGR; low = half of base."""
    quarters = [q["price_index_yoy_pct"] for q in appreciation.get("province_index_recent_quarters", [])[:4]
                if q.get("price_index_yoy_pct") is not None]
    base = statistics.mean(quarters) if quarters else 3.0
    high = max(appreciation.get("municipality_avg_sale_price_cagr_5y_pct") or base * 1.5, base)
    trend = None
    if len(quarters) >= 2:
        trend = "speeding_up" if quarters[0] > quarters[-1] else "slowing" if quarters[0] < quarters[-1] else "steady"
    return {"low": round(base / 2, 2), "base": round(base, 2), "high": round(high, 2),
            "latest_quarter_yoy": quarters[0] if quarters else None, "trend": trend}


def market_rent(sale_value: float, floor_area_m2: float, comparables: list[dict] | None = None) -> dict:
    """Monthly market rent band: median €/m² of real rent comparables when given
    ([{"rent": 1299, "m2": 88}]), otherwise sale value × gross yield."""
    if comparables:
        central = statistics.median(c["rent"] / c["m2"] for c in comparables) * floor_area_m2
        low, high, basis = central * 0.95, central * 1.05, "rent_comparables"
    else:
        low, central, high = (sale_value * y / 12 for y in (YIELD_LOW, YIELD_BASE, YIELD_HIGH))
        basis = "sale_value_x_gross_yield"
    return {"low": round(low, -1), "central": round(central, -1), "high": round(high, -1),
            "per_m2": round(central / floor_area_m2, 2) if floor_area_m2 else None, "basis": basis}


def value_path(start: float, rate_pct: float, years: int = YEARS) -> list[float]:
    return [round(start * (1 + rate_pct / 100) ** y, -2) for y in range(years + 1)]


def build(result: dict, rent_comparables: list[dict] | None = None) -> dict:
    purchase, rent, dwelling = result["purchase"], result["rent"], result["dwelling"]
    value = purchase["range"]["central"]
    asked = (purchase.get("asking_price_check") or {}).get("asking_price")
    price = asked or value
    growth = growth_scenarios(purchase["appreciation"])
    paths = {k: value_path(value, growth[k]) for k in ("low", "base", "high")}

    market = market_rent(value, dwelling.get("floor_area_m2"), rent_comparables)
    typical = rent["scenarios"]["typical"]
    regulated = typical["segment"] in REGULATED_SEGMENTS
    # a landlord can charge the market rent only up to the legal maximum
    landlord_rent = min(market["central"], typical["max_rent_eur"]) if regulated else market["central"]

    def returns(tax_rate: float, monthly_rent: float = 0.0) -> dict:
        tax = round(price * tax_rate, -2)
        rent_income = round(monthly_rent * 12 * YEARS, -2)
        out = {}
        for k, path in paths.items():
            gain = path[-1] - price - tax + rent_income
            out[k] = {"value_after": path[-1], "total_return": round(gain, -2),
                      "total_return_pct": round(100 * gain / (price + tax), 1)}
        return {"transfer_tax": tax, "transfer_tax_rate": tax_rate, "rent_income": rent_income, "scenarios": out}

    return {
        "years": YEARS,
        "price_basis": "asking_price" if asked else "estimated_value",
        "price": price,
        "growth_pct_per_year": growth,
        "value_paths": paths,
        "market_rent": market,
        "legal_max_rent": typical["max_rent_eur"],
        "rent_regulated": regulated,
        "landlord_monthly_rent": round(landlord_rent, -1),
        "gross_yield_pct": round(100 * landlord_rent * 12 / price, 2),
        "owner_occupier": returns(COSTS["transfer_tax"]["owner_occupier"]),
        "landlord": returns(COSTS["transfer_tax"]["investor"], landlord_rent),
        "starter_exemption_max_value": COSTS["starter_exemption_max_value"],
        "costs_valid_from": COSTS["valid_from"],
        "excluded": ["notary_and_valuation", "mortgage_interest", "maintenance", "service_charges",
                     "vacancy", "income_and_box3_tax", "selling_costs"],
    }
