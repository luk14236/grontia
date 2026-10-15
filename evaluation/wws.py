"""
Rent question: points under the woningwaarderingsstelsel (WWS) for a
self-contained dwelling, the rent segment and the legal maximum rent.

Anything the user does not supply is filled with an explicit assumption, and a
low / high scenario shows how far those assumptions can move the result. Only
the Huurcommissie gives a binding ruling.
"""
from __future__ import annotations

import csv
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml

RULES_DIR = Path(__file__).parent / "rules"


def load_rules(year: int = 2026) -> tuple[dict, dict[int, float]]:
    rules = yaml.safe_load((RULES_DIR / f"wws_{year}.yml").read_text(encoding="utf-8"))
    with open(RULES_DIR / f"max_rent_{year}.csv", encoding="utf-8") as f:
        table = {int(r["points"]): float(r["max_rent_eur"]) for r in csv.DictReader(f)}
    return rules, table


@dataclass
class DwellingFacts:
    """What the WWS needs. None means unknown and gets an assumption."""
    floor_area_m2: float                     # BAG gebruiksoppervlakte
    build_year: int
    is_apartment: bool
    rooms_m2: float | None = None            # rubriek 1 (WWS measurement, smaller than BAG)
    other_spaces_m2: float | None = None     # rubriek 2 (storage, attic...)
    heated_rooms: int | None = None
    heated_other_spaces: int | None = None
    energy_label: str | None = None          # valid label from EP-Online, e.g. "A"
    worktop_m: float | None = None
    kitchen_extras_points: float | None = None
    toilet_in_toilet_room: bool | None = None
    washbasins: int | None = None
    shower: bool | None = None
    bath: bool | None = None
    sanitary_extras_points: float | None = None
    private_outdoor_m2: float | None = None
    woz_value: float | None = None           # WOZ decision with value date 1 Jan 2025
    monument: bool = False
    assumptions: list[str] = field(default_factory=list)


def _round_half_up(x: float) -> int:
    return int(math.floor(x + 0.5))


def max_rent(points: int, table: dict[int, float]) -> float:
    lo, hi = min(table), max(table)
    if points <= lo:
        return table[lo]
    if points <= hi:
        return table[points]
    step = table[hi] - table[hi - 1]
    return round(table[hi] + (points - hi) * step, 2)


def segment(points: int, rules: dict) -> str:
    if points <= rules["segments"]["social_max"]:
        return "social"
    if points <= rules["segments"]["middle_max"]:
        return "middle"
    return "free"


def score(f: DwellingFacts, rules: dict, table: dict[int, float]) -> dict:
    r = {}
    rooms_m2 = round(f.rooms_m2)
    other_m2 = round(f.other_spaces_m2 or 0)
    r["1_rooms"] = rooms_m2 * rules["rooms_points_per_m2"]
    r["2_other_spaces"] = other_m2 * rules["other_spaces_points_per_m2"]

    h = rules["heating"]
    r["3_heating"] = (f.heated_rooms * h["per_heated_room"]
                      + min(f.heated_other_spaces * h["per_heated_other_space"], h["other_spaces_max"]))

    kind = "multi_family" if f.is_apartment else "single_family"
    if f.energy_label:
        energy = rules["energy_label"][kind][f.energy_label.upper()]
    else:
        col = 2 if f.is_apartment else 1
        energy = next(row[col] for row in rules["energy_by_build_year"] if f.build_year >= row[0])
    if f.monument and energy < 0:
        energy = 0
    r["4_energy"] = energy

    k = rules["kitchen"]
    base = k["under_1m"] if f.worktop_m < 1 else k["from_1_to_2m"] if f.worktop_m <= 2 else k["over_2m"]
    r["5_kitchen"] = base + min(f.kitchen_extras_points, base)

    s = rules["sanitary"]
    sanitary = (s["toilet_in_toilet_room"] if f.toilet_in_toilet_room else s["toilet_in_bathroom"])
    sanitary += f.washbasins * s["washbasin"]
    shower_bath = (s["bath_with_shower"] if f.bath and f.shower
                   else s["bath"] if f.bath else s["shower"] if f.shower else 0)
    sanitary += shower_bath + min(f.sanitary_extras_points, shower_bath)
    r["6_sanitary"] = sanitary

    o = rules["outdoor"]
    if f.private_outdoor_m2:
        r["8_outdoor"] = min(o["private_presence"] + f.private_outdoor_m2 * o["private_per_m2"], o["max"])
    else:
        r["8_outdoor"] = o["none"]

    wz = rules["woz"]
    woz = max(f.woz_value, wz["minimum_woz"])
    wws_m2 = rooms_m2 + other_m2
    woz_points = woz / wz["euro_per_point"] + (woz / wws_m2) / wz["euro_per_m2_per_point"]

    non_woz = sum(r.values())
    total = _round_half_up(non_woz + woz_points)
    capped = False
    if 2015 <= f.build_year <= 2019 and non_woz >= 110:
        # New builds 2015-2019: at least 40 WOZ points and no cap (2.11.5)
        woz_points = max(woz_points, wz["new_build_2015_2019_min_points"])
        total = _round_half_up(non_woz + woz_points)
    elif f.woz_value > wz["minimum_woz"] and total > rules["segments"]["middle_max"]:
        # WOZ may be at most 33% of the total; never pushes the total below 186 (2.11.7)
        max_woz = math.floor(non_woz * wz["cap_share"] / (1 - wz["cap_share"]))
        if woz_points > max_woz:
            woz_points, capped = max_woz, True
            total = max(_round_half_up(non_woz + woz_points), wz["cap_floor_total"])
    r["11_woz"] = round(woz_points, 2)

    return {
        "points_by_rubric": {k: round(v, 2) for k, v in r.items()},
        "total_points": total,
        "woz_cap_applied": capped,
        "segment": segment(total, rules),
        "max_rent_eur": max_rent(total, table),
    }


# Defaults for anything unknown, as (low, typical, high) scenarios
def _scenarios(facts: DwellingFacts, woz_range: tuple[float, float, float] | None) -> dict[str, DwellingFacts]:
    out = {}
    ga = facts.floor_area_m2
    rooms_est = max(2, round(ga / 22))
    for name, k in (("low", 0), ("typical", 1), ("high", 2)):
        d = DwellingFacts(**{**asdict(facts), "assumptions": []})

        def fill(attr, values, note):
            if getattr(d, attr) is None:
                setattr(d, attr, values[k])
                if name == "typical":
                    facts.assumptions.append(f"{attr}: {values[1]} ({note})")
        fill("rooms_m2", (0.75 * ga, 0.82 * ga, 0.9 * ga), "WWS room area is smaller than BAG floor area")
        fill("other_spaces_m2", (0, 0, 6), "storage inside the dwelling")
        fill("heated_rooms", (rooms_est - 1, rooms_est, rooms_est + 1), f"~1 room per 22 m² of {ga} m²")
        fill("heated_other_spaces", (1, 2, 3), "hall and bathroom")
        fill("worktop_m", (1.5, 1.8, 2.4), "standard kitchen")
        fill("kitchen_extras_points", (1, 3, 4), "hob, extractor, oven, fridge")
        fill("toilet_in_toilet_room", (False, True, True), "separate toilet")
        fill("washbasins", (1, 1, 2), "one washbasin")
        fill("shower", (True, True, True), "shower")
        fill("bath", (False, False, True), "no bath")
        fill("sanitary_extras_points", (0, 1, 3), "shower screen, towel radiator")
        fill("private_outdoor_m2", (0, 8, 15), "balcony")
        if woz_range:
            fill("woz_value", woz_range, "estimated from the purchase valuation at the WOZ value date")
        if d.energy_label is None and name == "high":
            d.energy_label = "A+" if d.build_year >= 2002 else None
        out[name] = d
    if facts.energy_label is None:
        facts.assumptions.append("energy_label: unknown, scored by build year (official rule without a valid label)")
    return out


def assess(facts: DwellingFacts, asked_rent: float | None = None,
           deposit: float | None = None, woz_range: tuple[float, float, float] | None = None,
           year: int = 2026) -> dict:
    rules, table = load_rules(year)
    results = {name: score(d, rules, table) for name, d in _scenarios(facts, woz_range).items()}
    typical = results["typical"]

    check = None
    if asked_rent:
        limit = typical["max_rent_eur"]
        if typical["segment"] == "free" and results["low"]["segment"] == "free":
            verdict = "free_sector"          # no legal maximum at the start of the contract
        elif asked_rent <= results["low"]["max_rent_eur"]:
            verdict = "within_limit"
        elif asked_rent <= results["high"]["max_rent_eur"]:
            verdict = "depends_on_details"
        else:
            verdict = "above_limit"
        check = {
            "asked_rent": asked_rent,
            "max_rent_typical": limit,
            "difference_eur": round(asked_rent - limit, 2),
            "verdict": verdict,
        }
        if deposit is not None:
            check["deposit_ok"] = deposit <= rules["deposit_max_months"] * asked_rent

    return {
        "rules_valid_from": rules["valid_from"],
        "rules_source": rules["source"],
        "scenarios": results,
        "assumptions": facts.assumptions,
        "rent_check": check,
        "segment_limits": {
            "social_max_points": rules["segments"]["social_max"],
            "middle_max_points": rules["segments"]["middle_max"],
            "social_max_rent": max_rent(rules["segments"]["social_max"], table),
            "middle_max_rent": max_rent(rules["segments"]["middle_max"], table),
        },
    }
