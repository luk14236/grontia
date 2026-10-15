# Methodology and interpretation rules

Read this before writing the narrative. Every number in the report comes from
the evaluation JSON or from `render_report.py`; the narrative explains them.

## Sale value

`purchase.estimates` holds up to four methods:

| Method | What it is | Known bias |
|---|---|---|
| `neighbourhood_price_per_m2` | Neighbourhood's current average price ÷ mean floor area of its dwellings (BAG) × this floor area | Ignores age, label and finish. **Underestimated a 2011 A+ apartment in a mixed town centre by ~20%** (5461KX, Oct 2026). |
| `home_type_ratio` | Neighbourhood average × national price ratio of this home type to all homes (CBS 85791NED) | Ignores size; national ratio. |
| `woz_indexed` | User's WOZ value brought to today with the provincial price index | Close to market, lags about a year. |
| `comparable_sale` | Known sale of a similar dwelling nearby, indexed and scaled by m² | Best evidence when truly similar (same building/street, size, age). Sets the centre. |

- Without a comparable or WOZ, say plainly that the value is a **model estimate from neighbourhood averages** and
  that newer or better-labelled homes than the neighbourhood typical usually sell above it.
- Check `model_vs_comparable_pct`: when a comparable exists, report how far the averages were off.
- When the user gives an asking price, `asking_price_check.verdict` is `below_range`, `in_range` or `above_range`.

## Rent: two separate questions

1. **Market rent** (what it fetches): `render_report.py` uses rent comparables from the narrative
   (`rent.comparables`, median €/m²) when given, otherwise sale estimate × gross yield 3.5% / 4.0% / 4.5% ÷ 12.
   Always prefer a real rent comparable; label the yield method as a heuristic.
2. **Legal maximum** (what the law allows): `rent.scenarios` low / typical / high under the WWS valid from
   1 January 2026 (Huurcommissie policy book). Segments: ≤143 points social, 144–186 regulated middle,
   ≥187 free sector (no initial maximum). The range reflects unknowns (room area, kitchen, bathroom,
   balcony, WOZ, label); list `rent.assumptions` in the report.

**Contract date changes the rule.** Ask (or use what the user said):

| Contract start | Rule that limits the rent |
|---|---|
| From 1 July 2024 | WWS points: social and middle segments have a legal maximum (Wet betaalbare huur). |
| Before 1 July 2024, initial rent above that year's liberalisation limit | Free-sector contract. The new middle-segment maximum does **not** apply retroactively. Only the yearly increase is capped. |

Free-sector yearly increase caps (Wet maximering huurprijsverhogingen geliberaliseerde
huurovereenkomsten): **2023 4.1%, 2024 5.5%, 2025 4.1%, 2026 4.4%**. At most one increase per 12 months.
To check a current rent, compound the caps over the years with an increase and compare.
Example: €1,100 in 2022 → maximum €1,312.95 after four increases (2023–2026).
Update this table every December (rijksoverheid.nl publishes the next year's cap).

## Growth outlook

`render_report.py` shows three scenarios over five years, never one forecast:
base = mean year-on-year change of the provincial price index over the last four quarters;
high = the municipality's 5-year compound growth of average sale prices; low = half of base.
In the narrative, say whether growth is speeding up or slowing (compare the latest quarters)
and name local drivers seen in the data (livability trend, new construction share, notices).

## Neighbourhood

- **Livability** (`neighbourhood.livability`, Leefbaarometer 3.0): report the national percentile and
  its trend since 2014/2020; the five dimensions are deviations from the national average
  (positive = better, `safety` positive = safer).
- **Crime** (`neighbourhood.crime`): per 1,000 residents vs municipality and NL, the 12-month change,
  and the top types. Town centres score high per resident because shops and nightlife draw visitors.
  Say so when the top types are bike theft, shoplifting or traffic offences rather than burglary or violence.
- **Infrastructure**: distances to supermarket, GP, school and daycare vs municipality and NL; urbanity class.
- **News**: official notices on the street first (permits, demolition, events), then the postcode area;
  press headlines as title + source + link only. Summarise what could affect living there; do not quote articles.

## Similar homes

Grontia never fetches listing portals (Funda, Pararius, Kamernet...). The report links to **search pages**
built from the city and a ±15% floor-area band, and to WOZ-waardeloket / EP-Online for the user to check.
Comparables the user gives (sales or rents) go into the evaluation (`--comparable-*`) or the narrative.
