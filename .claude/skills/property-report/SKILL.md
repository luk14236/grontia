---
name: property-report
description: Evaluate a Dutch home from its postcode and house number and publish an English property report artifact. Covers the estimated sale value and why, the estimated rent (market and legal WWS maximum) and why, growth outlook, neighbourhood livability, crime, infrastructure, local news and links to search for similar homes. Use whenever the user gives a Dutch postcode (e.g. "5461KX 9") or address and asks what a home is worth, whether a price or rent is fair, or what the neighbourhood is like.
---

# Grontia property report

Answer two questions for one address, from open Dutch data only:
1. **Sale:** what is it worth, and is the asking price in line?
2. **Rent:** what would it fetch, and is the asked rent within Dutch rules?

Always end with an **English artifact** with a polished UX. Chat replies follow the user's language.
Read `reference/methodology.md` before writing any interpretation.

## 1. Collect the input

Required: postcode + house number (and letter/addition if any). Do not block on anything else;
use what the user already said in the conversation. Useful when known:

| Input | Flag | Where the user finds it |
|---|---|---|
| Asking price | `--asking-price` | the listing |
| Asked or current base rent | `--rent` | contract / listing |
| Contract start date and initial rent | narrative only | contract (decides which rent rule applies) |
| Deposit | `--deposit` | contract |
| WOZ value + decision year | `--woz --woz-year` | wozwaardeloket.nl |
| Energy label | `--energy-label` | ep-online.nl |
| Known sale nearby | `--comparable-price --comparable-date --comparable-m2` | the user |
| Known rents nearby | narrative `rent.comparables` | the user |

Never open listing portals (Funda, Pararius, Kamernet...) or scrape listings, even when the user pastes a link.

## 2. Make sure the data is fresh

The stack lives in `orchestration/airflow` (Docker Compose).

```bash
cd orchestration/airflow
docker compose ps                                   # all services up?
docker compose exec airflow-scheduler airflow dags list-runs 40_silver_and_marts -o plain | head -3
```

If services are down, `docker compose up -d`. If DAG 40's last success is older than two days,
trigger `30_multi_source_daily_ingestion` (DAG 40 follows automatically) and wait for both.

## 3. Run the evaluation

```bash
docker compose exec -T -w /opt/grontia airflow-scheduler \
  /home/airflow/pipeline-venv/bin/python -m evaluation \
  --postcode 5461KX --number 9 [flags from step 1] \
  --out /opt/grontia/warehouse/evaluations/<POSTCODE>-<NUMBER>.json
docker compose cp airflow-scheduler:/opt/grontia/warehouse/evaluations/<POSTCODE>-<NUMBER>.json <scratchpad>/
```

The same run is available as DAG `50_evaluate_address` (form in the Airflow UI).
If a context source fails (crime, notices, headlines), the JSON holds an `error` for it; say so in the report.

## 4. Write the narrative

Create `<scratchpad>/narrative.json` (English). Keep each bullet one plain sentence with a number in it.

```json
{
  "title": "Mgr vdn Tillaartstraat 9",
  "headline": "One-sentence answer to both questions.",
  "sale": {"why": ["...", "..."]},
  "rent": {
    "why": ["...", "..."],
    "comparables": [{"rent": 1299.39, "m2": 88, "label": "Same building, current rent"}],
    "verdict": {"label": "Within the 2023–2026 increase caps", "tone": "good"}
  },
  "growth": {"summary": "..."},
  "neighbourhood": {"summary": "...", "highlights": ["..."], "watch_outs": ["..."]},
  "comparables_note": "Optional sentence about the comparables used.",
  "caveats": ["..."]
}
```

`tone` is `good`, `warn`, `bad` or `neutral`. Cover, at minimum:
- **Sale why:** which methods, the €/m², the comparable and how far the averages were off, the asking-price verdict.
- **Rent why:** market basis (comparables or yield), WWS points and segment, which rule applies for the contract date,
  increase-cap check when there is a rent history (see methodology).
- **Growth:** scenarios, whether the regional trend is accelerating or slowing, local drivers.
- **Neighbourhood:** livability percentile and trend, crime level and what kind, daily needs, notable news or notices.
- **Caveats:** what was assumed or missing and what would sharpen the result (WOZ, label, balcony m²).

## 5. Render and publish the artifact

```bash
python3 .claude/skills/property-report/scripts/render_report.py \
  <scratchpad>/<POSTCODE>-<NUMBER>.json <scratchpad>/narrative.json <scratchpad>/report-<POSTCODE>-<NUMBER>.html
```

Publish the HTML with the Artifact tool (`icon: "house"`, one-sentence description). Look at it once if the
session offers a preview, fix anything broken, then give the user the link. The page shows an address and
possibly the user's own rent: it stays private; tell the user before they share it.

## 6. Reply

In the user's language: the two answers in one or two sentences each, the main neighbourhood point,
what input would sharpen the result, and the artifact link.

## Rules

- Evaluation JSON and reports contain addresses: keep them in the scratchpad or the warehouse volume, never in git.
- Rent rules and caps change every year: check `evaluation/rules/wws_<year>.yml` and the cap table in
  `reference/methodology.md` are current for today's date; if not, update them from the Huurcommissie
  and rijksoverheid.nl before reporting.
- Never present a scenario as a forecast or an estimate as a valuation report (taxatierapport).
