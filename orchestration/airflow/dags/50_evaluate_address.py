"""
Evaluate one address on demand: purchase price check, rent check (WWS) and
neighbourhood context. Trigger from the UI with the form below; the JSON lands
in the warehouse volume (/opt/grontia/warehouse/evaluations), never in git.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

ARGS = [
    ("postcode", "--postcode"), ("number", "--number"), ("suffix", "--suffix"),
    ("asking_price", "--asking-price"), ("rent", "--rent"), ("deposit", "--deposit"),
    ("woz", "--woz"), ("woz_year", "--woz-year"), ("energy_label", "--energy-label"),
    ("comparable_price", "--comparable-price"), ("comparable_date", "--comparable-date"),
    ("comparable_m2", "--comparable-m2"),
]

# Each param becomes a CLI flag only when filled in
CLI_FLAGS = " ".join(
    f"{{% if params.{name} %}}{flag} {{{{ params.{name} | string | quote }}}}{{% endif %}}"
    for name, flag in ARGS
)

with DAG(
    dag_id="50_evaluate_address",
    description="Purchase and rent check for one address, from open data",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    render_template_as_native_obj=False,
    user_defined_filters={"quote": lambda s: "'" + str(s).replace("'", "'\\''") + "'"},
    params={
        "postcode": Param("", type="string", title="Postcode", description="e.g. 1234AB"),
        "number": Param(1, type="integer", title="House number"),
        "suffix": Param(None, type=["null", "string"], title="Letter / addition"),
        "asking_price": Param(None, type=["null", "number"], title="Asking price (€)"),
        "rent": Param(None, type=["null", "number"], title="Asked base rent per month (€)"),
        "deposit": Param(None, type=["null", "number"], title="Deposit (€)"),
        "woz": Param(None, type=["null", "number"], title="WOZ value (€)", description="wozwaardeloket.nl"),
        "woz_year": Param(None, type=["null", "integer"], title="Year of that WOZ decision"),
        "energy_label": Param(None, type=["null", "string"], title="Energy label", description="ep-online.nl"),
        "comparable_price": Param(None, type=["null", "number"], title="Comparable sale price (€)",
                                  description="Known sale of a similar dwelling nearby"),
        "comparable_date": Param(None, type=["null", "string"], title="Comparable sale date", description="YYYY-MM"),
        "comparable_m2": Param(None, type=["null", "number"], title="Comparable floor area (m²)"),
    },
    tags=["grontia", "evaluation"],
) as dag:

    BashOperator(
        task_id="evaluate",
        bash_command=f"cd /opt/grontia && $PIPELINE_VENV/bin/python -m evaluation {CLI_FLAGS}",
    )
