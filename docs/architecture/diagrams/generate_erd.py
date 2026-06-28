"""
Gera o diagrama ERD do projeto Grontia como imagem PNG.
Uso: python generate_erd.py
Saída: erd_grontia.png no mesmo diretório
"""

import os
import graphviz

OUTPUT = os.path.join(os.path.dirname(__file__), "erd_grontia")

SILVER = "#2E86AB"
STAGING = "#A23B72"
MARTS = "#F18F01"
TEXT_LIGHT = "white"
TEXT_DARK = "white"


def table(dot, name: str, columns: list[tuple], color: str, schema_label: str):
    """Renderiza uma tabela como nó HTML no graphviz."""
    header = (
        f'<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="6">'
        f'<TR><TD COLSPAN="3" BGCOLOR="{color}" ALIGN="CENTER">'
        f'<FONT COLOR="{TEXT_LIGHT}" FACE="Helvetica-Bold" POINT-SIZE="11">'
        f'[{schema_label}]<BR/>{name}</FONT></TD></TR>'
        f'<TR>'
        f'<TD BGCOLOR="#444444"><FONT COLOR="white" FACE="Helvetica-Bold" POINT-SIZE="9">PK</FONT></TD>'
        f'<TD BGCOLOR="#444444"><FONT COLOR="white" FACE="Helvetica-Bold" POINT-SIZE="9">Coluna</FONT></TD>'
        f'<TD BGCOLOR="#444444"><FONT COLOR="white" FACE="Helvetica-Bold" POINT-SIZE="9">Tipo</FONT></TD>'
        f'</TR>'
    )
    rows = ""
    for pk, col, dtype in columns:
        pk_mark = "🔑" if pk else ""
        bg = "#f0f0f0" if not pk else "#fffbe6"
        rows += (
            f'<TR>'
            f'<TD BGCOLOR="{bg}"><FONT POINT-SIZE="9">{pk_mark}</FONT></TD>'
            f'<TD BGCOLOR="{bg}" ALIGN="LEFT"><FONT POINT-SIZE="9">{col}</FONT></TD>'
            f'<TD BGCOLOR="{bg}" ALIGN="LEFT"><FONT COLOR="#555555" POINT-SIZE="9">{dtype}</FONT></TD>'
            f'</TR>'
        )
    label = f"<{header}{rows}</TABLE>>"
    dot.node(name, label=label, shape="none", margin="0")


def edge(dot, src, dst, label="", style="solid", color="#555555"):
    dot.edge(src, dst, label=label, style=style, color=color,
             fontsize="8", fontcolor="#333333", arrowhead="vee", arrowtail="none")


def build():
    dot = graphviz.Digraph(
        "erd_grontia",
        comment="Grontia ERD",
        format="png",
    )
    dot.attr(
        rankdir="LR",
        splines="ortho",
        bgcolor="#1a1a2e",
        fontname="Helvetica",
        pad="0.5",
        nodesep="0.6",
        ranksep="1.2",
    )
    dot.attr("node", shape="none", fontname="Helvetica")
    dot.attr("edge", fontname="Helvetica")

    # ------------------------------------------------------------------
    # SILVER tables
    # ------------------------------------------------------------------
    with dot.subgraph(name="cluster_silver") as s:
        s.attr(label="SCHEMA: SILVER  (Parquet via PySpark → MinIO)",
               style="filled", fillcolor="#0d1b2a", color=SILVER,
               fontcolor=SILVER, fontsize="12", penwidth="2")

        table(s, "CBS_NEIGHBOURHOOD_KEY_FIGURES", [
            (True,  "RegioS",                       "VARCHAR"),
            (True,  "Perioden",                     "VARCHAR"),
            (False, "RegioNaam",                    "VARCHAR"),
            (False, "SoortRegio_2",                 "VARCHAR"),
            (False, "Inwoners_5",                   "INTEGER"),
            (False, "AantalHuishoudens_10",          "INTEGER"),
            (False, "GemiddeldInkomenPerInwoner_25", "DOUBLE"),
            (False, "ingestion_date",               "DATE"),
        ], SILVER, "SILVER")

        table(s, "CBS_AVERAGE_WOZ_VALUE", [
            (True,  "RegioS",                 "VARCHAR"),
            (True,  "Perioden",               "VARCHAR"),
            (False, "RegioNaam",              "VARCHAR"),
            (False, "GemiddeldeWOZWaarde_1",  "DOUBLE"),
            (False, "ingestion_date",         "DATE"),
        ], SILVER, "SILVER")

        table(s, "CBS_HOUSING_STOCK", [
            (True,  "RegioS",           "VARCHAR"),
            (True,  "Perioden",         "VARCHAR"),
            (True,  "TypeWoning_3",     "VARCHAR"),
            (False, "RegioNaam",        "VARCHAR"),
            (False, "AantalWoningen_1", "INTEGER"),
            (False, "ingestion_date",   "DATE"),
        ], SILVER, "SILVER")

        table(s, "CBS_HOUSEHOLD_INCOME", [
            (True,  "RegioS",                        "VARCHAR"),
            (True,  "Perioden",                      "VARCHAR"),
            (False, "RegioNaam",                     "VARCHAR"),
            (False, "GemiddeldInkomenHuishouden_6",  "DOUBLE"),
            (False, "MediaanInkomenHuishouden_7",    "DOUBLE"),
            (False, "ingestion_date",                "DATE"),
        ], SILVER, "SILVER")

        table(s, "PDOK_BAG_PAND", [
            (True,  "id",            "VARCHAR"),
            (False, "geometry",      "VARCHAR"),
            (False, "identificatie", "VARCHAR"),
            (False, "status",        "VARCHAR"),
            (False, "bouwjaar",      "INTEGER"),
            (False, "ingestion_date","DATE"),
        ], SILVER, "SILVER")

        table(s, "PDOK_BAG_VERBLIJFSOBJECT", [
            (True,  "id",            "VARCHAR"),
            (False, "geometry",      "VARCHAR"),
            (False, "gebruiksdoel",  "VARCHAR"),
            (False, "oppervlakte",   "DOUBLE"),
            (False, "status",        "VARCHAR"),
            (False, "ingestion_date","DATE"),
        ], SILVER, "SILVER")

        table(s, "KNMI_DAILY_WEATHER", [
            (True,  "STN",           "VARCHAR"),
            (True,  "YYYYMMDD",      "VARCHAR"),
            (False, "TG",            "DOUBLE"),
            (False, "TN",            "DOUBLE"),
            (False, "TX",            "DOUBLE"),
            (False, "RH",            "DOUBLE"),
            (False, "FG",            "DOUBLE"),
            (False, "ingestion_date","DATE"),
        ], SILVER, "SILVER")

        table(s, "NDW_TRAFFIC_FLOW", [
            (True,  "id",                    "VARCHAR"),
            (False, "element_type",          "VARCHAR"),
            (False, "measurementSiteRef",    "VARCHAR"),
            (False, "period",                "VARCHAR"),
            (False, "vehicleFlowRate",       "INTEGER"),
            (False, "averageVehicleSpeed",   "DOUBLE"),
            (False, "ingestion_date",        "DATE"),
        ], SILVER, "SILVER")

    # ------------------------------------------------------------------
    # STAGING views
    # ------------------------------------------------------------------
    with dot.subgraph(name="cluster_staging") as s:
        s.attr(label="SCHEMA: STAGING  (dbt views)",
               style="filled", fillcolor="#0d1b2a", color=STAGING,
               fontcolor=STAGING, fontsize="12", penwidth="2")

        table(s, "STG_CBS_NEIGHBOURHOOD_KEY_FIGURES", [
            (True,  "region_code",              "VARCHAR"),
            (True,  "period_code",              "VARCHAR"),
            (False, "region_name",              "VARCHAR"),
            (False, "region_type",              "VARCHAR"),
            (False, "population",               "INTEGER"),
            (False, "households",               "INTEGER"),
            (False, "avg_income_per_resident_eur","DOUBLE"),
        ], STAGING, "STAGING")

        table(s, "STG_CBS_AVERAGE_WOZ_VALUE", [
            (True,  "region_code",     "VARCHAR"),
            (True,  "period_code",     "VARCHAR"),
            (False, "region_name",     "VARCHAR"),
            (False, "avg_woz_value_eur","DOUBLE"),
        ], STAGING, "STAGING")

        table(s, "STG_CBS_HOUSING_STOCK", [
            (True,  "region_code",        "VARCHAR"),
            (True,  "period_code",        "VARCHAR"),
            (True,  "dwelling_type",      "VARCHAR"),
            (False, "region_name",        "VARCHAR"),
            (False, "number_of_dwellings","INTEGER"),
        ], STAGING, "STAGING")

        table(s, "STG_CBS_HOUSEHOLD_INCOME", [
            (True,  "region_code",                 "VARCHAR"),
            (True,  "period_code",                 "VARCHAR"),
            (False, "region_name",                 "VARCHAR"),
            (False, "avg_household_income_eur",    "DOUBLE"),
            (False, "median_household_income_eur", "DOUBLE"),
        ], STAGING, "STAGING")

        table(s, "STG_PDOK_BAG_PAND", [
            (True,  "id",            "VARCHAR"),
            (False, "geometry",      "VARCHAR"),
            (False, "identificatie", "VARCHAR"),
            (False, "status",        "VARCHAR"),
            (False, "bouwjaar",      "INTEGER"),
        ], STAGING, "STAGING")

        table(s, "STG_PDOK_BAG_VERBLIJFSOBJECT", [
            (True,  "id",           "VARCHAR"),
            (False, "geometry",     "VARCHAR"),
            (False, "gebruiksdoel", "VARCHAR"),
            (False, "oppervlakte",  "DOUBLE"),
            (False, "status",       "VARCHAR"),
        ], STAGING, "STAGING")

        table(s, "STG_KNMI_DAILY_WEATHER", [
            (True,  "station_code", "VARCHAR"),
            (True,  "date",         "DATE"),
            (False, "temp_avg",     "DOUBLE"),
            (False, "temp_min",     "DOUBLE"),
            (False, "temp_max",     "DOUBLE"),
            (False, "precipitation","DOUBLE"),
            (False, "wind_speed",   "DOUBLE"),
        ], STAGING, "STAGING")

        table(s, "STG_NDW_TRAFFIC_FLOW", [
            (True,  "id",                  "VARCHAR"),
            (False, "element_type",        "VARCHAR"),
            (False, "measurement_site_ref","VARCHAR"),
            (False, "vehicle_flow_rate",   "INTEGER"),
            (False, "avg_vehicle_speed",   "DOUBLE"),
        ], STAGING, "STAGING")

    # ------------------------------------------------------------------
    # MARTS tables
    # ------------------------------------------------------------------
    with dot.subgraph(name="cluster_marts") as s:
        s.attr(label="SCHEMA: MARTS  (dbt tables — entrega final)",
               style="filled", fillcolor="#0d1b2a", color=MARTS,
               fontcolor=MARTS, fontsize="12", penwidth="2")

        table(s, "MART_REGIONAL_DASHBOARD", [
            (True,  "region_code",                 "VARCHAR"),
            (True,  "period_code",                 "VARCHAR"),
            (False, "region_name",                 "VARCHAR"),
            (False, "region_type",                 "VARCHAR"),
            (False, "population",                  "INTEGER"),
            (False, "households",                  "INTEGER"),
            (False, "avg_income_per_resident_eur", "DOUBLE"),
            (False, "avg_woz_value_eur",           "DOUBLE"),
            (False, "avg_household_income_eur",    "DOUBLE"),
            (False, "median_household_income_eur", "DOUBLE"),
            (False, "last_ingested_date",          "DATE"),
        ], MARTS, "MARTS")

        table(s, "MART_HOUSING_MARKET", [
            (True,  "region_code",       "VARCHAR"),
            (True,  "period_code",       "VARCHAR"),
            (True,  "dwelling_type",     "VARCHAR"),
            (False, "region_name",       "VARCHAR"),
            (False, "number_of_dwellings","INTEGER"),
            (False, "avg_woz_value_eur", "DOUBLE"),
            (False, "last_ingested_date","DATE"),
        ], MARTS, "MARTS")

        table(s, "MART_WEATHER_SUMMARY", [
            (True,  "station_code",   "VARCHAR"),
            (True,  "date",           "DATE"),
            (False, "temp_avg",       "DOUBLE"),
            (False, "temp_min",       "DOUBLE"),
            (False, "temp_max",       "DOUBLE"),
            (False, "precipitation",  "DOUBLE"),
            (False, "wind_speed",     "DOUBLE"),
        ], MARTS, "MARTS")

    # ------------------------------------------------------------------
    # Edges: Silver → Staging
    # ------------------------------------------------------------------
    silver_to_staging = [
        ("CBS_NEIGHBOURHOOD_KEY_FIGURES", "STG_CBS_NEIGHBOURHOOD_KEY_FIGURES"),
        ("CBS_AVERAGE_WOZ_VALUE",         "STG_CBS_AVERAGE_WOZ_VALUE"),
        ("CBS_HOUSING_STOCK",             "STG_CBS_HOUSING_STOCK"),
        ("CBS_HOUSEHOLD_INCOME",          "STG_CBS_HOUSEHOLD_INCOME"),
        ("PDOK_BAG_PAND",                 "STG_PDOK_BAG_PAND"),
        ("PDOK_BAG_VERBLIJFSOBJECT",      "STG_PDOK_BAG_VERBLIJFSOBJECT"),
        ("KNMI_DAILY_WEATHER",            "STG_KNMI_DAILY_WEATHER"),
        ("NDW_TRAFFIC_FLOW",              "STG_NDW_TRAFFIC_FLOW"),
    ]
    for src, dst in silver_to_staging:
        edge(dot, src, dst, style="dashed", color=SILVER)

    # Edges: Staging → Marts
    staging_to_mart = [
        ("STG_CBS_NEIGHBOURHOOD_KEY_FIGURES", "MART_REGIONAL_DASHBOARD"),
        ("STG_CBS_AVERAGE_WOZ_VALUE",         "MART_REGIONAL_DASHBOARD"),
        ("STG_CBS_HOUSEHOLD_INCOME",          "MART_REGIONAL_DASHBOARD"),
        ("STG_CBS_HOUSING_STOCK",             "MART_HOUSING_MARKET"),
        ("STG_CBS_AVERAGE_WOZ_VALUE",         "MART_HOUSING_MARKET"),
        ("STG_KNMI_DAILY_WEATHER",            "MART_WEATHER_SUMMARY"),
    ]
    for src, dst in staging_to_mart:
        edge(dot, src, dst, color=MARTS)

    dot.render(OUTPUT, cleanup=True)
    print(f"ERD gerado: {OUTPUT}.png")


if __name__ == "__main__":
    build()
