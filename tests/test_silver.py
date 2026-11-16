"""Tests de plata con PySpark local: cada regla de limpieza tiene al menos un caso."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.glue_jobs import silver as s

pytestmark = pytest.mark.spark

T0 = datetime(2026, 11, 15, 10, 0, 0)
T1 = datetime(2026, 11, 16, 10, 0, 0)
SILVER_AT = datetime(2026, 11, 16, 12, 0, 0)

BASE = {
    "flightdate": "2026-08-01",
    "year": "2026",
    "quarter": "3",
    "month": "8",
    "dayofmonth": "1",
    "dayofweek": "6",
    "reporting_airline": "AA",
    "dot_id_reporting_airline": "19805",
    "iata_code_reporting_airline": "AA",
    "tail_number": "N1AA",
    "flight_number_reporting_airline": "100",
    "originairportid": "12892",
    "origin": "LAX",
    "origincityname": "Los Angeles, CA",
    "originstate": "CA",
    "destairportid": "12478",
    "dest": "JFK",
    "destcityname": "New York, NY",
    "deststate": "NY",
    "crsdeptime": "0530",
    "crsarrtime": "1400",
    "crselapsedtime": "330.00",
    "distance": "2475.00",
    "distancegroup": "10",
    "deptime": "0535",
    "depdelay": "5.00",
    "depdel15": "0.00",
    "taxiout": "15.00",
    "taxiin": "8.00",
    "arrtime": "1405",
    "arrdelay": "5.00",
    "arrdel15": "0.00",
    "actualelapsedtime": "330.00",
    "airtime": "307.00",
    "carrierdelay": None,
    "weatherdelay": None,
    "nasdelay": None,
    "securitydelay": None,
    "lateaircraftdelay": None,
    "cancelled": "0.00",
    "diverted": "0.00",
    "cancellationcode": None,
}
BRONZE_COLS = [src for src, _, _ in s.COLUMNS]


def bronze_df(spark, rows: list[dict]):
    data = []
    for i, overrides in enumerate(rows):
        r = {**BASE, **overrides}
        data.append(
            tuple(r[c] for c in BRONZE_COLS)
            + (
                r.get("_source_file", f"s3://b/landing/f{i}.csv"),
                r.get("_ingested_at", T0),
                2026,
                8,
            )
        )
    schema = ", ".join(f"`{c}` string" for c in BRONZE_COLS)
    schema += ", _source_file string, _ingested_at timestamp, _year int, _month int"
    return spark.createDataFrame(data, schema)


@pytest.fixture
def lookups(spark):
    carriers = s.parse_carriers(
        spark.createDataFrame([("AA", "American Airlines Inc.")], "Code string, Description string")
    )
    airports = s.parse_airports(
        spark.createDataFrame(
            [
                ("12892", "Los Angeles, CA: Los Angeles International"),
                ("12478", "New York, NY: John F. Kennedy International"),
            ],
            "Code string, Description string",
        )
    )
    return carriers, airports


def run(spark, rows, lookups) -> s.SilverResult:
    return s.build_silver(bronze_df(spark, rows), *lookups, SILVER_AT)


# ------------------------------------------------------------------- tipos y horas
def test_cast_types_and_values(spark, lookups) -> None:
    res = run(spark, [{}], lookups)
    r = res.flights.first()
    assert r.flight_date == date(2026, 8, 1)
    assert r.flight_number == 100 and r.crs_dep_time == 530  # "0530" -> 530
    assert r.arr_del15 == 0 and r.cancelled == 0  # "0.00" -> 0
    assert r.distance == 2475.0
    types = dict(res.flights.dtypes)
    assert (types["flight_date"], types["arr_del15"], types["distance"]) == (
        "date",
        "int",
        "double",
    )


@pytest.mark.parametrize(
    ("raw", "hour", "minute", "ts"),
    [
        ("0530", 5, 30, datetime(2026, 8, 1, 5, 30)),
        ("530", 5, 30, datetime(2026, 8, 1, 5, 30)),  # sin cero a la izquierda
        ("0006", 0, 6, datetime(2026, 8, 1, 0, 6)),
        ("2359", 23, 59, datetime(2026, 8, 1, 23, 59)),
        ("2400", 0, 0, datetime(2026, 8, 2, 0, 0)),  # medianoche = día siguiente
    ],
)
def test_crs_dep_time_parsing(spark, lookups, raw, hour, minute, ts) -> None:
    r = run(spark, [{"crsdeptime": raw}], lookups).flights.first()
    assert (r.dep_hour, r.dep_minute, r.scheduled_dep_local) == (hour, minute, ts)


@pytest.mark.parametrize("raw", ["2360", "2500", "-5"])
def test_invalid_crs_dep_time_discarded(spark, lookups, raw) -> None:
    res = run(spark, [{"crsdeptime": raw}], lookups)
    assert res.report["discarded"]["by_rule"]["invalid_crs_dep_time"] == 1
    assert res.report["output_rows"]["flights"] == 0


def test_cast_failure_is_counted(spark, lookups) -> None:
    res = run(spark, [{"distance": "abc"}], lookups)
    assert res.report["cast_failures"] == {"distance": 1}
    assert res.report["discarded"]["by_rule"]["non_positive_distance"] == 1


# ------------------------------------------------------------------- descartes
@pytest.mark.parametrize(
    ("override", "rule"),
    [
        ({"origin": None}, "missing_key"),
        ({"flightdate": "2026/08/01"}, "missing_key"),  # fecha no parseable
        ({"distance": "0.00"}, "non_positive_distance"),
        ({"distance": "-10.00"}, "non_positive_distance"),
        ({"crselapsedtime": None}, "non_positive_crs_elapsed"),
        ({"dest": "LAX", "destairportid": "12892"}, "origin_equals_dest"),
        ({"arrdel15": None}, "missing_target"),
    ],
)
def test_discard_rules(spark, lookups, override, rule) -> None:
    res = run(spark, [override, {"flight_number_reporting_airline": "200"}], lookups)
    by_rule = res.report["discarded"]["by_rule"]
    assert by_rule[rule] == 1 and sum(by_rule.values()) == 1
    assert res.report["output_rows"]["flights"] == 1


def test_first_matching_rule_wins(spark, lookups) -> None:
    res = run(spark, [{"origin": None, "distance": "0.00"}], lookups)
    assert res.report["discarded"]["by_rule"]["missing_key"] == 1
    assert res.report["discarded"]["by_rule"]["non_positive_distance"] == 0


# ------------------------------------------------------------------- duplicados
def test_dedup_keeps_latest_ingestion(spark, lookups) -> None:
    rows = [
        {"_ingested_at": T0, "tail_number": "OLD"},
        {"_ingested_at": T1, "tail_number": "NEW"},
        {"crsdeptime": "0600"},  # otra hora = otro vuelo
    ]
    res = run(spark, rows, lookups)
    assert res.report["duplicates_removed"] == 1
    tails = {r.tail_number for r in res.flights.collect()}
    assert "NEW" in tails and "OLD" not in tails


# ------------------------------------------------------------------- cancelados/desviados
def test_disrupted_split(spark, lookups) -> None:
    rows = [
        {},
        {"flight_number_reporting_airline": "2", "cancelled": "1.00", "arrdel15": None,
         "arrdelay": None, "cancellationcode": "B"},
        {"flight_number_reporting_airline": "3", "diverted": "1.00", "arrdel15": None,
         "arrdelay": None},
    ]  # fmt: skip
    res = run(spark, rows, lookups)
    assert res.report["output_rows"] == {"flights": 1, "flights_disrupted": 2}
    assert res.report["disrupted"] == {"cancelled": 1, "diverted": 1}
    # Cancelado sin ArrDel15 NO es "missing_target": no llegó, va a la tabla aparte.
    assert res.report["discarded"]["total"] == 0
    assert res.disrupted.where("cancellation_code = 'B'").count() == 1


# ------------------------------------------------------------------- imputación
def test_delay_causes_and_tail_imputed(spark, lookups) -> None:
    rows = [
        {"tail_number": None},
        {"flight_number_reporting_airline": "2", "arrdel15": "1.00", "arrdelay": "45.00",
         "carrierdelay": "30.00", "weatherdelay": "0.00", "nasdelay": "15.00",
         "securitydelay": "0.00", "lateaircraftdelay": "0.00"},
    ]  # fmt: skip
    res = run(spark, rows, lookups)
    on_time = res.flights.where("flight_number = 100").first()
    late = res.flights.where("flight_number = 2").first()
    assert on_time.carrier_delay == 0.0 and on_time.late_aircraft_delay == 0.0
    assert on_time.tail_number == s.UNKNOWN
    assert (late.carrier_delay, late.nas_delay) == (30.0, 15.0)  # valores reales intactos
    assert res.report["imputed"]["carrier_delay"]["rows"] == 1
    assert res.report["imputed"]["tail_number"]["rows"] == 1


# ------------------------------------------------------------------- outliers
def test_extreme_delays_kept_and_flagged(spark, lookups) -> None:
    # Con pocas filas P99.9 = máximo; 1,500 filas normales + 1 extrema lo hacen realista.
    rows = [{"flight_number_reporting_airline": str(i), "arrdelay": "5.00"} for i in range(1500)]
    rows.append({"flight_number_reporting_airline": "99999", "arrdelay": "1500.00",
                 "arrdel15": "1.00"})  # fmt: skip
    res = run(spark, rows, lookups)
    assert res.report["output_rows"]["flights"] == 1501  # no se elimina
    extreme = res.flights.where("is_extreme_delay").collect()
    assert [r.flight_number for r in extreme] == [99999]
    assert res.report["extreme_delay"]["flagged_rows"] == 1


# ------------------------------------------------------------------- catálogos
def test_enrich_names_and_unknown_codes(spark, lookups) -> None:
    rows = [{}, {"reporting_airline": "ZZ", "flight_number_reporting_airline": "7"}]
    res = run(spark, rows, lookups)
    known = res.flights.where("airline = 'AA'").first()
    assert known.airline_name == "American Airlines Inc."
    assert known.origin_airport_name == "Los Angeles International"
    unknown = res.flights.where("airline = 'ZZ'").first()
    assert unknown.airline_name == s.UNKNOWN  # no se elimina, se marca
    assert res.report["unmatched_lookups"]["airline"] == 1


def test_parse_airports_splits_city_and_name(spark) -> None:
    raw = spark.createDataFrame(
        [("10001", "Afognak Lake, AK: Afognak Lake Airport")], "Code string, Description string"
    )
    r = s.parse_airports(raw).first()
    assert (r.airport_id, r.airport_city, r.airport_name) == (
        10001,
        "Afognak Lake, AK",
        "Afognak Lake Airport",
    )


# ------------------------------------------------------------------- reporte
def test_report_reconciles(spark, lookups) -> None:
    rows = [
        {},
        {},  # duplicado
        {"flight_number_reporting_airline": "2", "distance": "0.00"},  # descartado
        {"flight_number_reporting_airline": "3", "cancelled": "1.00", "arrdel15": None},
        {"flight_number_reporting_airline": "4", "arrdel15": "1.00", "arrdelay": "20.00"},
    ]
    rep = run(spark, rows, lookups).report
    assert rep["input_rows"] == 5
    assert rep["reconciliation"] == {"input_rows": 5, "accounted_rows": 5, "ok": True}
    assert rep["target"]["positives"] == 1 and rep["target"]["positive_rate"] == 0.5
    assert rep["target"]["label_mismatch_arr_del15_vs_arr_delay"] == 0
    assert rep["nulls_input"]["carrier_delay"] == 5  # ninguna fila trae causa


def test_output_schema_order(spark, lookups) -> None:
    res = run(spark, [{}], lookups)
    cols = res.flights.columns
    assert cols[cols.index("airline") + 1] == "airline_name"
    assert cols[cols.index("crs_dep_time") + 1 : cols.index("crs_dep_time") + 4] == [
        "dep_hour",
        "dep_minute",
        "scheduled_dep_local",
    ]
    assert cols[-5:] == ["_silver_at", "_source_file", "_ingested_at", "_year", "_month"]
    assert "is_extreme_delay" in cols and "is_extreme_delay" not in res.disrupted.columns
    assert not [c for c in cols if c.startswith("_known") or c == "_discard_reason"]
    assert res.flights.where(F.col("_year") != 2026).count() == 0
