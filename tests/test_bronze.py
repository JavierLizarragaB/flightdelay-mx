"""Tests de las transformaciones de bronce con PySpark local y un CSV con formato BTS."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from src.common import paths
from src.glue_jobs import bronze as b
from src.glue_jobs import iceberg
from src.ingestion import handler as ingest

# Igual que BTS: todo entre comillas, numéricos "0.00", coma final, campos vacíos.
BTS_CSV = (
    '"Year","Month","FlightDate","Reporting_Airline","CRSDepTime","Tail_Number","ArrDel15",\n'
    '"2026","8","2026-08-01","AA","0530","N123AA","0.00",\n'
    '"2026","8","2026-08-01","DL","2359","","",\n'
    '"2026","8","2026-08-02","UA","0006","N9UA","1.00",\n'
)
TS = datetime(2026, 11, 15, 12, 0, 0)
SRC = "s3://bkt/landing/year=2026/month=08/bts_2026_08.csv"


@pytest.fixture
def csv_path(tmp_path: Path) -> str:
    p = tmp_path / "bts.csv"
    p.write_text(BTS_CSV)
    return str(p)


# ---------------------------------------------------------------- rutas (sin Spark)
def test_paths_match_lambda() -> None:
    ym = ingest.YearMonth(2026, 8)
    assert paths.landing_csv_key(2026, 8) == ingest.csv_key(ym)
    assert paths.landing_manifest_key(2026, 8) == ingest.manifest_key(ym)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("2026-08", [(2026, 8)]),
        ("2026-08, 2026-07", [(2026, 7), (2026, 8)]),
        ("2025-11:2026-02", [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]),
        ("2026-08,2026-08", [(2026, 8)]),
    ],
)
def test_expand_year_months(spec: str, expected: list[tuple[int, int]]) -> None:
    assert paths.expand_year_months(spec) == expected


@pytest.mark.parametrize("spec", ["", "2026-8", "2026-13", "2026-08:2026-01", "abc"])
def test_expand_year_months_invalid(spec: str) -> None:
    with pytest.raises(ValueError):
        paths.expand_year_months(spec)


def test_validate_row_count() -> None:
    b.validate_row_count(10, 10, "2026-08")
    with pytest.raises(b.BronzeValidationError, match="manifiesto"):
        b.validate_row_count(9, 10, "2026-08")


# ---------------------------------------------------------------- Spark
pytestmark_spark = pytest.mark.spark


@pytestmark_spark
def test_to_bronze_shape_and_types(spark, csv_path: str) -> None:
    raw = b.read_bts_csv(spark, csv_path)
    assert "_c7" in raw.columns  # la coma final genera una columna sin nombre

    out = b.to_bronze(raw, SRC, 2026, 8, TS)

    assert out.columns == [
        "year", "month", "flightdate", "reporting_airline", "crsdeptime",
        "tail_number", "arrdel15", "_source_file", "_ingested_at", "_year", "_month",
    ]  # fmt: skip
    types = dict(out.dtypes)
    assert all(types[c] == "string" for c in out.columns[:7])
    assert (types["_year"], types["_month"], types["_ingested_at"]) == ("int", "int", "timestamp")
    assert out.count() == 3


@pytestmark_spark
def test_to_bronze_keeps_values_verbatim(spark, csv_path: str) -> None:
    rows = b.to_bronze(b.read_bts_csv(spark, csv_path), SRC, 2026, 8, TS).collect()
    first, second = rows[0], rows[1]
    assert first.crsdeptime == "0530"  # sin perder el cero a la izquierda
    assert first.arrdel15 == "0.00"  # sin castear
    assert second.tail_number is None and second.arrdel15 is None  # vacío -> null
    assert first._source_file == SRC and (first._year, first._month) == (2026, 8)


@pytestmark_spark
def test_trailing_column_with_data_fails(spark, tmp_path: Path) -> None:
    p = tmp_path / "bad.csv"
    p.write_text('"A","B",\n"1","2","sorpresa"\n')
    with pytest.raises(b.BronzeValidationError, match="sin encabezado con datos"):
        b.to_bronze(b.read_bts_csv(spark, str(p)), SRC, 2026, 8, TS)


@pytestmark_spark
def test_case_collision_fails(spark) -> None:
    df = spark.createDataFrame([("1", "2")], ["Origin", "ORIGIN"])
    with pytest.raises(b.BronzeValidationError, match="duplicadas"):
        b.lowercase_columns(df)


@pytestmark_spark
def test_align_to_schema(spark) -> None:
    df = spark.createDataFrame([("1", "2")], ["b", "a"])
    assert iceberg.align_to_schema(df, ["a", "b"]).columns == ["a", "b"]
    with pytest.raises(iceberg.SchemaMismatchError, match="Nuevas"):
        iceberg.align_to_schema(df, ["a"])
