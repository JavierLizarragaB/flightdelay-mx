"""Lectura de la capa oro para entrenar: Athena UNLOAD a Parquet en S3 y descarga local.

UNLOAD en vez de leer los resultados CSV de Athena: Parquet conserva tipos, pesa ~10x
menos y se lee en segundos con pyarrow. La descarga queda cacheada en data/ (gitignored)
para no volver a escanear la tabla en cada experimento.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds

from src.gold.feature_config import MODEL_FEATURES, TARGET

WORKGROUP = "flightdelay-mx"
FEATURE_QUERY_COLUMNS = ["flight_date", "_year", "_month", *MODEL_FEATURES, TARGET]
LOOKUP_TABLES = ["lookup_route", "lookup_origin", "lookup_dest", "lookup_airline", "lookup_global"]


def run_athena(athena: Any, sql: str, workgroup: str = WORKGROUP) -> str:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {status['State']}: {status.get('StateChangeReason')}\n{sql}")
    return qid


def _download_prefix(s3: Any, bucket: str, prefix: str, dest: Path) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if obj["Size"] == 0 or name.startswith("."):
                continue
            s3.download_file(bucket, obj["Key"], str(dest / name))
            n += 1
    return n


def unload(
    athena: Any, s3: Any, bucket: str, sql: str, name: str, cache_dir: Path, tag: str
) -> Path:
    """UNLOAD (sql) -> s3://bucket/gold/exports/<tag>/<name>/ -> cache_dir/<tag>/<name>/."""
    local = cache_dir / tag / name
    if local.exists() and any(local.iterdir()):
        return local  # caché
    prefix = f"gold/exports/{tag}/{name}/"
    run_athena(
        athena,
        f"UNLOAD ({sql}) TO 's3://{bucket}/{prefix}' "
        "WITH (format = 'PARQUET', compression = 'SNAPPY')",
    )
    _download_prefix(s3, bucket, prefix, local)
    return local


def read_parquet_dir(
    path: Path, columns: list[str] | None = None, categorical: tuple[str, ...] = ()
) -> pd.DataFrame:
    """`categorical`: columnas de texto que se cargan como `category` (25 M de strings como
    objetos de Python ocuparían ~1.5 GB; como categoría, ~25 MB)."""
    table = ds.dataset(str(path), format="parquet").to_table(columns=columns)
    for c in categorical:
        i = table.schema.get_field_index(c)
        table = table.set_column(i, c, pc.dictionary_encode(table.column(c)))
    return table.to_pandas()


def load_features(athena: Any, s3: Any, bucket: str, cache_dir: Path, tag: str) -> pd.DataFrame:
    cols = ", ".join(FEATURE_QUERY_COLUMNS)
    path = unload(
        athena, s3, bucket, f"SELECT {cols} FROM fd_gold.features_flights", "features",
        cache_dir, tag,
    )  # fmt: skip
    df = read_parquet_dir(path, categorical=("airline",))
    df["flight_date"] = pd.to_datetime(df["flight_date"])
    for c in MODEL_FEATURES:
        if c != "airline":
            df[c] = df[c].astype("float32")
    df[TARGET] = df[TARGET].astype("int8")
    return df


def load_lookups(
    athena: Any, s3: Any, bucket: str, cache_dir: Path, tag: str
) -> dict[str, pd.DataFrame]:
    return {
        t: read_parquet_dir(
            unload(athena, s3, bucket, f"SELECT * FROM fd_gold.{t}", t, cache_dir, tag)
        )
        for t in LOOKUP_TABLES
    }
