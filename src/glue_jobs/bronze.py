"""Transformaciones de la capa Bronce como funciones puras (DataFrame -> DataFrame).

Bronce = "lo que llegó, tal cual". Las únicas intervenciones, cada una justificada:

1. Todas las columnas se leen como `string` (inferSchema=False). Inferir tipos aquí
   escondería errores de formato que plata debe ver y contar; además BTS manda los
   numéricos como "0.00" y los códigos con ceros a la izquierda ("0530").
2. Se elimina la columna vacía `_c109` que Spark crea por la coma final de cada línea del
   CSV de BTS. Se verifica que esté 100 % vacía antes de tirarla: si algún día trae datos,
   el job falla en vez de perderlos.
3. Nombres en minúsculas: Glue Data Catalog y Athena guardan los nombres en minúsculas;
   alinearlos evita diferencias entre el esquema Iceberg y el catálogo.
4. Campos vacíos -> null: es el comportamiento por defecto del lector CSV de Spark
   (`nullValue=""`). BTS no distingue "vacío" de "desconocido", así que no se pierde
   información.
5. Metadatos de linaje: `_source_file`, `_ingested_at`, `_year`, `_month`.
"""

from __future__ import annotations

import re
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Columnas que Spark nombra solas cuando el encabezado está vacío (coma final).
_AUTO_COL_RE = re.compile(r"^_c\d+$")

PARTITION_COLUMNS = ("_year", "_month")
METADATA_COLUMNS = ("_source_file", "_ingested_at", *PARTITION_COLUMNS)

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "quote": '"',
    "escape": '"',
    "mode": "FAILFAST",  # una línea malformada detiene el job en vez de perderse en silencio
    "encoding": "UTF-8",
}


class BronzeValidationError(Exception):
    """Los datos no cumplen una invariante de bronce; el job debe fallar."""


def read_bts_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.options(**CSV_OPTIONS).csv(path)


def auto_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if _AUTO_COL_RE.match(c)]


def raw_stats(df: DataFrame) -> tuple[int, dict[str, int]]:
    """Una sola pasada: filas totales y valores no nulos de cada columna sin encabezado."""
    autos = auto_columns(df)
    row = df.agg(
        F.count(F.lit(1)).alias("__rows"), *[F.count(F.col(c)).alias(c) for c in autos]
    ).first()
    return int(row["__rows"]), {c: int(row[c]) for c in autos}


def drop_trailing_empty_columns(df: DataFrame, non_null: dict[str, int] | None = None) -> DataFrame:
    """`non_null` permite reutilizar el resultado de `raw_stats` y no leer el CSV otra vez."""
    auto_cols = auto_columns(df)
    if not auto_cols:
        return df
    if non_null is None:
        non_null = raw_stats(df)[1]
    with_data = [c for c in auto_cols if non_null.get(c, 0) > 0]
    if with_data:
        raise BronzeValidationError(
            f"Columnas sin encabezado con datos (¿cambió el formato de BTS?): {with_data}"
        )
    return df.drop(*auto_cols)


def lowercase_columns(df: DataFrame) -> DataFrame:
    lowered = [c.lower() for c in df.columns]
    dupes = sorted({c for c in lowered if lowered.count(c) > 1})
    if dupes:
        raise BronzeValidationError(f"Columnas duplicadas al pasar a minúsculas: {dupes}")
    return df.toDF(*lowered)


def add_lineage(
    df: DataFrame, source_file: str, year: int, month: int, ingested_at: datetime
) -> DataFrame:
    return df.select(
        "*",
        F.lit(source_file).alias("_source_file"),
        F.lit(ingested_at).cast(T.TimestampType()).alias("_ingested_at"),
        F.lit(year).cast(T.IntegerType()).alias("_year"),
        F.lit(month).cast(T.IntegerType()).alias("_month"),
    )


def to_bronze(
    raw: DataFrame,
    source_file: str,
    year: int,
    month: int,
    ingested_at: datetime,
    auto_col_non_null: dict[str, int] | None = None,
) -> DataFrame:
    df = drop_trailing_empty_columns(raw, auto_col_non_null)
    df = lowercase_columns(df)
    non_string = [c for c, t in df.dtypes if t != "string"]
    if non_string:
        raise BronzeValidationError(f"Bronce debe ser todo string; no lo son: {non_string}")
    return add_lineage(df, source_file, year, month, ingested_at)


def validate_row_count(actual: int, expected: int, label: str) -> None:
    """Compara contra el conteo del `_manifest.json` que escribió la Lambda de ingesta."""
    if actual != expected:
        raise BronzeValidationError(
            f"{label}: se leyeron {actual:,} filas pero el manifiesto dice {expected:,}"
        )
