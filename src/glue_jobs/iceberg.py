"""Utilidades compartidas por los Glue jobs: argumentos, sesión Iceberg y escrituras."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

CATALOG = "glue_catalog"

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
}


def parse_job_args(
    argv: list[str], required: Sequence[str], optional: dict[str, str]
) -> dict[str, str]:
    """`--clave valor` como en Glue. Usa getResolvedOptions dentro de Glue y argparse en
    local; los opcionales toman su valor por defecto si no vienen."""
    try:
        from awsglue.utils import getResolvedOptions
    except ImportError:
        p = argparse.ArgumentParser()
        for k in required:
            p.add_argument(f"--{k}", required=True)
        for k, v in optional.items():
            p.add_argument(f"--{k}", default=v)
        return vars(p.parse_known_args(argv[1:])[0])
    present = [k for k in optional if f"--{k}" in argv]
    return {**optional, **getResolvedOptions(argv, [*required, *present])}


class SchemaMismatchError(Exception):
    """El DataFrame no coincide con el esquema de la tabla existente."""


def build_spark(app_name: str, bucket: str) -> SparkSession:
    """Catálogo Iceberg respaldado por Glue Data Catalog. Glue 5.0 trae los JARs de Iceberg
    con `--datalake-formats iceberg`; aquí solo se configura el catálogo."""
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{bucket}/")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def table_name(database: str, table: str) -> str:
    return f"{CATALOG}.{database}.{table}"


def align_to_schema(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Ordena las columnas como la tabla existente. Si cambió el esquema falla con un
    mensaje claro en lugar de escribir datos desalineados."""
    missing = [c for c in columns if c not in df.columns]
    extra = [c for c in df.columns if c not in columns]
    if missing or extra:
        raise SchemaMismatchError(
            f"El esquema cambió. Faltan: {missing}. Nuevas: {extra}. "
            "Revisa y evoluciona la tabla (ALTER TABLE) antes de reprocesar."
        )
    return df.select(*columns)


def write_partitioned(
    spark: SparkSession, df: DataFrame, table: str, partition_cols: Sequence[str]
) -> str:
    """Crea la tabla si no existe; si existe, `overwritePartitions()` reemplaza SOLO las
    particiones presentes en `df` (idempotente por mes)."""
    if spark.catalog.tableExists(table):
        df = align_to_schema(df, spark.table(table).columns)
        df.writeTo(table).overwritePartitions()
        return "overwrite_partitions"
    writer = df.writeTo(table).using("iceberg").partitionedBy(*partition_cols)
    for k, v in TABLE_PROPERTIES.items():
        writer = writer.tableProperty(k, v)
    writer.create()
    return "create"


def replace_table(df: DataFrame, table: str, partition_cols: Sequence[str] = ()) -> None:
    """Reemplazo completo (nuevo snapshot de Iceberg; el anterior queda en el historial).
    Para catálogos y para oro, que se recalcula entero porque sus features dependen de
    toda la historia."""
    writer = df.writeTo(table).using("iceberg")
    if partition_cols:
        writer = writer.partitionedBy(*partition_cols)
    for k, v in TABLE_PROPERTIES.items():
        writer = writer.tableProperty(k, v)
    writer.createOrReplace()
