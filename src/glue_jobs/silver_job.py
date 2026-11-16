"""Glue job de Plata: fd_bronze.flights_raw -> fd_silver.flights + fd_silver.flights_disrupted.

Solo orquesta; las reglas viven en src/glue_jobs/silver.py (testeadas local).

Parámetros del job:
  --FD_BUCKET     bucket del lakehouse
  --year_months   meses a procesar: "2026-08", "2026-07,2026-08" o "2023-01:2026-08"
  --bronze_db     por defecto fd_bronze
  --silver_db     por defecto fd_silver

Procesa mes por mes (cada mes es independiente: el P99.9 y el reporte son por mes) y
escribe con `overwritePartitions()` (idempotente). Además:
  - fd_silver.airlines y fd_silver.airports: catálogos BTS limpios (se reemplazan).
  - reports/quality_<yyyy_mm>.json: reporte de calidad por mes.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import expand_year_months
from src.glue_jobs import iceberg
from src.glue_jobs import silver as s

PARTITIONS = ("_year", "_month")
LOOKUP_FILES = {
    "carriers": "landing/lookups/L_UNIQUE_CARRIERS.csv",
    "airports": "landing/lookups/L_AIRPORT_ID.csv",
}


def read_lookup(spark: SparkSession, bucket: str, key: str) -> DataFrame:
    path = f"s3://{bucket}/{key}"
    try:
        return spark.read.option("header", "true").option("mode", "FAILFAST").csv(path)
    except Exception as e:  # AnalysisException: path does not exist
        raise s.SilverValidationError(
            f"No se encontró {path}. Ingiere los catálogos con la Lambda: "
            '{"lookups": true} (make lookups).'
        ) from e


def main(argv: list[str]) -> None:
    args = iceberg.parse_job_args(
        argv,
        ["FD_BUCKET", "year_months"],
        {"bronze_db": "fd_bronze", "silver_db": "fd_silver"},
    )
    bucket, months = args["FD_BUCKET"], expand_year_months(args["year_months"])
    spark = iceberg.build_spark("fd-silver", bucket)
    s3 = boto3.client("s3")
    silver_at = datetime.now(UTC).replace(tzinfo=None)

    bronze_table = iceberg.table_name(args["bronze_db"], "flights_raw")
    flights_table = iceberg.table_name(args["silver_db"], "flights")
    disrupted_table = iceberg.table_name(args["silver_db"], "flights_disrupted")

    carriers = s.parse_carriers(read_lookup(spark, bucket, LOOKUP_FILES["carriers"])).cache()
    airports = s.parse_airports(read_lookup(spark, bucket, LOOKUP_FILES["airports"])).cache()
    iceberg.replace_table(carriers, iceberg.table_name(args["silver_db"], "airlines"))
    iceberg.replace_table(airports, iceberg.table_name(args["silver_db"], "airports"))
    print(f"[silver] catálogos: {carriers.count():,} aerolíneas, {airports.count():,} aeropuertos")

    bronze_all = spark.table(bronze_table)
    for year, month in months:
        label = f"{year}-{month:02d}"
        bronze = bronze_all.where((F.col("_year") == year) & (F.col("_month") == month))
        result = s.build_silver(bronze, carriers, airports, silver_at)
        rep = result.report
        if rep["input_rows"] == 0:
            raise s.SilverValidationError(
                f"{bronze_table} no tiene filas para {label}: corre bronce primero."
            )

        modes = {
            "flights": iceberg.write_partitioned(spark, result.flights, flights_table, PARTITIONS),
            "flights_disrupted": iceberg.write_partitioned(
                spark, result.disrupted, disrupted_table, PARTITIONS
            ),
        }
        rep.update(
            {
                "year": year,
                "month": month,
                "source_table": bronze_table,
                "tables": {"flights": flights_table, "flights_disrupted": disrupted_table},
                "write_modes": modes,
            }
        )
        s3.put_object(
            Bucket=bucket,
            Key=f"reports/quality_{year}_{month:02d}.json",
            Body=json.dumps(rep, indent=2, default=str).encode(),
            ContentType="application/json",
        )
        out = rep["output_rows"]
        print(
            f"[silver] {label}: entrada {rep['input_rows']:,} -> vuelos {out['flights']:,} + "
            f"interrumpidos {out['flights_disrupted']:,} | descartes "
            f"{rep['discarded']['total']:,} | duplicados {rep['duplicates_removed']:,} | "
            f"positivos {rep['target']['positive_rate']}"
        )
        spark.catalog.clearCache()
        carriers.cache()
        airports.cache()


if __name__ == "__main__":
    main(sys.argv)
