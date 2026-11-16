"""Glue job de Oro: fd_silver.flights -> fd_gold.features_flights + lookups para la API.

Solo orquesta; la lógica vive en src/glue_jobs/gold.py (testeada local).

Se recalcula completo en cada corrida: las features históricas (ventanas de 30 días y
target encoding expansivo) dependen de toda la historia, y con ~24 M de filas un
recálculo completo cuesta minutos. Iceberg conserva el snapshot anterior.

Parámetros: --FD_BUCKET, --silver_db (fd_silver), --gold_db (fd_gold).
Deja un resumen en reports/gold/gold_summary.json (filas y nulos por feature).
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import boto3
from pyspark.sql import functions as F

from src.glue_jobs import gold as g
from src.glue_jobs import iceberg
from src.gold.feature_config import MODEL_FEATURES, TARGET


def main(argv: list[str]) -> None:
    args = iceberg.parse_job_args(
        argv, ["FD_BUCKET"], {"silver_db": "fd_silver", "gold_db": "fd_gold"}
    )
    bucket = args["FD_BUCKET"]
    spark = iceberg.build_spark("fd-gold", bucket)
    started = datetime.now(UTC)

    flights = spark.table(iceberg.table_name(args["silver_db"], "flights")).cache()
    years = flights.agg(F.min("_year"), F.max("_year")).first()
    holidays = g.holidays_df(spark, years[0] - 1, years[1] + 1)

    features = g.build_features(flights, holidays)
    features_table = iceberg.table_name(args["gold_db"], "features_flights")
    iceberg.replace_table(features, features_table, g.PARTITIONS)

    lookups = g.build_lookups(flights)
    for name, df in lookups.items():
        iceberg.replace_table(df, iceberg.table_name(args["gold_db"], name))

    written = spark.table(features_table)
    stats = (
        written.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(TARGET).alias("positives"),
            *[F.sum(F.col(c).isNull().cast("int")).alias(f"null__{c}") for c in MODEL_FEATURES],
        )
        .first()
        .asDict()
    )
    per_month = {
        f"{r['_year']}-{r['_month']:02d}": r["count"]
        for r in written.groupBy("_year", "_month").count().orderBy("_year", "_month").collect()
    }
    summary = {
        "generated_at": started.isoformat(timespec="seconds"),
        "table": features_table,
        "rows": stats.pop("rows"),
        "positive_rate": round(stats.pop("positives") / max(sum(per_month.values()), 1), 4),
        "nulls_by_feature": {k[6:]: v for k, v in stats.items()},
        "rows_by_month": per_month,
        "lookups": {
            name: spark.table(iceberg.table_name(args["gold_db"], name)).count() for name in lookups
        },  # fmt: skip
    }
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key="reports/gold/gold_summary.json",
        Body=json.dumps(summary, indent=2, default=str).encode(),
        ContentType="application/json",
    )
    print(f"[gold] {features_table}: {summary['rows']:,} filas; lookups {summary['lookups']}")


if __name__ == "__main__":
    main(sys.argv)
