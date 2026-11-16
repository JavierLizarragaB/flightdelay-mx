"""Glue job de Bronce: landing/ (CSV) -> fd_bronze.flights_raw (Apache Iceberg).

Solo orquesta; las transformaciones viven en src/glue_jobs/bronze.py (testeadas local).

Parámetros del job:
  --FD_BUCKET     bucket del lakehouse
  --year_months   meses a procesar: "2026-08", "2026-07,2026-08" o "2023-01:2026-08"
  --bronze_db     base de Glue (por defecto fd_bronze)

Por cada mes: lee el `_manifest.json` de la ingesta, lee el CSV, valida que el conteo de
filas coincida con el manifiesto y que la columna de la coma final venga vacía, y escribe.
Idempotente: `overwritePartitions()` reemplaza solo las particiones (_year, _month) que
vienen en el DataFrame; los demás meses no se tocan. Deja un reporte por mes en
reports/bronze/.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from functools import reduce

import boto3

from src.common.paths import expand_year_months, landing_csv_key, landing_manifest_key
from src.glue_jobs import bronze as b
from src.glue_jobs import iceberg

TABLE_NAME = "flights_raw"


def read_manifest(s3, bucket: str, year: int, month: int) -> dict:
    key = landing_manifest_key(year, month)
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey as e:
        raise b.BronzeValidationError(
            f"No existe s3://{bucket}/{key}: el mes {year}-{month:02d} no se ha ingerido "
            "(o la ingesta no terminó). Corre primero la Lambda de ingesta."
        ) from e
    return json.loads(body)


def main(argv: list[str]) -> None:
    args = iceberg.parse_job_args(argv, ["FD_BUCKET", "year_months"], {"bronze_db": "fd_bronze"})
    bucket, months = args["FD_BUCKET"], expand_year_months(args["year_months"])
    table = iceberg.table_name(args["bronze_db"], TABLE_NAME)
    spark = iceberg.build_spark("fd-bronze", bucket)
    s3 = boto3.client("s3")
    ingested_at = datetime.now(UTC).replace(tzinfo=None)

    frames, reports = [], []
    for year, month in months:
        label = f"{year}-{month:02d}"
        manifest = read_manifest(s3, bucket, year, month)
        source = f"s3://{bucket}/{landing_csv_key(year, month)}"

        raw = b.read_bts_csv(spark, source)
        rows, auto_non_null = b.raw_stats(raw)  # 1.ª pasada: conteo + columna vacía
        b.validate_row_count(rows, int(manifest["csv_rows"]), label)
        frames.append(b.to_bronze(raw, source, year, month, ingested_at, auto_non_null))
        reports.append(
            {
                "year": year,
                "month": month,
                "source_file": source,
                "rows_manifest": int(manifest["csv_rows"]),
                "rows_bronze": rows,
                "dropped_columns": sorted(auto_non_null),
            }
        )
        print(f"[bronze] {label}: {rows:,} filas validadas contra el manifiesto")

    df = reduce(lambda a, c: a.unionByName(c), frames)
    mode = iceberg.write_partitioned(spark, df, table, b.PARTITION_COLUMNS)  # 2.ª pasada

    for r in reports:
        r.update({"table": table, "write_mode": mode, "ingested_at": ingested_at.isoformat()})
        s3.put_object(
            Bucket=bucket,
            Key=f"reports/bronze/bronze_{r['year']}_{r['month']:02d}.json",
            Body=json.dumps(r, indent=2).encode(),
            ContentType="application/json",
        )
    total = sum(r["rows_bronze"] for r in reports)
    print(f"[bronze] {table}: {mode}, {len(months)} mes(es), {total:,} filas")


if __name__ == "__main__":
    main(sys.argv)
