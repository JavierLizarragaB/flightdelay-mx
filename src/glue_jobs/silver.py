"""Transformaciones de la capa Plata como funciones puras (DataFrame -> DataFrame).

Plata = "limpio y confiable". Cada regla está justificada aquí y en la sección 4 del
README; las cifras de evidencia vienen de la muestra de agosto 2026 (608,389 filas).

Orden del pipeline (`build_silver`):
  1. cast_columns          tipos + nombres snake_case (selección explícita de columnas)
  2. add_schedule_columns  CRSDepTime hhmm -> hora, minuto y timestamp local programado
  3. flag_discards         reglas de descarte, cada fila descartada con su motivo
  4. deduplicate           llave de negocio del vuelo
  5. split_disrupted       cancelados/desviados -> tabla aparte
  6. impute                causas de retraso nulas -> 0; Tail_Number nulo -> "UNKNOWN"
  7. flag_extreme_delay    marca (no elimina) retrasos > P99.9
  8. enrich                nombres de aerolínea y aeropuerto desde catálogos BTS

Todas las etapas son perezosas; `build_silver` calcula las métricas del reporte de calidad
y verifica que entrada = descartes + duplicados + vuelos + interrumpidos.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------------------
# Esquema: (columna en bronce, columna en plata, tipo). Selección EXPLÍCITA: el esquema de
# plata no depende de qué columnas vinieron vacías en un mes dado (evita que la tabla
# Iceberg cambie de forma entre meses). Quedan fuera las Div1*–Div5* (detalle de
# aterrizajes de vuelos desviados, 100 % vacías en Div3–Div5 y solo con sentido para los
# desviados), FirstDepTime/TotalAddGTime/LongestAddGTime (solo vuelos que regresaron a la
# puerta) y los "*Group"/"*Minutes" derivados (redundantes con las columnas base).
# ---------------------------------------------------------------------------------------
COLUMNS: list[tuple[str, str, str]] = [
    # Calendario
    ("flightdate", "flight_date", "date"),
    ("year", "year", "int"),
    ("quarter", "quarter", "int"),
    ("month", "month", "int"),
    ("dayofmonth", "day_of_month", "int"),
    ("dayofweek", "day_of_week", "int"),
    # Aerolínea y vuelo
    ("reporting_airline", "airline", "string"),
    ("dot_id_reporting_airline", "airline_dot_id", "int"),
    ("iata_code_reporting_airline", "airline_iata", "string"),
    ("tail_number", "tail_number", "string"),
    ("flight_number_reporting_airline", "flight_number", "int"),
    # Ruta
    ("originairportid", "origin_airport_id", "int"),
    ("origin", "origin", "string"),
    ("origincityname", "origin_city", "string"),
    ("originstate", "origin_state", "string"),
    ("destairportid", "dest_airport_id", "int"),
    ("dest", "dest", "string"),
    ("destcityname", "dest_city", "string"),
    ("deststate", "dest_state", "string"),
    # Programación (conocida antes del vuelo)
    ("crsdeptime", "crs_dep_time", "int"),
    ("crsarrtime", "crs_arr_time", "int"),
    ("crselapsedtime", "crs_elapsed_time", "double"),
    ("distance", "distance", "double"),
    ("distancegroup", "distance_group", "int"),
    # Resultado del vuelo (POST-vuelo: se conservan para análisis y para la etiqueta, pero
    # están prohibidas como features; ver src/gold/feature_config.py)
    ("deptime", "dep_time", "int"),
    ("depdelay", "dep_delay", "double"),
    ("depdel15", "dep_del15", "int"),
    ("taxiout", "taxi_out", "double"),
    ("taxiin", "taxi_in", "double"),
    ("arrtime", "arr_time", "int"),
    ("arrdelay", "arr_delay", "double"),
    ("arrdel15", "arr_del15", "int"),
    ("actualelapsedtime", "actual_elapsed_time", "double"),
    ("airtime", "air_time", "double"),
    ("carrierdelay", "carrier_delay", "double"),
    ("weatherdelay", "weather_delay", "double"),
    ("nasdelay", "nas_delay", "double"),
    ("securitydelay", "security_delay", "double"),
    ("lateaircraftdelay", "late_aircraft_delay", "double"),
    ("cancelled", "cancelled", "int"),
    ("diverted", "diverted", "int"),
    ("cancellationcode", "cancellation_code", "string"),
]
LINEAGE = ["_source_file", "_ingested_at", "_year", "_month"]

DEDUP_KEY = ["flight_date", "airline", "flight_number", "origin", "dest", "crs_dep_time"]
DELAY_CAUSES = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]
UNKNOWN = "UNKNOWN"
EXTREME_DELAY_QUANTILE = 0.999


# ---------------------------------------------------------------------------------------
# 1. Tipos
# ---------------------------------------------------------------------------------------
def _cast(col: str, kind: str) -> F.Column:
    c = F.trim(F.col(col))
    if kind == "string":
        return F.when(c == "", None).otherwise(c)
    if kind == "date":
        return F.to_date(c, "yyyy-MM-dd")
    if kind == "int":
        # BTS manda los enteros como "1.00": pasar por double evita que "1.00" -> null.
        d = c.cast(T.DoubleType())
        return F.when(d == F.floor(d), d.cast(T.IntegerType()))  # 1.5 -> null (falla de cast)
    if kind == "double":
        return c.cast(T.DoubleType())
    raise ValueError(f"Tipo no soportado: {kind}")


def cast_columns(bronze: DataFrame) -> DataFrame:
    """Selecciona, renombra y castea. Un valor no vacío que no se puede castear queda null y
    se cuenta en `cast_failure_counts` (no se pierde en silencio)."""
    return bronze.select(
        *[_cast(src, kind).alias(dst) for src, dst, kind in COLUMNS],
        *[F.col(c) for c in LINEAGE],
    )


def cast_failure_exprs() -> list[F.Column]:
    """Agregaciones sobre el DataFrame de bronce: valores presentes que no castearon."""
    out = []
    for src, dst, kind in COLUMNS:
        if kind == "string":
            continue
        raw = F.trim(F.col(src))
        failed = raw.isNotNull() & (raw != "") & _cast(src, kind).isNull()
        out.append(F.sum(failed.cast("int")).alias(f"castfail__{dst}"))
    return out


# ---------------------------------------------------------------------------------------
# 2. Hora programada
# ---------------------------------------------------------------------------------------
def hhmm_parts(col: F.Column) -> tuple[F.Column, F.Column, F.Column, F.Column]:
    """`hhmm` entero -> (hora, minuto, desfase_de_día, es_válido).

    Acepta 4 dígitos (`530` llega como entero 530 tras el cast, igual que "0530") y `2400`,
    que BTS usa para medianoche y equivale a 00:00 del día siguiente.
    """
    is_2400 = col == 2400
    hour = F.when(is_2400, F.lit(0)).otherwise(F.floor(col / 100).cast("int"))
    minute = F.when(is_2400, F.lit(0)).otherwise((col % 100).cast("int"))
    offset = F.when(is_2400, F.lit(1)).otherwise(F.lit(0))
    valid = col.isNotNull() & (is_2400 | (col.between(0, 2359) & (col % 100 < 60)))
    return hour, minute, offset, valid


def add_schedule_columns(df: DataFrame) -> DataFrame:
    hour, minute, offset, valid = hhmm_parts(F.col("crs_dep_time"))
    return (
        df.withColumn("dep_hour", F.when(valid, hour))
        .withColumn("dep_minute", F.when(valid, minute))
        .withColumn("_dep_offset", F.when(valid, offset))
        .withColumn("_crs_dep_valid", valid)
        .withColumn("_dep_date", F.date_add("flight_date", F.col("_dep_offset")))
        # Hora local del aeropuerto de origen. BTS no trae zona horaria, así que se guarda
        # como TIMESTAMP_NTZ (hora "de reloj", sin zona): con `timestamp` normal Spark la
        # convertiría según la zona de la sesión. Es la que usa oro para las ventanas.
        .withColumn(
            "scheduled_dep_local",
            F.when(
                valid,
                F.make_timestamp_ntz(
                    F.year("_dep_date"),
                    F.month("_dep_date"),
                    F.dayofmonth("_dep_date"),
                    F.col("dep_hour"),
                    F.col("dep_minute"),
                    F.lit(0),
                ),
            ),
        )
        .drop("_dep_offset", "_dep_date")
    )


# ---------------------------------------------------------------------------------------
# 3. Reglas de descarte (en orden; se registra la primera que aplica)
# ---------------------------------------------------------------------------------------
DISCARD_RULES: list[tuple[str, str]] = [
    (
        "missing_key",
        "Falta un campo de la llave del vuelo (fecha, aerolínea, número, origen, "
        "destino u hora programada): no se puede identificar ni deduplicar",
    ),
    ("invalid_crs_dep_time", "Hora programada fuera de rango (hh > 23 o mm > 59, salvo 2400)"),
    ("non_positive_distance", "Distance nula o <= 0: error de captura, físicamente imposible"),
    ("non_positive_crs_elapsed", "CRSElapsedTime nulo o <= 0: duración programada imposible"),
    ("origin_equals_dest", "Origen = destino: no es un vuelo entre dos aeropuertos"),
    (
        "missing_target",
        "Vuelo completado (ni cancelado ni desviado) sin ArrDel15: no tiene "
        "etiqueta y no puede entrenar ni evaluarse",
    ),
]


def flag_discards(df: DataFrame) -> DataFrame:
    key_missing = F.lit(False)
    for c in DEDUP_KEY:
        key_missing = key_missing | F.col(c).isNull()
    completed = (F.coalesce("cancelled", F.lit(0)) == 0) & (F.coalesce("diverted", F.lit(0)) == 0)
    conditions = {
        "missing_key": key_missing,
        "invalid_crs_dep_time": ~F.col("_crs_dep_valid"),
        "non_positive_distance": F.col("distance").isNull() | (F.col("distance") <= 0),
        "non_positive_crs_elapsed": F.col("crs_elapsed_time").isNull()
        | (F.col("crs_elapsed_time") <= 0),
        "origin_equals_dest": F.col("origin") == F.col("dest"),
        "missing_target": completed & F.col("arr_del15").isNull(),
    }
    reason = F.lit(None).cast("string")
    for name, _ in reversed(DISCARD_RULES):  # la primera regla de la lista gana
        reason = F.when(conditions[name], F.lit(name)).otherwise(reason)
    return df.withColumn("_discard_reason", reason).drop("_crs_dep_valid")


# ---------------------------------------------------------------------------------------
# 4. Duplicados
# ---------------------------------------------------------------------------------------
def deduplicate(df: DataFrame) -> DataFrame:
    """Un vuelo = fecha + aerolínea + número + origen + destino + hora programada. Ante
    duplicados se conserva la versión ingerida más recientemente (y, a igualdad, la de
    archivo mayor) para que el resultado sea determinista."""
    w = Window.partitionBy(*DEDUP_KEY).orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------------------
# 5. Cancelados y desviados
# ---------------------------------------------------------------------------------------
def is_disrupted() -> F.Column:
    return (F.coalesce("cancelled", F.lit(0)) == 1) | (F.coalesce("diverted", F.lit(0)) == 1)


def split_disrupted(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Los nulos de ArrDel15 son EXACTAMENTE cancelados + desviados (14,182 = 11,313 +
    2,869 en ago 2026): el vuelo no llegó, no es un dato faltante. Se separan porque
    "¿se cancelará?" es otro problema; mezclarlos cambiaría la definición del objetivo."""
    return df.where(~is_disrupted()), df.where(is_disrupted())


# ---------------------------------------------------------------------------------------
# 6. Imputación
# ---------------------------------------------------------------------------------------
def impute(flights: DataFrame) -> DataFrame:
    """Causas de retraso: BTS solo las llena cuando ArrDel15 = 1 (159,636 de 159,636 en ago
    2026). Nulo significa "0 minutos atribuibles a esa causa", no un dato faltante.
    Tail_Number: 0.2 % nulo; no es feature, se rellena para trazabilidad y joins."""
    out = flights
    for c in DELAY_CAUSES:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
    return out.withColumn("tail_number", F.coalesce("tail_number", F.lit(UNKNOWN)))


# ---------------------------------------------------------------------------------------
# 7. Outliers
# ---------------------------------------------------------------------------------------
def flag_extreme_delay(flights: DataFrame, threshold: float | None) -> DataFrame:
    """Se CONSERVAN: un retraso de 10 h es real y sigue siendo "retraso" (eliminarlo
    sesgaría la clase positiva). Solo se marcan para análisis descriptivo."""
    if threshold is None:
        return flights.withColumn("is_extreme_delay", F.lit(False))
    return flights.withColumn(
        "is_extreme_delay", F.coalesce(F.col("arr_delay") > F.lit(threshold), F.lit(False))
    )


# ---------------------------------------------------------------------------------------
# 8. Catálogos
# ---------------------------------------------------------------------------------------
def parse_carriers(raw: DataFrame) -> DataFrame:
    """L_UNIQUE_CARRIERS: Code, Description -> airline, airline_name."""
    return raw.select(
        F.trim("Code").alias("airline"), F.trim("Description").alias("airline_name")
    ).dropDuplicates(["airline"])


def parse_airports(raw: DataFrame) -> DataFrame:
    """L_AIRPORT_ID: Code, "Ciudad, ST: Nombre del aeropuerto" -> id, ciudad, nombre."""
    desc = F.trim("Description")
    return raw.select(
        F.trim("Code").cast("int").alias("airport_id"),
        F.trim(F.split(desc, ":").getItem(0)).alias("airport_city"),
        F.trim(F.regexp_extract(desc, r":\s*(.*)$", 1)).alias("airport_name"),
    ).dropDuplicates(["airport_id"])


def enrich(df: DataFrame, carriers: DataFrame, airports: DataFrame) -> DataFrame:
    """Left join (broadcast; los catálogos son pequeños): un código sin catálogo NO elimina
    el vuelo, queda como UNKNOWN y se cuenta en el reporte."""
    o = airports.select(
        F.col("airport_id").alias("origin_airport_id"),
        F.col("airport_name").alias("origin_airport_name"),
    )
    d = airports.select(
        F.col("airport_id").alias("dest_airport_id"),
        F.col("airport_name").alias("dest_airport_name"),
    )
    out = (
        df.join(F.broadcast(carriers), "airline", "left")
        .join(F.broadcast(o), "origin_airport_id", "left")
        .join(F.broadcast(d), "dest_airport_id", "left")
    )
    for c in ("airline_name", "origin_airport_name", "dest_airport_name"):
        out = out.withColumn(f"_known_{c}", F.col(c).isNotNull()).withColumn(
            c, F.coalesce(F.col(c), F.lit(UNKNOWN))
        )
    return out


# ---------------------------------------------------------------------------------------
# Orden final de columnas
# ---------------------------------------------------------------------------------------
_NAMES = [dst for _, dst, _ in COLUMNS]
_AFTER_AIRLINE = ["airline_name"]
_SCHEDULE = ["dep_hour", "dep_minute", "scheduled_dep_local"]


def _ordered(df: DataFrame, extra: list[str]) -> DataFrame:
    cols = []
    for c in _NAMES:
        cols.append(c)
        if c == "airline":
            cols += _AFTER_AIRLINE
        elif c == "origin":
            cols.append("origin_airport_name")
        elif c == "dest":
            cols.append("dest_airport_name")
        elif c == "crs_dep_time":
            cols += _SCHEDULE
    return df.select(*cols, *extra, "_silver_at", *LINEAGE)


# ---------------------------------------------------------------------------------------
# Orquestación con métricas de calidad
# ---------------------------------------------------------------------------------------
class SilverValidationError(Exception):
    """Las cuentas del pipeline no cuadran; el job debe fallar."""


@dataclass
class SilverResult:
    flights: DataFrame
    disrupted: DataFrame
    report: dict[str, Any] = field(default_factory=dict)


def _count_by(df: DataFrame, col: str) -> dict[str, int]:
    return {str(r[col]): int(r["count"]) for r in df.groupBy(col).count().collect()}


def build_silver(
    bronze: DataFrame,
    carriers: DataFrame,
    airports: DataFrame,
    silver_at: datetime,
) -> SilverResult:
    """Pipeline completo para un lote (normalmente un mes). Materializa conteos para el
    reporte de calidad; los DataFrames devueltos están cacheados para escribirlos."""
    report: dict[str, Any] = {"generated_at": silver_at.isoformat()}

    # Entrada: filas, nulos por columna (sobre las columnas que plata usa) y fallas de cast.
    agg = (
        bronze.agg(
            F.count(F.lit(1)).alias("__rows"),
            *[
                F.sum(F.col(src).isNull().cast("int")).alias(f"null__{dst}")
                for src, dst, _ in COLUMNS
            ],
            *cast_failure_exprs(),
        )
        .first()
        .asDict()
    )
    input_rows = int(agg.pop("__rows"))
    report["input_rows"] = input_rows
    report["nulls_input"] = {k[6:]: int(v) for k, v in agg.items() if k.startswith("null__")}
    report["cast_failures"] = {
        k[10:]: int(v) for k, v in agg.items() if k.startswith("castfail__") and v
    }

    typed = flag_discards(add_schedule_columns(cast_columns(bronze))).cache()
    discarded_counts = _count_by(
        typed.where(F.col("_discard_reason").isNotNull()), "_discard_reason"
    )
    report["discarded"] = {
        "total": sum(discarded_counts.values()),
        "by_rule": {name: discarded_counts.get(name, 0) for name, _ in DISCARD_RULES},
        "rules": dict(DISCARD_RULES),
    }

    kept = typed.where(F.col("_discard_reason").isNull()).drop("_discard_reason")
    deduped = deduplicate(kept).cache()
    kept_rows, deduped_rows = kept.count(), deduped.count()
    report["duplicates_removed"] = kept_rows - deduped_rows
    report["dedup_key"] = DEDUP_KEY

    flights_raw, disrupted_raw = split_disrupted(deduped)

    # Imputación (contar antes de imputar)
    imputed = (
        flights_raw.agg(
            *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in DELAY_CAUSES],
            F.sum(F.col("tail_number").isNull().cast("int")).alias("tail_number"),
        )
        .first()
        .asDict()
    )
    report["imputed"] = {
        **{c: {"rule": "null -> 0", "rows": int(imputed[c] or 0)} for c in DELAY_CAUSES},
        "tail_number": {"rule": f"null -> {UNKNOWN}", "rows": int(imputed["tail_number"] or 0)},
    }

    q = flights_raw.where(F.col("arr_delay").isNotNull()).approxQuantile(
        "arr_delay", [EXTREME_DELAY_QUANTILE], 0.0001
    )
    threshold = float(q[0]) if q else None

    flights = enrich(flag_extreme_delay(impute(flights_raw), threshold), carriers, airports)
    disrupted = enrich(
        disrupted_raw.withColumn("tail_number", F.coalesce("tail_number", F.lit(UNKNOWN))),
        carriers,
        airports,
    )
    flights = flights.withColumn("_silver_at", F.lit(silver_at).cast("timestamp"))
    disrupted = disrupted.withColumn("_silver_at", F.lit(silver_at).cast("timestamp"))

    stats = flights.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("arr_del15").alias("positives"),
        F.sum(F.col("is_extreme_delay").cast("int")).alias("extreme"),
        F.min("arr_delay").alias("min_arr_delay"),
        F.max("arr_delay").alias("max_arr_delay"),
        F.sum((~F.col("_known_airline_name")).cast("int")).alias("unk_airline"),
        F.sum((~F.col("_known_origin_airport_name")).cast("int")).alias("unk_origin"),
        F.sum((~F.col("_known_dest_airport_name")).cast("int")).alias("unk_dest"),
        F.sum(((F.col("arr_del15") == 1) != (F.col("arr_delay") >= 15)).cast("int")).alias(
            "label_mismatch"
        ),
    ).first()
    d_stats = disrupted.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.coalesce("cancelled", F.lit(0))).alias("cancelled"),
        F.sum(F.coalesce("diverted", F.lit(0))).alias("diverted"),
    ).first()

    flights_rows, disrupted_rows = int(stats["rows"]), int(d_stats["rows"])
    positives = int(stats["positives"] or 0)
    report["output_rows"] = {"flights": flights_rows, "flights_disrupted": disrupted_rows}
    report["disrupted"] = {
        "cancelled": int(d_stats["cancelled"] or 0),
        "diverted": int(d_stats["diverted"] or 0),
    }
    report["target"] = {
        "column": "arr_del15",
        "positives": positives,
        "positive_rate": round(positives / flights_rows, 4) if flights_rows else None,
        "label_mismatch_arr_del15_vs_arr_delay": int(stats["label_mismatch"] or 0),
    }
    report["extreme_delay"] = {
        "quantile": EXTREME_DELAY_QUANTILE,
        "threshold_minutes": threshold,
        "flagged_rows": int(stats["extreme"] or 0),
        "min_arr_delay": stats["min_arr_delay"],
        "max_arr_delay": stats["max_arr_delay"],
        "treatment": "se conservan; solo se marca is_extreme_delay",
    }
    report["unmatched_lookups"] = {
        "airline": int(stats["unk_airline"] or 0),
        "origin_airport_id": int(stats["unk_origin"] or 0),
        "dest_airport_id": int(stats["unk_dest"] or 0),
    }

    accounted = (
        report["discarded"]["total"] + report["duplicates_removed"] + flights_rows + disrupted_rows
    )
    report["reconciliation"] = {
        "input_rows": input_rows,
        "accounted_rows": accounted,
        "ok": accounted == input_rows,
    }
    if accounted != input_rows:
        raise SilverValidationError(
            f"No cuadran las filas: entrada {input_rows:,} vs contabilizadas {accounted:,}"
        )

    extra_flags = ["is_extreme_delay"]
    return SilverResult(
        flights=_ordered(flights, extra_flags),
        disrupted=_ordered(disrupted, []),
        report=report,
    )
