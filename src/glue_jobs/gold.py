"""Capa Oro: features del modelo y lookups para la API, como funciones puras de Spark.

Regla central: **ninguna feature ve el futuro**. Las features históricas usan solo datos
estrictamente anteriores al vuelo:

* Tasas de 30 días (`route/origin/airline_delay_rate_30d`): ventana [d-30, d-1] sobre el
  día del vuelo d (el mismo día no se conoce al comprar el boleto), suavizadas hacia la
  tasa global de esa misma ventana (k = 20 pseudo-vuelos) para que una ruta con 3 vuelos
  no salte entre 0 % y 100 %.
* Target encoding (`origin_te`, `dest_te`): media *expansiva por mes*: un vuelo de marzo
  solo ve enero y febrero. Más estricto que calcularla con todo el set de entrenamiento,
  que filtraría la etiqueta de la propia fila hacia su feature. Suavizado k = 50.

Consecuencia: el primer mes (ene 2023) no tiene historial (TE = null) y los primeros
días tienen ventanas incompletas; el entrenamiento lo usa solo como calentamiento.

Las fórmulas deterministas (hora y mes cíclicos, días al feriado) replican
src/gold/features.py, que es lo que usa la API; un test verifica que coincidan.
"""

from __future__ import annotations

import math
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.gold import features as fx
from src.gold.feature_config import MODEL_FEATURES, TARGET, assert_no_leakage

ID_COLUMNS = ["flight_date", "origin", "dest", "dep_hour"]
PARTITIONS = ["_year", "_month"]
GOLD_COLUMNS = [*ID_COLUMNS, *MODEL_FEATURES, TARGET, *PARTITIONS]


# ---------------------------------------------------------------------------------------
# Features deterministas
# ---------------------------------------------------------------------------------------
def add_calendar_features(df: DataFrame) -> DataFrame:
    two_pi = 2 * math.pi
    hour_frac = (F.col("dep_hour") + F.col("dep_minute") / 60.0) / 24.0
    month_frac = (F.col("month") - 1) / 12.0
    return (
        df.withColumn("dep_hour_sin", F.sin(F.lit(two_pi) * hour_frac))
        .withColumn("dep_hour_cos", F.cos(F.lit(two_pi) * hour_frac))
        .withColumn("month_sin", F.sin(F.lit(two_pi) * month_frac))
        .withColumn("month_cos", F.cos(F.lit(two_pi) * month_frac))
    )


def holidays_df(spark: SparkSession, start_year: int, end_year: int) -> DataFrame:
    rows = [(h,) for h in fx.holidays_between(start_year, end_year)]
    return spark.createDataFrame(rows, "holiday date")


def add_days_to_holiday(df: DataFrame, holidays: DataFrame) -> DataFrame:
    """Cruce de fechas distintas (~1,300) x feriados (~60): barato, luego join por fecha."""
    dates = df.select("flight_date").distinct()
    dist = (
        dates.crossJoin(F.broadcast(holidays))
        .groupBy("flight_date")
        .agg(F.min(F.abs(F.datediff("holiday", "flight_date"))).alias("_d"))
        .select(
            "flight_date",
            F.least(F.col("_d"), F.lit(fx.HOLIDAY_CAP_DAYS)).alias("days_to_holiday"),
        )
    )
    return df.join(F.broadcast(dist), "flight_date", "left")


# ---------------------------------------------------------------------------------------
# Tasas de 30 días con desfase
# ---------------------------------------------------------------------------------------
def _day_number(col: str) -> F.Column:
    return F.datediff(F.col(col), F.lit("1970-01-01"))


def global_rate_30d(flights: DataFrame) -> DataFrame:
    """Tasa global de retrasos en [d-30, d-1] para cada día d con vuelos (prior)."""
    daily = flights.groupBy("flight_date").agg(
        F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p")
    )
    w = Window.orderBy(_day_number("flight_date")).rangeBetween(-30, -1)
    return daily.select(
        "flight_date",
        (F.sum("p").over(w) / F.sum("n").over(w)).alias("prior_30d"),
    )


def lagged_rate_30d(flights: DataFrame, keys: list[str], name: str, prior: DataFrame) -> DataFrame:
    """Devuelve (keys, flight_date, <name>) con la tasa suavizada de los 30 días previos."""
    daily = flights.groupBy(*keys, "flight_date").agg(
        F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p")
    )
    w = Window.partitionBy(*keys).orderBy(_day_number("flight_date")).rangeBetween(-30, -1)
    k = F.lit(float(fx.SMOOTHING_30D))
    return (
        daily.withColumn("n30", F.coalesce(F.sum("n").over(w), F.lit(0)))
        .withColumn("p30", F.coalesce(F.sum("p").over(w), F.lit(0)))
        .join(prior, "flight_date", "left")
        .select(
            *keys,
            "flight_date",
            ((F.col("p30") + k * F.col("prior_30d")) / (F.col("n30") + k)).alias(name),
        )
    )


# ---------------------------------------------------------------------------------------
# Target encoding expansivo por mes
# ---------------------------------------------------------------------------------------
def _month_index() -> F.Column:
    return F.col("_year") * 12 + F.col("_month")


def expanding_target_encoding(flights: DataFrame, key: str, name: str) -> DataFrame:
    """Devuelve (key, _year, _month, <name>) usando solo meses ANTERIORES."""
    monthly = flights.groupBy(key, "_year", "_month").agg(
        F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p")
    )
    totals = monthly.groupBy("_year", "_month").agg(F.sum("n").alias("gn"), F.sum("p").alias("gp"))
    wg = Window.orderBy(_month_index()).rangeBetween(Window.unboundedPreceding, -1)
    prior = totals.select(
        "_year", "_month", (F.sum("gp").over(wg) / F.sum("gn").over(wg)).alias("prior")
    )
    wk = Window.partitionBy(key).orderBy(_month_index()).rangeBetween(Window.unboundedPreceding, -1)
    k = F.lit(float(fx.SMOOTHING_TE))
    return (
        monthly.withColumn("n_hist", F.coalesce(F.sum("n").over(wk), F.lit(0)))
        .withColumn("p_hist", F.coalesce(F.sum("p").over(wk), F.lit(0)))
        .join(prior, ["_year", "_month"], "left")
        .select(
            key,
            "_year",
            "_month",
            ((F.col("p_hist") + k * F.col("prior")) / (F.col("n_hist") + k)).alias(name),
        )
    )


# ---------------------------------------------------------------------------------------
# Tabla de features
# ---------------------------------------------------------------------------------------
def build_features(flights: DataFrame, holidays: DataFrame) -> DataFrame:
    """fd_silver.flights -> fd_gold.features_flights (una fila por vuelo)."""
    base = flights.select(
        "flight_date",
        "month",
        "day_of_week",
        "airline",
        "origin",
        "dest",
        "dep_hour",
        "dep_minute",
        "distance",
        "crs_elapsed_time",
        TARGET,
        *PARTITIONS,
    )
    prior = global_rate_30d(base).cache()
    route = lagged_rate_30d(base, ["origin", "dest"], "route_delay_rate_30d", prior)
    origin = lagged_rate_30d(base, ["origin"], "origin_delay_rate_30d", prior)
    airline = lagged_rate_30d(base, ["airline"], "airline_delay_rate_30d", prior)
    te_o = expanding_target_encoding(base, "origin", "origin_te")
    te_d = expanding_target_encoding(base, "dest", "dest_te")

    out = (
        base.join(route, ["origin", "dest", "flight_date"], "left")
        .join(origin, ["origin", "flight_date"], "left")
        .join(airline, ["airline", "flight_date"], "left")
        .join(te_o, ["origin", *PARTITIONS], "left")
        .join(te_d, ["dest", *PARTITIONS], "left")
    )
    out = add_days_to_holiday(add_calendar_features(out), holidays)
    out = out.withColumn("distance", F.col("distance").cast("double")).withColumn(
        "crs_elapsed_time", F.col("crs_elapsed_time").cast("double")
    )
    assert_no_leakage(MODEL_FEATURES)  # salvaguarda: la lista de features sigue limpia
    return out.select(*GOLD_COLUMNS)


# ---------------------------------------------------------------------------------------
# Lookups para la API (estadísticas al cierre de los datos)
# ---------------------------------------------------------------------------------------
def _smooth(p: F.Column, n: F.Column, prior: float, k: float) -> F.Column:
    return (p + F.lit(k * prior)) / (n + F.lit(float(k)))


def build_lookups(flights: DataFrame) -> dict[str, DataFrame]:
    """Misma definición que las features, evaluada "mañana" respecto al último día con
    datos: tasas de los últimos 30 días y TE con todo el historial."""
    max_date: date = flights.agg(F.max("flight_date")).first()[0]
    window_start = F.date_sub(F.lit(max_date), 29)
    last30 = flights.where(F.col("flight_date") >= window_start)

    g30 = last30.agg(F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p")).first()
    prior_30d = g30["p"] / g30["n"]
    gall = flights.agg(F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p")).first()
    prior_te = gall["p"] / gall["n"]
    k30, kte = fx.SMOOTHING_30D, fx.SMOOTHING_TE

    def rate30(keys: list[str], name: str) -> DataFrame:
        agg = last30.groupBy(*keys).agg(F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p"))
        return agg.select(*keys, _smooth(F.col("p"), F.col("n"), prior_30d, k30).alias(name),
                          F.col("n").alias(f"{name}_n"))  # fmt: skip

    def te(key: str, name: str) -> DataFrame:
        agg = flights.groupBy(key).agg(F.count(F.lit(1)).alias("n"), F.sum(TARGET).alias("p"))
        return agg.select(key, _smooth(F.col("p"), F.col("n"), prior_te, kte).alias(name))

    def mode_name(code: str, name_col: str, city_col: str) -> DataFrame:
        w = Window.partitionBy(code).orderBy(F.col("cnt").desc())
        return (
            flights.groupBy(code, name_col, city_col)
            .agg(F.count(F.lit(1)).alias("cnt"))
            .withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .select(code, F.col(name_col).alias("airport_name"), F.col(city_col).alias("city"))
        )

    route_stats = flights.groupBy("origin", "dest").agg(
        F.count(F.lit(1)).alias("flights_total"),
        F.percentile_approx("distance", 0.5).alias("distance"),
        F.percentile_approx("crs_elapsed_time", 0.5).alias("crs_elapsed_time"),
    )
    lookup_route = route_stats.join(
        rate30(["origin", "dest"], "route_delay_rate_30d"), ["origin", "dest"], "left"
    )

    origin_stats = flights.groupBy("origin").agg(
        F.percentile_approx("distance", 0.5).alias("median_distance"),
        F.percentile_approx("crs_elapsed_time", 0.5).alias("median_crs_elapsed_time"),
    )
    lookup_origin = (
        mode_name("origin", "origin_airport_name", "origin_city")
        .join(origin_stats, "origin")
        .join(te("origin", "origin_te"), "origin")
        .join(rate30(["origin"], "origin_delay_rate_30d"), "origin", "left")
    )
    lookup_dest = mode_name("dest", "dest_airport_name", "dest_city").join(
        te("dest", "dest_te"), "dest"
    )
    names = flights.groupBy("airline", "airline_name").agg(F.count(F.lit(1)).alias("flights_total"))
    lookup_airline = names.join(rate30(["airline"], "airline_delay_rate_30d"), "airline", "left")

    spark = flights.sparkSession
    lookup_global = spark.createDataFrame(
        [(max_date, float(prior_30d), float(prior_te), int(gall["n"]))],
        "max_flight_date date, prior_30d double, prior_te double, flights_total long",
    )
    return {
        "lookup_route": lookup_route,
        "lookup_origin": lookup_origin,
        "lookup_dest": lookup_dest,
        "lookup_airline": lookup_airline,
        "lookup_global": lookup_global,
    }
