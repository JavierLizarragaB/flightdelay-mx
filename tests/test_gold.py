"""Tests de oro: sin fuga temporal, fórmulas idénticas a la API y lookups coherentes."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from src.glue_jobs import gold as g
from src.gold import features as fx
from src.gold.feature_config import MODEL_FEATURES, TARGET, is_forbidden

K30, KTE = fx.SMOOTHING_30D, fx.SMOOTHING_TE


# ------------------------------------------------------------------ Python puro
@pytest.mark.parametrize(
    ("year", "expected"),
    [
        (2026, {date(2026, 1, 19), date(2026, 5, 25), date(2026, 9, 7), date(2026, 11, 26)}),
        (2024, {date(2024, 1, 15), date(2024, 5, 27), date(2024, 11, 28), date(2024, 10, 14)}),
    ],
)
def test_federal_holidays(year: int, expected: set[date]) -> None:
    hol = set(fx.us_federal_holidays(year))
    assert len(hol) == 11 and expected <= hol


def test_days_to_holiday() -> None:
    assert fx.days_to_holiday(date(2026, 7, 4)) == 0
    assert fx.days_to_holiday(date(2026, 7, 1)) == 3
    assert fx.days_to_holiday(date(2026, 12, 30)) == 2  # año nuevo siguiente
    assert fx.days_to_holiday(date(2026, 8, 15)) == 23  # Labor Day (7 sep)
    assert fx.days_to_holiday(date(2026, 3, 20)) == fx.HOLIDAY_CAP_DAYS  # tope


def test_cyclic_wraps_around() -> None:
    late, early = fx.hour_cyclic(23, 50), fx.hour_cyclic(0, 10)
    assert math.dist(late, early) < 0.1  # 23:50 y 00:10 quedan cerca
    assert math.dist(fx.month_cyclic(12), fx.month_cyclic(1)) < 0.6


def test_model_features_have_no_leakage() -> None:
    assert not [f for f in MODEL_FEATURES if is_forbidden(f)]


# ------------------------------------------------------------------ Spark
pytestmark_spark = pytest.mark.spark

D0 = date(2026, 1, 1)


def flight(spark_rows, d: date, origin="LAX", dest="JFK", airline="AA", y=1, hour=8, minute=0):
    spark_rows.append(
        {
            "flight_date": d,
            "year": d.year,
            "month": d.month,
            "day_of_week": d.isoweekday(),
            "airline": airline,
            "airline_name": f"{airline} Airlines",
            "origin": origin,
            "origin_airport_name": f"{origin} Intl",
            "origin_city": f"{origin} City",
            "dest": dest,
            "dest_airport_name": f"{dest} Intl",
            "dest_city": f"{dest} City",
            "dep_hour": hour,
            "dep_minute": minute,
            "distance": 1000.0,
            "crs_elapsed_time": 180.0,
            "arr_del15": y,
            "_year": d.year,
            "_month": d.month,
        }
    )


def to_df(spark, rows):
    return spark.createDataFrame(rows)


@pytest.fixture
def holidays(spark):
    return g.holidays_df(spark, 2025, 2027)


@pytestmark_spark
def test_calendar_features_match_python(spark, holidays) -> None:
    rows: list[dict] = []
    cases = [(date(2026, 7, 1), 0, 0), (date(2026, 12, 30), 23, 50), (date(2026, 3, 20), 13, 15)]
    for d, h, m in cases:
        flight(rows, d, hour=h, minute=m)
    out = g.add_days_to_holiday(g.add_calendar_features(to_df(spark, rows)), holidays)
    for r in out.collect():
        hs, hc = fx.hour_cyclic(r.dep_hour, r.dep_minute)
        ms, mc = fx.month_cyclic(r.month)
        assert r.dep_hour_sin == pytest.approx(hs) and r.dep_hour_cos == pytest.approx(hc)
        assert r.month_sin == pytest.approx(ms) and r.month_cos == pytest.approx(mc)
        assert r.days_to_holiday == fx.days_to_holiday(r.flight_date)


@pytestmark_spark
def test_rate_30d_uses_only_previous_days(spark, holidays) -> None:
    rows: list[dict] = []
    flight(rows, D0, y=1)  # día 0: 1 retraso en LAX-JFK
    flight(rows, D0, origin="SFO", y=0)  # día 0: 1 a tiempo en otra ruta
    flight(rows, D0 + timedelta(days=1), y=0)  # día 1: su propia etiqueta no debe contar
    flight(rows, D0 + timedelta(days=40), y=0)  # día 40: el día 0 ya salió de la ventana
    feats = {
        (r.origin, r.flight_date): r
        for r in g.build_features(to_df(spark, rows), holidays).collect()
    }

    assert feats[("LAX", D0)].route_delay_rate_30d is None  # sin historia: no inventa
    day1 = feats[("LAX", D0 + timedelta(days=1))]
    prior = 1 / 2  # día 0 global: 1 retraso de 2 vuelos
    assert day1.route_delay_rate_30d == pytest.approx((1 + K30 * prior) / (1 + K30))
    assert day1.origin_delay_rate_30d == pytest.approx((1 + K30 * prior) / (1 + K30))
    # Día 40: ventana [10, 39] vacía -> sin historial para ruta ni global.
    assert feats[("LAX", D0 + timedelta(days=40))].route_delay_rate_30d is None


@pytestmark_spark
def test_target_encoding_uses_only_previous_months(spark, holidays) -> None:
    rows: list[dict] = []
    for _ in range(3):
        flight(rows, date(2026, 1, 10), origin="ORD", y=1)
    flight(rows, date(2026, 1, 10), origin="ATL", y=0)
    flight(rows, date(2026, 2, 10), origin="ORD", y=0)
    flight(rows, date(2026, 3, 10), origin="ORD", y=0)
    feats = g.build_features(to_df(spark, rows), holidays)
    te = {(r.origin, r.flight_date.month): r.origin_te for r in feats.collect()}

    assert te[("ORD", 1)] is None  # enero: sin meses previos
    prior_jan = 3 / 4
    assert te[("ORD", 2)] == pytest.approx((3 + KTE * prior_jan) / (3 + KTE))
    prior_feb = 3 / 5  # ene + feb
    assert te[("ORD", 3)] == pytest.approx((3 + KTE * prior_feb) / (4 + KTE))


@pytestmark_spark
def test_build_features_schema(spark, holidays) -> None:
    rows: list[dict] = []
    for i in range(5):
        flight(rows, D0 + timedelta(days=i), y=i % 2)
    feats = g.build_features(to_df(spark, rows), holidays)
    assert feats.columns == g.GOLD_COLUMNS
    assert feats.count() == 5
    # La única columna "post-vuelo" permitida en oro es el objetivo.
    assert [c for c in feats.columns if is_forbidden(c)] == [TARGET]


@pytestmark_spark
def test_lookups(spark) -> None:
    rows: list[dict] = []
    for i in range(10):
        flight(rows, D0 + timedelta(days=i), y=1 if i < 4 else 0)
    flight(rows, D0 + timedelta(days=9), origin="SFO", dest="SEA", airline="AS", y=1)
    lk = g.build_lookups(to_df(spark, rows))

    glob = lk["lookup_global"].first()
    assert glob.max_flight_date == D0 + timedelta(days=9) and glob.flights_total == 11
    prior = 5 / 11
    route = lk["lookup_route"].where("origin = 'LAX'").first()
    assert route.route_delay_rate_30d == pytest.approx((4 + K30 * prior) / (10 + K30))
    assert route.distance == 1000.0 and route.crs_elapsed_time == 180.0
    origin = lk["lookup_origin"].where("origin = 'LAX'").first()
    assert origin.airport_name == "LAX Intl" and origin.city == "LAX City"
    assert origin.origin_te == pytest.approx((4 + KTE * prior) / (10 + KTE))
    airline = lk["lookup_airline"].where("airline = 'AS'").first()
    assert airline.airline_name == "AS Airlines" and airline.airline_delay_rate_30d_n == 1
    assert set(lk) == {"lookup_route", "lookup_origin", "lookup_dest", "lookup_airline",
                       "lookup_global"}  # fmt: skip
