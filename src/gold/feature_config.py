"""Fuente única de verdad sobre qué columnas puede ver el modelo.

Regla: solo se usan como features variables conocidas *al momento de comprar el boleto*.
Todo lo que se mide durante o después del vuelo es fuga de datos (data leakage).

La lista prohibida se construyó a partir del encabezado real de BTS (110 columnas,
verificado con el archivo de agosto 2026). Incluye columnas que no aparecen en la lista
inicial del README, como `DepDelayMinutes`, `ArrivalDelayGroups`, `FirstDepTime` o las
`Div*` de desvíos: todas se conocen solo después del vuelo.

`assert_no_leakage()` debe llamarse en el entrenamiento y en la API justo antes de
construir la matriz de features. `tests/test_feature_config.py` falla si alguien
agrega una columna prohibida a `MODEL_FEATURES`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

# --- Objetivo ---------------------------------------------------------------------------
TARGET = "arr_del15"

# --- Features del modelo (nombres de la tabla gold.features_flights) ------------------
CATEGORICAL_FEATURES: tuple[str, ...] = (
    "airline",
    "day_of_week",
)

# Codificadas con target encoding suavizado (alta cardinalidad, ~350 aeropuertos).
TARGET_ENCODED_FEATURES: tuple[str, ...] = (
    "origin_te",
    "dest_te",
)

NUMERIC_FEATURES: tuple[str, ...] = (
    "dep_hour_sin",
    "dep_hour_cos",
    "month_sin",
    "month_cos",
    "distance",
    "crs_elapsed_time",
    "days_to_holiday",
    # Tasas históricas con ventana desfasada (solo ven días estrictamente anteriores).
    "route_delay_rate_30d",
    "origin_delay_rate_30d",
    "airline_delay_rate_30d",
)

MODEL_FEATURES: tuple[str, ...] = CATEGORICAL_FEATURES + TARGET_ENCODED_FEATURES + NUMERIC_FEATURES

# --- Columnas de la fuente BTS que nunca pueden ser feature ---------------------------
FORBIDDEN_SOURCE_COLUMNS: frozenset[str] = frozenset(
    {
        # Salida real
        "DepTime",
        "DepDelay",
        "DepDelayMinutes",
        "DepDel15",
        "DepartureDelayGroups",
        "TaxiOut",
        "WheelsOff",
        # Llegada real
        "WheelsOn",
        "TaxiIn",
        "ArrTime",
        "ArrDelay",
        "ArrDelayMinutes",
        "ArrDel15",  # es el objetivo; nunca entra como feature
        "ArrivalDelayGroups",
        "ActualElapsedTime",
        "AirTime",
        # Causas de retraso (se llenan solo si hubo retraso >= 15 min)
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
        # Resultado operativo
        "Cancelled",
        "CancellationCode",
        "Diverted",
        "DivAirportLandings",
        "DivReachedDest",
        "DivActualElapsedTime",
        "DivArrDelay",
        "DivDistance",
        # Regreso a puerta / tiempo extra en tierra
        "FirstDepTime",
        "TotalAddGTime",
        "LongestAddGTime",
    }
)

# Div1Airport, Div2WheelsOn, ... Div5TailNum (desvíos, post-vuelo).
_FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (re.compile(r"^div\d"),)

# Columnas permitidas en plata pero que no deben usarse como feature por otras razones
# (identificadores: el modelo memorizaría vuelos concretos en vez de generalizar).
NON_FEATURE_IDENTIFIERS: frozenset[str] = frozenset(
    {"Tail_Number", "Flight_Number_Reporting_Airline"}
)


def _normalize(name: str) -> str:
    """`DepDelay`, `dep_delay` y `DEP-DELAY` se comparan igual."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


_FORBIDDEN_NORMALIZED: frozenset[str] = frozenset(
    _normalize(c) for c in FORBIDDEN_SOURCE_COLUMNS | NON_FEATURE_IDENTIFIERS
)


def is_forbidden(column: str) -> bool:
    """True si la columna (en cualquier convención de nombres) es fuga de datos."""
    norm = _normalize(column)
    return norm in _FORBIDDEN_NORMALIZED or any(p.match(norm) for p in _FORBIDDEN_PATTERNS)


class LeakageError(ValueError):
    """Se intentó entrenar o inferir con una columna prohibida."""


def assert_no_leakage(columns: Iterable[str]) -> None:
    """Lanza `LeakageError` si alguna columna está prohibida o es el objetivo."""
    cols = list(columns)
    bad = sorted(c for c in cols if is_forbidden(c) or _normalize(c) == _normalize(TARGET))
    if bad:
        raise LeakageError(f"Columnas con fuga de datos en las features: {bad}")
