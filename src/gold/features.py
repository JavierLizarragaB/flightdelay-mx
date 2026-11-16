"""Fórmulas de features en Python puro, compartidas por oro (Spark) y la API (inferencia).

Tener una sola definición evita el *training/serving skew*: si la API calculara la hora
cíclica o los días al feriado de otra forma que el job de oro, el modelo recibiría en
producción features distintas a las que vio al entrenar. `tests/test_gold.py` compara
estas funciones contra las expresiones de Spark de src/glue_jobs/gold.py.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from functools import lru_cache

HOLIDAY_CAP_DAYS = 30  # más allá de un mes el efecto de un feriado es nulo
SMOOTHING_30D = 20  # pseudo-vuelos hacia la tasa global en las tasas de 30 días
SMOOTHING_TE = 50  # pseudo-vuelos hacia la media global en el target encoding


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """n-ésimo `weekday` (0 = lunes) del mes; n = -1 es el último."""
    if n > 0:
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


@lru_cache(maxsize=64)
def us_federal_holidays(year: int) -> tuple[date, ...]:
    """Feriados federales de EE. UU. (5 U.S.C. 6103), en su fecha nominal."""
    return tuple(
        sorted(
            [
                date(year, 1, 1),  # Año Nuevo
                _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr.
                _nth_weekday(year, 2, 0, 3),  # Presidents' Day
                _nth_weekday(year, 5, 0, -1),  # Memorial Day
                date(year, 6, 19),  # Juneteenth
                date(year, 7, 4),  # Independencia
                _nth_weekday(year, 9, 0, 1),  # Labor Day
                _nth_weekday(year, 10, 0, 2),  # Columbus Day
                date(year, 11, 11),  # Veterans Day
                _nth_weekday(year, 11, 3, 4),  # Thanksgiving
                date(year, 12, 25),  # Navidad
            ]
        )
    )


def holidays_between(start_year: int, end_year: int) -> list[date]:
    return [h for y in range(start_year, end_year + 1) for h in us_federal_holidays(y)]


def days_to_holiday(d: date) -> int:
    """Días al feriado federal más cercano (antes o después), con tope de 30."""
    nearest = min(abs((h - d).days) for h in holidays_between(d.year - 1, d.year + 1))
    return min(nearest, HOLIDAY_CAP_DAYS)


def hour_cyclic(hour: int, minute: int) -> tuple[float, float]:
    """Hora del día en el círculo: 23:50 queda junto a 00:10 (una hora lineal no lo ve)."""
    frac = (hour + minute / 60.0) / 24.0
    return math.sin(2 * math.pi * frac), math.cos(2 * math.pi * frac)


def month_cyclic(month: int) -> tuple[float, float]:
    """Diciembre queda junto a enero."""
    frac = (month - 1) / 12.0
    return math.sin(2 * math.pi * frac), math.cos(2 * math.pi * frac)


def day_of_week(d: date) -> int:
    """Convención de BTS: 1 = lunes … 7 = domingo."""
    return d.isoweekday()


def smoothed_rate(positives: float, n: float, prior: float, k: float) -> float:
    """(positivos + k·prior) / (n + k): con pocos vuelos tiende a la media global."""
    return (positives + k * prior) / (n + k)
