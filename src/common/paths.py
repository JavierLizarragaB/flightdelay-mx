"""Rutas del lakehouse compartidas entre la Lambda de ingesta y los Glue jobs.

La Lambda (src/ingestion/handler.py) es autocontenida y repite estas funciones; un test
verifica que ambas implementaciones coincidan.
"""

from __future__ import annotations

import re

_YM_RE = re.compile(r"^(\d{4})-(\d{2})$")


def parse_year_month(value: str) -> tuple[int, int]:
    m = _YM_RE.match(value.strip())
    if not m:
        raise ValueError(f"Formato inválido {value!r}; se espera YYYY-MM")
    year, month = int(m.group(1)), int(m.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"Mes inválido en {value!r}")
    return year, month


def expand_year_months(spec: str) -> list[tuple[int, int]]:
    """Acepta `2026-08`, `2026-07,2026-08` o un rango inclusivo `2023-01:2026-08`."""
    out: list[tuple[int, int]] = []
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        if ":" in part:
            start, end = (parse_year_month(x) for x in part.split(":", 1))
            if start > end:
                raise ValueError(f"Rango invertido: {part!r}")
            y, m = start
            while (y, m) <= end:
                out.append((y, m))
                y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        else:
            out.append(parse_year_month(part))
    if not out:
        raise ValueError("No se indicó ningún mes")
    return sorted(set(out))


def landing_prefix(year: int, month: int) -> str:
    return f"landing/year={year}/month={month:02d}/"


def landing_csv_key(year: int, month: int) -> str:
    return f"{landing_prefix(year, month)}bts_{year}_{month:02d}.csv"


def landing_manifest_key(year: int, month: int) -> str:
    return f"{landing_prefix(year, month)}_manifest.json"
