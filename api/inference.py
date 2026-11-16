"""Construye las features de un request con las MISMAS fórmulas que oro y predice.

El usuario solo manda aerolínea, ruta, fecha y hora. El resto sale de los lookups de oro
(estadísticas al cierre de los datos) y de src/gold/features.py (calendario y cíclicas).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from api.model_store import ModelBundle
from api.schemas import FlightRequest, Prediction
from src.gold import features as fx
from src.gold.feature_config import MODEL_FEATURES, assert_no_leakage
from src.ml.matrix import to_matrix


class CatalogError(ValueError):
    """Código que no existe en el catálogo del modelo -> 422 con el campo."""

    def __init__(self, field: str, detail: str) -> None:
        super().__init__(detail)
        self.field = field
        self.detail = detail


def validate_catalog(req: FlightRequest, lookups: dict[str, Any], prefix: str = "") -> None:
    if req.airline not in lookups["airlines"]:
        raise CatalogError(f"{prefix}airline", f"Aerolínea desconocida: {req.airline}")
    if req.origin not in lookups["origins"]:
        raise CatalogError(f"{prefix}origin", f"Aeropuerto de origen desconocido: {req.origin}")
    if req.dest not in lookups["dests"]:
        raise CatalogError(f"{prefix}dest", f"Aeropuerto de destino desconocido: {req.dest}")


def _first(*values: float | None) -> float | None:
    return next((v for v in values if v is not None), None)


def build_features(req: FlightRequest, lookups: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    glob = lookups["global"]
    origin = lookups["origins"][req.origin]
    dest = lookups["dests"][req.dest]
    airline = lookups["airlines"][req.airline]
    route = lookups["routes"].get(f"{req.origin}-{req.dest}")

    if route is None:
        warnings.append(
            f"La ruta {req.origin}-{req.dest} no tiene historial: se usan el promedio del "
            f"aeropuerto {req.origin} y su distancia/duración típicas."
        )
        route = {}
    elif route.get("route_delay_rate_30d") is None:
        warnings.append("La ruta no tuvo vuelos en los últimos 30 días: se usa el aeropuerto.")

    hour, minute = req.hour_minute
    hs, hc = fx.hour_cyclic(hour, minute)
    ms, mc = fx.month_cyclic(req.flight_date.month)
    feats = {
        "airline": req.airline,
        "day_of_week": fx.day_of_week(req.flight_date),
        "origin_te": _first(origin.get("origin_te"), glob["prior_te"]),
        "dest_te": _first(dest.get("dest_te"), glob["prior_te"]),
        "dep_hour_sin": hs,
        "dep_hour_cos": hc,
        "month_sin": ms,
        "month_cos": mc,
        "distance": _first(route.get("distance"), origin.get("median_distance")),
        "crs_elapsed_time": _first(
            route.get("crs_elapsed_time"), origin.get("median_crs_elapsed_time")
        ),
        "days_to_holiday": fx.days_to_holiday(req.flight_date),
        "route_delay_rate_30d": _first(
            route.get("route_delay_rate_30d"),
            origin.get("origin_delay_rate_30d"),
            glob["prior_30d"],
        ),
        "origin_delay_rate_30d": _first(origin.get("origin_delay_rate_30d"), glob["prior_30d"]),
        "airline_delay_rate_30d": _first(airline.get("airline_delay_rate_30d"), glob["prior_30d"]),
    }
    assert_no_leakage(feats)
    return feats, warnings


def risk_level(p: float, threshold: float) -> str:
    if p >= threshold:
        return "alto"
    return "medio" if p >= threshold * 0.75 else "bajo"


def predict(bundle: ModelBundle, requests: list[FlightRequest]) -> list[Prediction]:
    rows, warns = [], []
    for i, req in enumerate(requests):
        validate_catalog(req, bundle.lookups, prefix=f"flights[{i}]." if len(requests) > 1 else "")
        f, w = build_features(req, bundle.lookups)
        rows.append(f)
        warns.append(w)
    X = to_matrix(
        pd.DataFrame(rows, columns=list(MODEL_FEATURES)), bundle.metadata["airline_levels"]
    )
    proba = bundle.booster.predict(X, num_iteration=bundle.metadata.get("best_iteration"))
    out = []
    for req, p, w in zip(requests, proba, warns, strict=True):
        p = float(p)
        if not 0.0 <= p <= 1.0:  # salvaguarda: nunca devolver una probabilidad inválida
            raise RuntimeError(f"Probabilidad fuera de rango: {p}")
        out.append(
            Prediction(
                probability=round(p, 4),
                delayed=p >= bundle.threshold,
                threshold=bundle.threshold,
                risk_level=risk_level(p, bundle.threshold),
                model_version=bundle.version,
                warnings=w,
                input=req,
            )
        )
    return out
