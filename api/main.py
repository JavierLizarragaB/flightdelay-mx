"""FlightDelay-MX API: probabilidad de que un vuelo doméstico de EE. UU. llegue ≥ 15 min tarde.

Swagger UI en /docs, ReDoc en /redoc, esquema en /openapi.json.
Todos los errores responden con el mismo formato: {"error", "detail", "field"}.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api import model_store
from api.inference import CatalogError, predict
from api.schemas import (
    Airline,
    Airport,
    BatchRequest,
    BatchResponse,
    ErrorResponse,
    FlightRequest,
    Health,
    Prediction,
)

logging.getLogger().setLevel(logging.INFO)

ERRORS = {
    422: {"model": ErrorResponse, "description": "Entrada inválida"},
    503: {"model": ErrorResponse, "description": "Modelo no disponible"},
    500: {"model": ErrorResponse, "description": "Error interno"},
}

app = FastAPI(
    title="FlightDelay-MX API",
    version="1.0.0",
    description=(
        "Predice la probabilidad de que un vuelo doméstico de EE. UU. llegue con **15 minutos "
        "o más** de retraso, usando solo información conocida al comprar el boleto "
        "(aerolínea, ruta, fecha y hora). Modelo LightGBM entrenado con datos BTS "
        "On-Time Performance (2023 → 2026) procesados en una arquitectura Medallion en AWS."
    ),
)


# --------------------------------------------------------------------------- errores
def error(status: int, kind: str, detail: str, field: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status, content={"error": kind, "detail": detail, "field": field}
    )


@app.exception_handler(RequestValidationError)
async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
    first = exc.errors()[0]
    loc = [str(p) for p in first.get("loc", []) if p != "body"]
    field = ".".join(loc).replace(".0.", "[0].") if loc else None
    if loc and loc[0] == "flights" and len(loc) > 2 and loc[1].isdigit():
        field = f"flights[{loc[1]}]." + ".".join(loc[2:])
    msg = first.get("msg", "Entrada inválida").removeprefix("Value error, ")
    return error(422, "validation_error", msg, field)


@app.exception_handler(StarletteHTTPException)
async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    kind = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
    return error(exc.status_code, kind, str(exc.detail))


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
    logging.exception("Error no controlado")
    return error(500, "internal_error", "Error interno del servidor")


def _bundle_or_503():
    bundle = model_store.get_bundle()
    if bundle is None:
        return None, error(503, "model_unavailable",
                           f"El modelo no está disponible: {model_store.last_error()}")  # fmt: skip
    return bundle, None


# --------------------------------------------------------------------------- endpoints
@app.get("/health", response_model=Health, tags=["sistema"])
def health() -> Health:
    """Estado del servicio y del modelo cargado."""
    bundle = model_store.get_bundle()
    if bundle is None:
        return Health(status="degraded", model_loaded=False, detail=model_store.last_error())
    return Health(status="ok", model_loaded=True, model_version=bundle.version)


@app.get("/metadata", tags=["sistema"], responses={503: ERRORS[503]})
def metadata():
    """Versión, umbral, periodo de datos, features y métricas del modelo publicado."""
    bundle, err = _bundle_or_503()
    if err:
        return err
    md = bundle.metadata
    lgbm = md["metrics"]["lightgbm"]
    return {
        "model_version": md["version"],
        "model_type": md["model_type"],
        "trained_at": md["trained_at"],
        "target": md["target"],
        "primary_metric": md["primary_metric"],
        "threshold": md["threshold"],
        "features": md["features"],
        "splits": md["splits"],
        "lookups_as_of": md["lookups_as_of"],
        "metrics_test": lgbm["test"],
        "metrics_valid": lgbm["valid"],
        "baselines_test": {
            "always_on_time": md["metrics"]["baseline_always_on_time"]["test"],
            "logistic_regression": md["metrics"]["logistic_regression"]["test"],
        },
        "feature_importance_gain": md["feature_importance_gain"],
    }


@app.get("/catalog/airlines", response_model=list[Airline], tags=["catálogos"],
         responses={503: ERRORS[503]})  # fmt: skip
def catalog_airlines():
    """Aerolíneas que el modelo conoce (con vuelos en los datos de entrenamiento)."""
    bundle, err = _bundle_or_503()
    if err:
        return err
    return sorted(
        (
            Airline(code=k, name=v.get("airline_name") or k)
            for k, v in bundle.lookups["airlines"].items()
        ),
        key=lambda a: a.name,
    )


@app.get("/catalog/airports", response_model=list[Airport], tags=["catálogos"],
         responses={503: ERRORS[503]})  # fmt: skip
def catalog_airports():
    """Aeropuertos de origen que el modelo conoce."""
    bundle, err = _bundle_or_503()
    if err:
        return err
    return sorted(
        (Airport(code=k, name=v.get("airport_name") or k, city=v.get("city"))
         for k, v in bundle.lookups["origins"].items()),
        key=lambda a: a.code,
    )  # fmt: skip


@app.post("/predict", response_model=Prediction, tags=["predicción"], responses=ERRORS)
def predict_one(req: FlightRequest):
    """Probabilidad de retraso ≥ 15 min para un vuelo."""
    bundle, err = _bundle_or_503()
    if err:
        return err
    try:
        return predict(bundle, [req])[0]
    except CatalogError as e:
        return error(422, "validation_error", e.detail, e.field)


@app.post("/predict/batch", response_model=BatchResponse, tags=["predicción"], responses=ERRORS)
def predict_batch(req: BatchRequest):
    """Hasta 100 vuelos por llamada. Si uno es inválido, responde 422 indicando cuál."""
    bundle, err = _bundle_or_503()
    if err:
        return err
    try:
        return BatchResponse(predictions=predict(bundle, req.flights), model_version=bundle.version)
    except CatalogError as e:
        return error(422, "validation_error", e.detail, e.field)
