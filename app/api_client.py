"""Cliente HTTP de la API publicada. La app NUNCA carga el modelo: todo pasa por aquí.

Robustez (rúbrica, criterio 4):
  - timeout de 5 s por intento (API_TIMEOUT_SECONDS)
  - 2 reintentos con backoff exponencial (0.5 s, 1 s) ante caída de conexión, timeout y
    5xx transitorios (502/503/504). Un 4xx NO se reintenta: repetirlo daría lo mismo.
  - cada respuesta se valida con Pydantic del lado cliente: JSON inválido, campos
    faltantes o probabilidad fuera de [0, 1] -> InvalidResponseError (no se muestra basura).
"""

from __future__ import annotations

import os
import time
from datetime import date
from typing import Any, Literal

import requests
from pydantic import BaseModel, Field, ValidationError

RETRYABLE_STATUS = {502, 503, 504}


# --------------------------------------------------------------------------- errores
class ApiError(Exception):
    user_message = "Error al consultar el servicio de predicción."

    def __init__(self, detail: str = "") -> None:
        super().__init__(detail or self.user_message)
        self.detail = detail or self.user_message


class ApiUnavailableError(ApiError):
    user_message = "El servicio de predicción no está disponible (no responde). Intenta más tarde."


class ApiTimeoutError(ApiError):
    user_message = "El servicio tardó demasiado en responder (timeout). Intenta de nuevo."


class ApiValidationError(ApiError):
    user_message = "Algún dato del vuelo no es válido."

    def __init__(self, detail: str, field: str | None) -> None:
        super().__init__(detail)
        self.field = field


class ApiServerError(ApiError):
    user_message = "El servicio tuvo un error interno."

    def __init__(self, detail: str, status: int) -> None:
        super().__init__(detail)
        self.status = status


class InvalidResponseError(ApiError):
    user_message = "El servicio respondió con datos inválidos; no se muestran."


# --------------------------------------------------------------------------- contratos
class FlightIn(BaseModel):
    airline: str
    origin: str
    dest: str
    flight_date: date
    crs_dep_time: str


class PredictionOut(BaseModel):
    probability: float = Field(..., ge=0.0, le=1.0)
    delayed: bool
    threshold: float = Field(..., ge=0.0, le=1.0)
    risk_level: Literal["bajo", "medio", "alto"]
    model_version: str
    warnings: list[str] = []


class BatchOut(BaseModel):
    predictions: list[PredictionOut]
    model_version: str


class AirlineOut(BaseModel):
    code: str
    name: str


class AirportOut(BaseModel):
    code: str
    name: str
    city: str | None = None


class HealthOut(BaseModel):
    status: str
    model_loaded: bool
    model_version: str | None = None
    detail: str | None = None


# --------------------------------------------------------------------------- cliente
class ApiClient:
    def __init__(
        self,
        base_url: str | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float = 0.5,
        session: Any | None = None,
        sleep=time.sleep,
    ) -> None:
        url = base_url or os.environ.get("API_URL", "")
        if not url:
            raise ValueError("Falta API_URL")
        self.base_url = url.rstrip("/")
        self.timeout = timeout or float(os.environ.get("API_TIMEOUT_SECONDS", "5"))
        self.retries = int(os.environ.get("API_MAX_RETRIES", "2")) if retries is None else retries
        self.backoff = backoff
        self.session = session or requests.Session()
        self._sleep = sleep

    def _request(self, method: str, path: str, json: Any = None) -> Any:
        last: ApiError | None = None
        for attempt in range(self.retries + 1):
            if attempt:
                self._sleep(self.backoff * 2 ** (attempt - 1))
            try:
                resp = self.session.request(
                    method, f"{self.base_url}{path}", json=json, timeout=self.timeout
                )
            except requests.Timeout:
                last = ApiTimeoutError()
                continue
            except requests.ConnectionError:
                last = ApiUnavailableError()
                continue
            if resp.status_code in RETRYABLE_STATUS:
                last = ApiServerError(self._error_detail(resp), resp.status_code)
                continue
            return self._handle(resp)
        assert last is not None
        raise last

    @staticmethod
    def _error_detail(resp: Any) -> str:
        try:
            body = resp.json()
            return str(body.get("detail") or body)
        except ValueError:
            return f"HTTP {resp.status_code}"

    def _handle(self, resp: Any) -> Any:
        if resp.status_code == 422:
            try:
                body = resp.json()
                raise ApiValidationError(
                    str(body.get("detail", "Dato inválido")), body.get("field")
                )
            except ValueError as e:
                raise InvalidResponseError("422 sin JSON válido") from e
        if resp.status_code >= 500:
            raise ApiServerError(self._error_detail(resp), resp.status_code)
        if resp.status_code >= 400:
            raise ApiError(f"HTTP {resp.status_code}: {self._error_detail(resp)}")
        try:
            return resp.json()
        except ValueError as e:
            raise InvalidResponseError("La respuesta no es JSON válido") from e

    @staticmethod
    def _parse(model: type[BaseModel], data: Any) -> Any:
        try:
            return model.model_validate(data)
        except ValidationError as e:
            raise InvalidResponseError(
                f"Respuesta con formato inesperado: {e.errors()[0]['msg']}"
            ) from e

    # ------------------------------------------------------------------ endpoints
    def health(self) -> HealthOut:
        return self._parse(HealthOut, self._request("GET", "/health"))

    def airlines(self) -> list[AirlineOut]:
        data = self._request("GET", "/catalog/airlines")
        if not isinstance(data, list):
            raise InvalidResponseError("El catálogo de aerolíneas no es una lista")
        return [self._parse(AirlineOut, a) for a in data]

    def airports(self) -> list[AirportOut]:
        data = self._request("GET", "/catalog/airports")
        if not isinstance(data, list):
            raise InvalidResponseError("El catálogo de aeropuertos no es una lista")
        return [self._parse(AirportOut, a) for a in data]

    def metadata(self) -> dict[str, Any]:
        data = self._request("GET", "/metadata")
        if not isinstance(data, dict) or "model_version" not in data:
            raise InvalidResponseError("Metadata sin model_version")
        return data

    def predict(self, flight: FlightIn) -> PredictionOut:
        return self._parse(
            PredictionOut, self._request("POST", "/predict", flight.model_dump(mode="json"))
        )

    def predict_batch(self, flights: list[FlightIn]) -> BatchOut:
        payload = {"flights": [f.model_dump(mode="json") for f in flights]}
        out = self._parse(BatchOut, self._request("POST", "/predict/batch", payload))
        if len(out.predictions) != len(flights):
            raise InvalidResponseError("El lote devolvió un número distinto de predicciones")
        return out
