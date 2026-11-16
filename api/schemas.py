"""Contratos de la API (Pydantic v2). Validan formato; el catálogo se valida en inference."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_DAYS_AHEAD = 365
MAX_BATCH = 100
_HHMM = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
_CODE = re.compile(r"^[A-Z0-9]{2,3}$")


class FlightRequest(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        json_schema_extra={
            "examples": [
                {
                    "airline": "AA",
                    "origin": "DFW",
                    "dest": "LAX",
                    "flight_date": (date.today() + timedelta(days=30)).isoformat(),
                    "crs_dep_time": "18:30",
                }
            ]
        },
    )

    airline: str = Field(..., description="Código de aerolínea BTS/IATA (p. ej. AA, DL, WN)")
    origin: str = Field(..., description="Aeropuerto de origen, código IATA (p. ej. DFW)")
    dest: str = Field(..., description="Aeropuerto de destino, código IATA (p. ej. LAX)")
    flight_date: date = Field(..., description="Fecha del vuelo (YYYY-MM-DD), hoy a +365 días")
    crs_dep_time: str = Field(..., description="Hora programada de salida, local (HH:MM)")

    @field_validator("airline", "origin", "dest", mode="before")
    @classmethod
    def _upper_code(cls, v: object) -> object:
        if isinstance(v, str):
            v = v.strip().upper()
            if not _CODE.match(v):
                raise ValueError("debe ser un código de 2-3 letras/dígitos (p. ej. AA, DFW)")
        return v

    @field_validator("crs_dep_time")
    @classmethod
    def _valid_time(cls, v: str) -> str:
        if not _HHMM.match(v):
            raise ValueError("hora inválida; formato HH:MM de 00:00 a 23:59")
        return v

    @field_validator("flight_date")
    @classmethod
    def _date_window(cls, v: date) -> date:
        today = date.today()
        if v < today:
            raise ValueError("la fecha no puede estar en el pasado")
        if v > today + timedelta(days=MAX_DAYS_AHEAD):
            raise ValueError(f"la fecha no puede ser mayor a {MAX_DAYS_AHEAD} días")
        return v

    @model_validator(mode="after")
    def _distinct_airports(self) -> FlightRequest:
        if self.origin == self.dest:
            raise ValueError("origin y dest deben ser distintos")
        return self

    @property
    def hour_minute(self) -> tuple[int, int]:
        h, m = self.crs_dep_time.split(":")
        return int(h), int(m)


class BatchRequest(BaseModel):
    flights: list[FlightRequest] = Field(..., min_length=1, max_length=MAX_BATCH)


class Prediction(BaseModel):
    probability: float = Field(..., ge=0.0, le=1.0, description="P(llegar ≥ 15 min tarde)")
    delayed: bool = Field(..., description="probability ≥ umbral del modelo")
    threshold: float
    risk_level: Literal["bajo", "medio", "alto"]
    model_version: str
    warnings: list[str] = []
    input: FlightRequest


class BatchResponse(BaseModel):
    predictions: list[Prediction]
    model_version: str


class ErrorResponse(BaseModel):
    error: str = Field(..., description="Tipo de error (validation_error, not_found, …)")
    detail: str = Field(..., description="Mensaje legible")
    field: str | None = Field(None, description="Campo que causó el error, si aplica")


class Airline(BaseModel):
    code: str
    name: str


class Airport(BaseModel):
    code: str
    name: str
    city: str | None = None


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    model_loaded: bool
    model_version: str | None = None
    detail: str | None = None
