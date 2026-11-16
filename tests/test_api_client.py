"""Tests del cliente de la app: cada escenario de falla exigido por la rúbrica."""

from __future__ import annotations

from datetime import date

import pytest
import requests

from app.api_client import (
    ApiClient,
    ApiServerError,
    ApiTimeoutError,
    ApiUnavailableError,
    ApiValidationError,
    FlightIn,
    InvalidResponseError,
)

FLIGHT = FlightIn(airline="AA", origin="DFW", dest="LAX", flight_date=date(2026, 12, 20),
                  crs_dep_time="18:30")  # fmt: skip
GOOD = {"probability": 0.31, "delayed": False, "threshold": 0.42, "risk_level": "medio",
        "model_version": "v1", "warnings": []}  # fmt: skip


class Resp:
    def __init__(self, status: int, body=None, raw: str | None = None) -> None:
        self.status_code = status
        self._body, self._raw = body, raw

    def json(self):
        if self._raw is not None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    """Devuelve (o lanza) los elementos de `script` en orden y registra las llamadas."""

    def __init__(self, *script) -> None:
        self.script = list(script)
        self.calls: list[dict] = []

    def request(self, method, url, json=None, timeout=None):
        self.calls.append({"method": method, "url": url, "json": json, "timeout": timeout})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make(*script) -> tuple[ApiClient, FakeSession, list[float]]:
    session, sleeps = FakeSession(*script), []
    c = ApiClient(
        "https://api.example/", timeout=5, retries=2, session=session, sleep=sleeps.append
    )
    return c, session, sleeps


def test_predict_ok_sends_payload_with_timeout() -> None:
    c, s, sleeps = make(Resp(200, GOOD))
    out = c.predict(FLIGHT)
    assert out.probability == 0.31 and out.risk_level == "medio"
    call = s.calls[0]
    assert call["url"] == "https://api.example/predict" and call["timeout"] == 5
    assert call["json"]["flight_date"] == "2026-12-20" and sleeps == []


def test_retries_with_exponential_backoff_then_succeeds() -> None:
    c, s, sleeps = make(requests.ConnectionError(), Resp(503, {"detail": "frío"}), Resp(200, GOOD))
    assert c.predict(FLIGHT).probability == 0.31
    assert len(s.calls) == 3 and sleeps == [0.5, 1.0]


def test_api_down_after_retries() -> None:
    c, s, _ = make(*[requests.ConnectionError()] * 3)
    with pytest.raises(ApiUnavailableError):
        c.predict(FLIGHT)
    assert len(s.calls) == 3  # 1 intento + 2 reintentos


def test_timeout() -> None:
    c, _, _ = make(*[requests.Timeout()] * 3)
    with pytest.raises(ApiTimeoutError):
        c.health()


def test_422_shows_field_and_is_not_retried() -> None:
    c, s, _ = make(Resp(422, {"error": "validation_error", "detail": "Aeropuerto desconocido",
                              "field": "origin"}))  # fmt: skip
    with pytest.raises(ApiValidationError) as e:
        c.predict(FLIGHT)
    assert e.value.field == "origin" and "desconocido" in e.value.detail
    assert len(s.calls) == 1


def test_500_is_server_error_without_retry() -> None:
    c, s, _ = make(Resp(500, {"error": "internal_error", "detail": "boom", "field": None}))
    with pytest.raises(ApiServerError) as e:
        c.predict(FLIGHT)
    assert e.value.status == 500 and len(s.calls) == 1


def test_persistent_503_raises_server_error() -> None:
    c, _, _ = make(*[Resp(503, {"detail": "modelo no disponible"})] * 3)
    with pytest.raises(ApiServerError, match="modelo no disponible"):
        c.predict(FLIGHT)


@pytest.mark.parametrize(
    "resp",
    [
        Resp(200, raw="<html>gateway</html>"),  # JSON inválido
        Resp(200, {"delayed": True}),  # faltan campos
        Resp(200, {**GOOD, "probability": 1.7}),  # fuera de [0, 1]
        Resp(200, {**GOOD, "probability": -0.1}),
        Resp(200, {**GOOD, "risk_level": "extremo"}),
    ],
)
def test_invalid_responses_are_rejected(resp) -> None:
    c, _, _ = make(resp)
    with pytest.raises(InvalidResponseError):
        c.predict(FLIGHT)


def test_catalog_must_be_a_list() -> None:
    c, _, _ = make(Resp(200, {"airlines": []}))
    with pytest.raises(InvalidResponseError):
        c.airlines()


def test_batch_count_mismatch_rejected() -> None:
    c, _, _ = make(Resp(200, {"predictions": [GOOD], "model_version": "v1"}))
    with pytest.raises(InvalidResponseError, match="distinto"):
        c.predict_batch([FLIGHT, FLIGHT])


def test_requires_api_url(monkeypatch) -> None:
    monkeypatch.delenv("API_URL", raising=False)
    with pytest.raises(ValueError, match="API_URL"):
        ApiClient()
