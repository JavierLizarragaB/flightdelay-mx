"""Tests de la API con un modelo diminuto entrenado al vuelo (sin AWS)."""

from __future__ import annotations

import json
from datetime import date, timedelta

import lightgbm as lgb
import numpy as np
import pytest
from fastapi.testclient import TestClient

from api import model_store
from api.inference import build_features
from api.main import app
from api.schemas import FlightRequest
from src.gold import features as fx
from src.gold.feature_config import MODEL_FEATURES
from src.ml.matrix import categorical_indices

FUTURE = (date.today() + timedelta(days=30)).isoformat()
OK = {
    "airline": "AA",
    "origin": "DFW",
    "dest": "LAX",
    "flight_date": FUTURE,
    "crs_dep_time": "18:30",
}

LOOKUPS = {
    "global": {"max_flight_date": "2026-08-31", "prior_30d": 0.27, "prior_te": 0.21},
    "routes": {
        "DFW-LAX": {"distance": 1235.0, "crs_elapsed_time": 215.0, "route_delay_rate_30d": 0.31},
        "LAX-DFW": {"distance": 1235.0, "crs_elapsed_time": 190.0, "route_delay_rate_30d": None},
    },
    "origins": {
        "DFW": {"airport_name": "Dallas/Fort Worth International", "city": "Dallas/Fort Worth, TX",
                "origin_te": 0.24, "origin_delay_rate_30d": 0.30, "median_distance": 900.0,
                "median_crs_elapsed_time": 150.0},
        "LAX": {"airport_name": "Los Angeles International", "city": "Los Angeles, CA",
                "origin_te": 0.20, "origin_delay_rate_30d": 0.22, "median_distance": 1100.0,
                "median_crs_elapsed_time": 170.0},
        "SEA": {"airport_name": "Seattle/Tacoma International", "city": "Seattle, WA",
                "origin_te": 0.18, "origin_delay_rate_30d": None, "median_distance": 1000.0,
                "median_crs_elapsed_time": 160.0},
    },
    "dests": {"LAX": {"dest_te": 0.19}, "DFW": {"dest_te": 0.25}, "SEA": {"dest_te": 0.17}},
    "airlines": {"AA": {"airline_name": "American Airlines Inc.", "airline_delay_rate_30d": 0.28},
                 "DL": {"airline_name": "Delta Air Lines Inc.", "airline_delay_rate_30d": 0.2}},
}  # fmt: skip


@pytest.fixture(scope="module")
def model_dir(tmp_path_factory):
    rng = np.random.default_rng(0)
    X = rng.random((400, len(MODEL_FEATURES))).astype(np.float32)
    X[:, MODEL_FEATURES.index("airline")] = rng.integers(0, 2, 400)
    X[:, MODEL_FEATURES.index("day_of_week")] = rng.integers(1, 8, 400)
    y = (X[:, MODEL_FEATURES.index("route_delay_rate_30d")] > 0.5).astype(int)
    booster = lgb.train(
        {"objective": "binary", "verbose": -1, "min_data_in_leaf": 5},
        lgb.Dataset(
            X, y, feature_name=list(MODEL_FEATURES), categorical_feature=categorical_indices()
        ),
        num_boost_round=10,
    )
    d = tmp_path_factory.mktemp("model")
    booster.save_model(str(d / "model.txt"))
    metrics = {"f1": 0.5, "pr_auc": 0.5}
    (d / "metadata.json").write_text(json.dumps({
        "version": "vtest", "model_type": "lightgbm", "trained_at": "2026-11-16T00:00:00",
        "target": "arr_del15", "primary_metric": "f1_positive_class", "threshold": 0.4,
        "features": list(MODEL_FEATURES), "airline_levels": ["AA", "DL"], "best_iteration": 10,
        "splits": {}, "lookups_as_of": "2026-08-31", "feature_importance_gain": {},
        "metrics": {"lightgbm": {"test": metrics, "valid": metrics},
                    "baseline_always_on_time": {"test": {"f1": 0.0}},
                    "logistic_regression": {"test": metrics}},
    }))  # fmt: skip
    (d / "lookups.json").write_text(json.dumps(LOOKUPS))
    return d


@pytest.fixture
def client(model_dir, monkeypatch):
    monkeypatch.setenv("MODEL_DIR", str(model_dir))
    model_store.reset()
    yield TestClient(app)
    model_store.reset()


# ------------------------------------------------------------------ sistema y catálogos
def test_health(client) -> None:
    assert client.get("/health").json() == {"status": "ok", "model_loaded": True,
                                            "model_version": "vtest", "detail": None}  # fmt: skip


def test_metadata(client) -> None:
    body = client.get("/metadata").json()
    assert body["model_version"] == "vtest" and body["threshold"] == 0.4
    assert body["features"] == list(MODEL_FEATURES)


def test_catalogs(client) -> None:
    airlines = client.get("/catalog/airlines").json()
    assert {"code": "AA", "name": "American Airlines Inc."} in airlines
    airports = client.get("/catalog/airports").json()
    assert [a["code"] for a in airports] == ["DFW", "LAX", "SEA"]


def test_openapi_documents_endpoints(client) -> None:
    paths = client.get("/openapi.json").json()["paths"]
    assert {"/health", "/metadata", "/catalog/airlines", "/catalog/airports", "/predict",
            "/predict/batch"} <= set(paths)  # fmt: skip
    assert client.get("/docs").status_code == 200


# ------------------------------------------------------------------ predicción
def test_predict_ok(client) -> None:
    r = client.post("/predict", json=OK)
    assert r.status_code == 200
    body = r.json()
    assert 0 <= body["probability"] <= 1
    assert body["delayed"] == (body["probability"] >= 0.4)
    assert body["risk_level"] in {"bajo", "medio", "alto"} and body["warnings"] == []
    assert body["input"]["origin"] == "DFW"


def test_predict_normalizes_codes(client) -> None:
    r = client.post("/predict", json={**OK, "airline": " aa ", "origin": "dfw", "dest": "lax"})
    assert r.status_code == 200 and r.json()["input"]["airline"] == "AA"


def test_route_without_history_falls_back_with_warning(client) -> None:
    r = client.post("/predict", json={**OK, "origin": "SEA", "dest": "DFW"})
    assert r.status_code == 200
    assert "no tiene historial" in r.json()["warnings"][0]


def test_batch(client) -> None:
    r = client.post("/predict/batch", json={"flights": [OK, {**OK, "airline": "DL"}]})
    assert r.status_code == 200 and len(r.json()["predictions"]) == 2


# ------------------------------------------------------------------ validación (422 uniforme)
@pytest.mark.parametrize(
    ("override", "field", "fragment"),
    [
        ({"airline": "XX"}, "airline", "desconocida"),
        ({"origin": "ZZZ"}, "origin", "desconocido"),
        ({"dest": "QQQ"}, "dest", "desconocido"),
        ({"dest": "DFW"}, None, "distintos"),
        ({"crs_dep_time": "25:00"}, "crs_dep_time", "hora inválida"),
        ({"crs_dep_time": "9:30"}, "crs_dep_time", "hora inválida"),
        ({"flight_date": "2020-01-01"}, "flight_date", "pasado"),
        ({"flight_date": (date.today() + timedelta(days=400)).isoformat()}, "flight_date", "365"),
        ({"flight_date": "mañana"}, "flight_date", "date"),
        ({"airline": "A$"}, "airline", "código"),
    ],
)
def test_validation_errors(client, override, field, fragment) -> None:
    r = client.post("/predict", json={**OK, **override})
    assert r.status_code == 422
    body = r.json()
    assert set(body) == {"error", "detail", "field"} and body["error"] == "validation_error"
    assert body["field"] == field and fragment in body["detail"]


def test_missing_field(client) -> None:
    r = client.post("/predict", json={k: v for k, v in OK.items() if k != "dest"})
    assert r.status_code == 422 and r.json()["field"] == "dest"


def test_batch_limits_and_error_location(client) -> None:
    r = client.post("/predict/batch", json={"flights": [OK] * 101})
    assert r.status_code == 422 and r.json()["field"] == "flights"
    r = client.post("/predict/batch", json={"flights": [OK, {**OK, "origin": "ZZZ"}]})
    assert r.status_code == 422 and r.json()["field"] == "flights[1].origin"
    r = client.post("/predict/batch", json={"flights": [OK, {**OK, "crs_dep_time": "99:99"}]})
    assert r.json()["field"] == "flights[1].crs_dep_time"


def test_not_found_uses_uniform_format(client) -> None:
    r = client.get("/nope")
    assert r.status_code == 404 and r.json()["error"] == "not_found"


def test_model_unavailable_returns_503(monkeypatch) -> None:
    monkeypatch.setenv("MODEL_DIR", "/no/existe")
    model_store.reset()
    c = TestClient(app)
    assert c.get("/health").json()["status"] == "degraded"
    r = c.post("/predict", json=OK)
    assert r.status_code == 503 and r.json()["error"] == "model_unavailable"
    model_store.reset()


# ------------------------------------------------------------------ features = oro
def test_features_use_shared_formulas() -> None:
    req = FlightRequest(**OK)
    feats, warnings = build_features(req, LOOKUPS)
    assert warnings == []
    assert (feats["dep_hour_sin"], feats["dep_hour_cos"]) == fx.hour_cyclic(18, 30)
    assert feats["days_to_holiday"] == fx.days_to_holiday(req.flight_date)
    assert feats["day_of_week"] == req.flight_date.isoweekday()
    assert feats["route_delay_rate_30d"] == 0.31 and feats["distance"] == 1235.0
    assert set(feats) == set(MODEL_FEATURES)


def test_route_rate_missing_uses_origin_then_global() -> None:
    req = FlightRequest(**{**OK, "origin": "LAX", "dest": "DFW"})
    feats, warnings = build_features(req, LOOKUPS)
    assert feats["route_delay_rate_30d"] == 0.22 and "30 días" in warnings[0]
    req = FlightRequest(**{**OK, "origin": "SEA", "dest": "LAX"})
    feats, _ = build_features(req, LOOKUPS)
    assert feats["route_delay_rate_30d"] == 0.27  # sin ruta ni tasa del origen -> global
    assert feats["distance"] == 1000.0  # distancia típica del origen
