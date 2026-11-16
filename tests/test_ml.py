"""Tests de la matriz de features, métricas, split temporal y empaquetado del modelo."""

from __future__ import annotations

import json
from datetime import date

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from src.gold.feature_config import MODEL_FEATURES, TARGET
from src.ml import export
from src.ml.evaluate import best_f1_threshold, classification_metrics
from src.ml.matrix import airline_levels, categorical_indices, to_matrix
from src.ml.train import split


def feature_frame(n: int = 4, airlines=("AA", "DL", "AA", "ZZ")) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    df = pd.DataFrame({c: rng.random(n) for c in MODEL_FEATURES})
    df["airline"] = list(airlines)[:n]
    df["day_of_week"] = [1, 2, 7, 3][:n]
    return df


def test_matrix_order_and_encoding() -> None:
    df = feature_frame()
    m = to_matrix(df, ["AA", "DL"])
    assert m.shape == (4, len(MODEL_FEATURES)) and m.dtype == np.float32
    a = MODEL_FEATURES.index("airline")
    assert m[0, a] == 0 and m[1, a] == 1 and np.isnan(m[3, a])  # aerolínea nueva -> NaN
    assert m[2, MODEL_FEATURES.index("day_of_week")] == 7
    i = MODEL_FEATURES.index("distance")
    assert m[0, i] == pytest.approx(df["distance"].iloc[0])


def test_matrix_rejects_missing_feature() -> None:
    with pytest.raises(KeyError, match="distance"):
        to_matrix(feature_frame().drop(columns=["distance"]), ["AA"])


def test_airline_levels_sorted_unique() -> None:
    assert airline_levels(pd.Series(["DL", "AA", "DL", None])) == ["AA", "DL"]
    assert categorical_indices() == [MODEL_FEATURES.index("airline"),
                                     MODEL_FEATURES.index("day_of_week")]  # fmt: skip


def test_best_threshold_and_metrics() -> None:
    y = np.array([0, 0, 0, 1, 1, 1])
    p = np.array([0.1, 0.2, 0.6, 0.55, 0.8, 0.9])
    thr, f1 = best_f1_threshold(y, p)
    assert thr == pytest.approx(0.55) and f1 == pytest.approx(6 / 7)
    m = classification_metrics(y, p, thr)
    assert m["confusion_matrix"] == {"tn": 2, "fp": 1, "fn": 0, "tp": 3}
    assert m["recall"] == 1.0 and m["pr_auc"] > 0.8


def test_baseline_always_on_time_has_zero_f1() -> None:
    y = np.array([0, 0, 0, 1])
    m = classification_metrics(y, None, 0.5)
    assert m["f1"] == 0.0 and m["accuracy"] == 0.75  # accuracy engaña con desbalance


def test_temporal_split_has_no_overlap() -> None:
    dates = pd.to_datetime(["2023-01-15", "2023-02-01", "2025-06-30", "2025-07-01",
                            "2025-09-30", "2025-10-01", "2026-08-31"])  # fmt: skip
    parts = split(pd.DataFrame({"flight_date": dates}))
    assert parts["train"]["flight_date"].dt.date.tolist() == [date(2023, 2, 1), date(2025, 6, 30)]
    assert parts["valid"]["flight_date"].dt.date.tolist() == [date(2025, 7, 1), date(2025, 9, 30)]
    assert len(parts["test"]) == 2
    assert parts["train"]["flight_date"].max() < parts["valid"]["flight_date"].min()
    assert parts["valid"]["flight_date"].max() < parts["test"]["flight_date"].min()


def _lookup_tables() -> dict[str, pd.DataFrame]:
    return {
        "lookup_global": pd.DataFrame(
            [
                {
                    "max_flight_date": date(2026, 8, 31),
                    "prior_30d": 0.27,
                    "prior_te": 0.21,
                    "flights_total": 100,
                }
            ]
        ),  # fmt: skip
        "lookup_route": pd.DataFrame(
            [
                {
                    "origin": "LAX",
                    "dest": "JFK",
                    "distance": 2475.0,
                    "route_delay_rate_30d": float("nan"),
                }
            ]
        ),  # fmt: skip
        "lookup_origin": pd.DataFrame(
            [{"origin": "LAX", "airport_name": "LAX Intl", "origin_te": np.float64(0.2)}]
        ),  # fmt: skip
        "lookup_dest": pd.DataFrame([{"dest": "JFK", "airport_name": "JFK Intl", "dest_te": 0.3}]),
        "lookup_airline": pd.DataFrame([{"airline": "AA", "airline_name": "American"}]),
    }


def test_lookups_payload_is_json_and_keyed() -> None:
    payload = export.build_lookups_payload(_lookup_tables())
    json.dumps(payload)  # serializable (sin NaN, fechas ni numpy)
    assert payload["routes"]["LAX-JFK"]["route_delay_rate_30d"] is None  # NaN -> null
    assert payload["origins"]["LAX"]["origin_te"] == 0.2
    assert payload["global"]["max_flight_date"] == "2026-08-31"


def test_write_and_upload_artifacts(tmp_path) -> None:
    X = np.random.default_rng(0).random((50, 2))
    booster = lgb.train({"objective": "binary", "verbose": -1}, lgb.Dataset(X, (X[:, 0] > 0.5)),
                        num_boost_round=2)  # fmt: skip
    out = export.write_artifacts(tmp_path / "v1", booster, {"threshold": 0.4}, {"routes": {}})
    assert sorted(p.name for p in out.iterdir()) == sorted(export.ARTIFACTS)

    class FakeS3:
        def __init__(self) -> None:
            self.keys: list[str] = []

        def upload_file(self, path: str, bucket: str, key: str) -> None:
            self.keys.append(key)

        def put_object(self, **kw) -> None:
            self.keys.append(kw["Key"])
            self.latest = json.loads(kw["Body"])

    s3 = FakeS3()
    assert export.upload_artifacts(s3, "bkt", out, "v1") == "s3://bkt/models/v1/"
    assert "models/latest.json" in s3.keys and s3.latest["version"] == "v1"
    assert TARGET not in MODEL_FEATURES
