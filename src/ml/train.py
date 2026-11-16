"""Entrenamiento: baseline -> regresión logística -> LightGBM, con split temporal.

Uso:
    python -m src.ml.train --bucket <bucket> [--upload] [--max-train-rows 8000000]
    python -m src.ml.train --bucket <bucket> --compare   # + XGBoost y CatBoost

Con --compare se entrenan además XGBoost y CatBoost con los mismos datos, el mismo split,
early stopping en validación y umbral F1-óptimo de validación. Son solo comparación: el
modelo exportado y servido por la API sigue siendo LightGBM (ver README, sección 10).

Split temporal (nunca aleatorio: el modelo se usa para predecir el futuro):
    calentamiento  ene 2023            (sin historial para TE/ventanas; no se usa)
    entrenamiento  feb 2023 – jun 2025
    validación     jul – sep 2025      (early stopping y elección del umbral)
    prueba         oct 2025 – último   (se toca una sola vez, al final)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.gold.feature_config import CATEGORICAL_FEATURES, MODEL_FEATURES, TARGET
from src.ml import data, export
from src.ml.evaluate import best_f1_threshold, classification_metrics
from src.ml.matrix import airline_levels, categorical_indices, to_matrix

SPLITS = {
    "train": ("2023-02-01", "2025-06-30"),
    "valid": ("2025-07-01", "2025-09-30"),
    "test": ("2025-10-01", "2100-01-01"),
}
# Selección SOLO en validación (jul–sep 2025) entre 3 configuraciones:
#   A num_leaves=127, min_data_in_leaf=500        -> iter 37,  PR-AUC 0.4078, F1 0.4621
#   B (elegida) regularizada, ver abajo           -> iter 146, PR-AUC 0.4092, F1 0.4631
#   C num_leaves=15, min_data_in_leaf=20000       -> iter 125, PR-AUC 0.4102, F1 0.4627
# Las diferencias son mínimas: el techo lo pone la información disponible al comprar el
# boleto, no los hiperparámetros. Se elige B por F1 (métrica principal) y porque A deja de
# mejorar en la iteración 37 (sobreajuste a los años de entrenamiento).
LGB_PARAMS = {
    "objective": "binary",
    "metric": ["average_precision", "binary_logloss"],
    "learning_rate": 0.03,
    "num_leaves": 31,
    "min_data_in_leaf": 5000,
    "feature_fraction": 0.7,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 10.0,
    "cat_smooth": 50,
    "min_data_per_group": 1000,
    "verbose": -1,
    "seed": 42,
}
# XGBoost y CatBoost: configuraciones equivalentes a LGB_PARAMS (mismo learning rate,
# ~31 hojas, regularización L2 = 10, submuestreo de filas y columnas). No se afinaron más
# para que la comparación mida el algoritmo, no horas de búsqueda de hiperparámetros.
XGB_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": ["aucpr", "logloss"],
    "tree_method": "hist",
    "grow_policy": "lossguide",
    "max_leaves": 31,
    "max_depth": 0,
    "learning_rate": 0.03,
    # min_child_weight es suma de hessianos p(1-p) ≈ 0.17 por fila: 850 ≈ 5000 filas.
    "min_child_weight": 850,
    "subsample": 0.8,
    "colsample_bytree": 0.7,
    "lambda": 10.0,
    "max_cat_to_onehot": 1,  # partición categórica óptima, como LightGBM
    "seed": 42,
}
CATBOOST_PARAMS = {
    "loss_function": "Logloss",
    # Early stopping por Logloss: el PRAUC interno de CatBoost no coincide con el de
    # scikit-learn (0.459 vs 0.358 en validación) y detenía el entrenamiento en la iteración 0.
    "eval_metric": "Logloss",
    "iterations": 3000,
    "learning_rate": 0.08,  # CatBoost (árboles simétricos) converge más lento; 0.03 tardaría horas
    "depth": 6,  # árbol simétrico de 64 hojas
    "l2_leaf_reg": 10.0,
    "rsm": 0.7,
    "bootstrap_type": "Bernoulli",
    "subsample": 0.8,
    "random_seed": 42,
    "od_type": "Iter",
    "od_wait": 150,
    "use_best_model": True,
    "verbose": 100,
    "allow_writing_files": False,  # sin carpeta catboost_info/ en el repo
}
LOGREG_SAMPLE = 1_500_000
# CatBoost con 16.5 M de filas tardaba ~11 s por iteración en una laptop de 8 GB (swap);
# con 3 M de filas el costo es manejable y la comparación sigue siendo representativa.
CATBOOST_SAMPLE = 3_000_000  # la logística es un punto de comparación; 1.5 M filas bastan


def split(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    out = {}
    for name, (start, end) in SPLITS.items():
        mask = (df["flight_date"] >= start) & (df["flight_date"] <= end)
        out[name] = df.loc[mask]
    return out


def logistic_pipeline() -> Pipeline:
    cat = categorical_indices()
    num = [i for i in range(len(MODEL_FEATURES)) if i not in cat]
    pre = ColumnTransformer(
        [
            ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                              ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat),
            ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                              ("sc", StandardScaler())]), num),
        ]
    )  # fmt: skip
    return Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=500, C=1.0))])


def train_xgboost(X: dict[str, np.ndarray], y: dict[str, np.ndarray]) -> dict[str, Any]:
    import xgboost as xgb

    cat = set(categorical_indices())
    types = ["c" if i in cat else "q" for i in range(len(MODEL_FEATURES))]
    kw = {"feature_names": list(MODEL_FEATURES), "feature_types": types, "enable_categorical": True}
    # QuantileDMatrix cuantiza al construir: ~4x menos memoria que DMatrix con 16 M de filas.
    dtrain = xgb.QuantileDMatrix(X["train"], y["train"], **kw)
    dvalid = xgb.QuantileDMatrix(X["valid"], y["valid"], ref=dtrain, **kw)
    booster = xgb.train(XGB_PARAMS, dtrain, num_boost_round=3000, evals=[(dvalid, "valid")],
                        early_stopping_rounds=150, verbose_eval=100)  # fmt: skip
    rng = (0, booster.best_iteration + 1)
    proba = {
        s: booster.predict(xgb.DMatrix(X[s], **kw), iteration_range=rng) for s in ("valid", "test")
    }
    return {"proba": proba, "best_iteration": booster.best_iteration}


def _catboost_frame(X: np.ndarray) -> pd.DataFrame:
    """CatBoost exige categóricas enteras o texto: NaN (aerolínea desconocida) -> -1."""
    df = pd.DataFrame(X, columns=list(MODEL_FEATURES))
    for i in categorical_indices():
        c = MODEL_FEATURES[i]
        df[c] = df[c].fillna(-1).astype("int32")
    return df


def train_catboost(X: dict[str, np.ndarray], y: dict[str, np.ndarray]) -> dict[str, Any]:
    from catboost import CatBoostClassifier, Pool

    cat = categorical_indices()
    rng = np.random.default_rng(42)
    n = len(y["train"])
    idx = np.sort(rng.choice(n, size=min(CATBOOST_SAMPLE, n), replace=False))
    data_ = {**X, "train": X["train"][idx]}
    labels = {**y, "train": y["train"][idx]}
    pool = {s: Pool(_catboost_frame(data_[s]), labels[s], cat_features=cat) for s in data_}
    model = CatBoostClassifier(**CATBOOST_PARAMS, thread_count=os.cpu_count())
    model.fit(pool["train"], eval_set=pool["valid"])
    proba = {s: model.predict_proba(pool[s])[:, 1] for s in ("valid", "test")}
    return {
        "proba": proba,
        "best_iteration": int(model.get_best_iteration()),
        "train_rows": len(idx),
    }


def evaluate_candidate(name: str, fit, X, y, train_rows: int) -> dict[str, Any]:
    """Entrena un modelo de comparación y lo evalúa con el mismo protocolo que LightGBM."""
    t = time.time()
    out = fit(X, y)
    p = out["proba"]
    thr, _ = best_f1_threshold(y["valid"], p["valid"])
    res = {
        "valid": classification_metrics(y["valid"], p["valid"], thr),
        "test": classification_metrics(y["test"], p["test"], thr),
        "train_rows": out.get("train_rows", train_rows),
        "best_iteration": out["best_iteration"],
        "seconds": round(time.time() - t, 1),
    }
    print(f"[train] {name} F1 valid {res['valid']['f1']} test {res['test']['f1']}")
    return res


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=os.environ.get("FD_BUCKET"))
    p.add_argument("--cache-dir", default="data/gold_export")
    p.add_argument("--export-tag", default=None, help="reutiliza un UNLOAD previo")
    p.add_argument("--max-train-rows", type=int, default=None)
    p.add_argument("--out-dir", default="models")
    p.add_argument("--upload", action="store_true")
    p.add_argument("--compare", action="store_true", help="entrena también XGBoost y CatBoost")
    p.add_argument("--comparison-out", default="reports/model_comparison.json")
    args = p.parse_args()
    if not args.bucket:
        raise SystemExit("Falta --bucket o FD_BUCKET")

    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    athena, s3 = session.client("athena"), session.client("s3")
    tag = args.export_tag or datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    version = datetime.now(UTC).strftime("v%Y%m%d-%H%M%S")
    t0 = time.time()

    print(f"[train] leyendo fd_gold.features_flights (export {tag})…")
    df = data.load_features(athena, s3, args.bucket, Path(args.cache_dir), tag)
    lookups = data.load_lookups(athena, s3, args.bucket, Path(args.cache_dir), tag)
    print(f"[train] {len(df):,} filas en {time.time() - t0:.0f}s")

    parts = split(df)
    train = parts["train"]
    if args.max_train_rows and len(train) > args.max_train_rows:
        train = train.sample(n=args.max_train_rows, random_state=42)
    levels = airline_levels(df["airline"])
    X = {k: to_matrix(v, levels) for k, v in {**parts, "train": train}.items()}
    y = {k: v[TARGET].to_numpy() for k, v in {**parts, "train": train}.items()}
    sizes = {k: len(v) for k, v in y.items()}
    print(f"[train] tamaños: {sizes}")

    results: dict[str, Any] = {}

    # 1) Baseline: "siempre a tiempo"
    results["baseline_always_on_time"] = {
        s: classification_metrics(y[s], None, 0.5) for s in ("valid", "test")
    }

    # 2) Regresión logística
    t = time.time()
    rng = np.random.default_rng(42)
    idx = rng.choice(len(y["train"]), size=min(LOGREG_SAMPLE, len(y["train"])), replace=False)
    logreg = logistic_pipeline().fit(X["train"][idx], y["train"][idx])
    p_valid = logreg.predict_proba(X["valid"])[:, 1]
    thr_lr, _ = best_f1_threshold(y["valid"], p_valid)
    results["logistic_regression"] = {
        "valid": classification_metrics(y["valid"], p_valid, thr_lr),
        "test": classification_metrics(y["test"], logreg.predict_proba(X["test"])[:, 1], thr_lr),
        "train_rows": len(idx),
        "seconds": round(time.time() - t, 1),
    }
    print(f"[train] logística F1 valid {results['logistic_regression']['valid']['f1']}")

    # 3) LightGBM
    t = time.time()
    cat = categorical_indices()
    dtrain = lgb.Dataset(X["train"], y["train"], feature_name=list(MODEL_FEATURES),
                         categorical_feature=cat, free_raw_data=True)  # fmt: skip
    dvalid = lgb.Dataset(X["valid"], y["valid"], reference=dtrain)
    booster = lgb.train(
        LGB_PARAMS,
        dtrain,
        num_boost_round=3000,
        valid_sets=[dvalid],
        valid_names=["valid"],
        callbacks=[lgb.early_stopping(150, first_metric_only=True), lgb.log_evaluation(100)],
    )
    p_valid = booster.predict(X["valid"], num_iteration=booster.best_iteration)
    threshold, _ = best_f1_threshold(y["valid"], p_valid)
    p_test = booster.predict(X["test"], num_iteration=booster.best_iteration)
    results["lightgbm"] = {
        "valid": classification_metrics(y["valid"], p_valid, threshold),
        "test": classification_metrics(y["test"], p_test, threshold),
        "train_rows": sizes["train"],
        "best_iteration": booster.best_iteration,
        "seconds": round(time.time() - t, 1),
    }
    if args.compare:
        for name, fit in (("xgboost", train_xgboost), ("catboost", train_catboost)):
            results[name] = evaluate_candidate(name, fit, X, y, sizes["train"])

    gain = booster.feature_importance(importance_type="gain")
    importance = {
        f: round(float(v / gain.sum()), 4)
        for f, v in sorted(zip(MODEL_FEATURES, gain, strict=True), key=lambda x: -x[1])
    }
    print(json.dumps({k: v.get("test", v) for k, v in results.items()}, indent=1))

    # Export
    test_months = parts["test"]["flight_date"]
    metadata = {
        "version": version,
        "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "model_type": "lightgbm",
        "target": TARGET,
        "task": "binary_classification",
        "primary_metric": "f1_positive_class",
        "features": list(MODEL_FEATURES),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "airline_levels": levels,
        "threshold": round(threshold, 4),
        "best_iteration": booster.best_iteration,
        "params": LGB_PARAMS,
        "splits": {
            **{k: {"start": s, "end": e, "rows": sizes[k]} for k, (s, e) in SPLITS.items()},
            "test_effective_end": str(test_months.max().date()) if len(test_months) else None,
        },
        "data": {"rows_total": len(df), "export_tag": tag},
        "metrics": results,
        "feature_importance_gain": importance,
        "lookups_as_of": str(lookups["lookup_global"]["max_flight_date"].iloc[0]),
    }
    out_dir = export.write_artifacts(
        Path(args.out_dir) / version, booster, metadata, export.build_lookups_payload(lookups)
    )
    print(f"[train] artefactos en {out_dir}")
    if args.compare:
        comparison = {
            "version": version,
            "protocol": "mismo split temporal y features; early stopping y umbral en validación",
            "splits": metadata["splits"],
            "params": {"lightgbm": LGB_PARAMS, "xgboost": XGB_PARAMS, "catboost": CATBOOST_PARAMS},
            "metrics": results,
        }
        Path(args.comparison_out).write_text(json.dumps(comparison, indent=2, default=str) + "\n")
        print(f"[train] comparación en {args.comparison_out}")
    if args.upload:
        uri = export.upload_artifacts(s3, args.bucket, out_dir, version)
        s3.put_object(
            Bucket=args.bucket,
            Key=f"reports/ml/metrics_{version}.json",
            Body=json.dumps(metadata, indent=2, default=str).encode(),
            ContentType="application/json",
        )
        print(f"[train] publicado en {uri} (models/latest.json -> {version})")
    print(f"[train] total {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
