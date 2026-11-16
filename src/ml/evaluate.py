"""Métricas de clasificación binaria. Métrica principal: F1 de la clase positiva (retraso).

Por qué F1 y no accuracy: con ~20–27 % de positivos, el modelo "siempre a tiempo" logra
73–80 % de accuracy sin detectar un solo retraso (F1 = 0). F1 obliga a equilibrar precisión
(no alarmar de más) y recall (no dejar pasar retrasos).
"""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)


def best_f1_threshold(y_true: np.ndarray, proba: np.ndarray) -> tuple[float, float]:
    """Umbral que maximiza F1 (se elige en VALIDACIÓN y se congela para prueba)."""
    precision, recall, thresholds = precision_recall_curve(y_true, proba)
    p, r = precision[:-1], recall[:-1]  # el último punto no tiene umbral
    f1 = np.divide(2 * p * r, p + r, out=np.zeros_like(p), where=(p + r) > 0)
    i = int(np.argmax(f1))
    return float(thresholds[i]), float(f1[i])


def classification_metrics(
    y_true: np.ndarray, proba: np.ndarray | None, threshold: float
) -> dict[str, Any]:
    """Si `proba` es None se evalúa el baseline "siempre a tiempo" (todo 0)."""
    y_true = np.asarray(y_true).astype(int)
    y_pred = np.zeros_like(y_true) if proba is None else (proba >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    out: dict[str, Any] = {
        "n": int(len(y_true)),
        "positive_rate": round(float(y_true.mean()), 4),
        "threshold": None if proba is None else round(float(threshold), 4),
        "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "accuracy": round(float((y_pred == y_true).mean()), 4),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }
    if proba is not None:
        out["pr_auc"] = round(float(average_precision_score(y_true, proba)), 4)
        out["roc_auc"] = round(float(roc_auc_score(y_true, proba)), 4)
    return out
