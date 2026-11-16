"""Construcción de la matriz de features: la MISMA función en entrenamiento y en la API.

Las categóricas se codifican como enteros con niveles fijos guardados en metadata.json:
  - airline: índice en la lista ordenada de aerolíneas vistas al entrenar; una aerolínea
    nueva queda NaN (LightGBM la trata como "faltante" en vez de confundirla con otra).
  - day_of_week: 1..7 tal cual (convención BTS).
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd

from src.gold.feature_config import CATEGORICAL_FEATURES, MODEL_FEATURES, assert_no_leakage


def airline_levels(airlines: pd.Series) -> list[str]:
    return sorted(str(a) for a in airlines.dropna().unique())


def to_matrix(df: pd.DataFrame, airlines: Sequence[str]) -> np.ndarray:
    """DataFrame con columnas MODEL_FEATURES -> matriz float32 en el orden de MODEL_FEATURES."""
    assert_no_leakage(MODEL_FEATURES)
    missing = [c for c in MODEL_FEATURES if c not in df.columns]
    if missing:
        raise KeyError(f"Faltan features: {missing}")
    index = {a: i for i, a in enumerate(airlines)}
    cols = []
    for c in MODEL_FEATURES:
        if c == "airline":
            cols.append(df[c].map(index).astype("float32").to_numpy())
        else:
            cols.append(pd.to_numeric(df[c], errors="coerce").astype("float32").to_numpy())
    return np.column_stack(cols).astype(np.float32, copy=False)


def categorical_indices() -> list[int]:
    return [MODEL_FEATURES.index(c) for c in CATEGORICAL_FEATURES]
