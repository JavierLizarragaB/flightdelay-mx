"""Empaquetado del modelo: un directorio versionado con todo lo que la API necesita.

models/<version>/
  model.txt       booster de LightGBM (texto: portable y diffeable)
  metadata.json   features, niveles categóricos, umbral, métricas, periodos, priors
  lookups.json    estadísticas por ruta/origen/destino/aerolínea + catálogo con nombres

Se sube a s3://<bucket>/models/<version>/ y s3://<bucket>/models/latest.json apunta a la
versión vigente. La API lee latest.json al arrancar: publicar un modelo nuevo no
requiere reconstruir la imagen.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

ARTIFACTS = ("model.txt", "metadata.json", "lookups.json")


def _clean(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if hasattr(v, "item"):  # numpy escalares
        return _clean(v.item())
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _records(df: pd.DataFrame, key: list[str] | str) -> dict[str, dict[str, Any]]:
    keys = [key] if isinstance(key, str) else key
    out = {}
    for row in df.to_dict(orient="records"):
        k = "-".join(str(row[c]) for c in keys)
        out[k] = {c: _clean(v) for c, v in row.items() if c not in keys}
    return out


def build_lookups_payload(lookups: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Tablas de fd_gold.lookup_* -> dict JSON indexado por código (ruta = 'ORIG-DEST')."""
    glob = {c: _clean(v) for c, v in lookups["lookup_global"].iloc[0].to_dict().items()}
    return {
        "global": glob,
        "routes": _records(lookups["lookup_route"], ["origin", "dest"]),
        "origins": _records(lookups["lookup_origin"], "origin"),
        "dests": _records(lookups["lookup_dest"], "dest"),
        "airlines": _records(lookups["lookup_airline"], "airline"),
    }


def write_artifacts(
    out_dir: Path, booster: Any, metadata: dict[str, Any], lookups_payload: dict[str, Any]
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(out_dir / "model.txt"))
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str))
    (out_dir / "lookups.json").write_text(json.dumps(lookups_payload, default=str))
    return out_dir


def upload_artifacts(s3: Any, bucket: str, out_dir: Path, version: str) -> str:
    for name in ARTIFACTS:
        s3.upload_file(str(out_dir / name), bucket, f"models/{version}/{name}")
    s3.put_object(
        Bucket=bucket,
        Key="models/latest.json",
        Body=json.dumps({"version": version, "prefix": f"models/{version}/"}).encode(),
        ContentType="application/json",
    )
    return f"s3://{bucket}/models/{version}/"
