"""Carga del modelo publicado: s3://<bucket>/models/latest.json -> artefactos de la versión.

En Lambda se carga una vez por contenedor (en el primer request) y se reutiliza. Para
pruebas locales, MODEL_DIR apunta a un directorio con model.txt/metadata.json/lookups.json.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb

log = logging.getLogger(__name__)


@dataclass
class ModelBundle:
    booster: lgb.Booster
    metadata: dict[str, Any]
    lookups: dict[str, Any]

    @property
    def version(self) -> str:
        return self.metadata["version"]

    @property
    def threshold(self) -> float:
        return float(self.metadata["threshold"])


def load_from_dir(path: Path) -> ModelBundle:
    return ModelBundle(
        booster=lgb.Booster(model_file=str(path / "model.txt")),
        metadata=json.loads((path / "metadata.json").read_text()),
        lookups=json.loads((path / "lookups.json").read_text()),
    )


def load_from_s3(bucket: str) -> ModelBundle:
    import boto3

    s3 = boto3.client("s3")
    latest = json.loads(s3.get_object(Bucket=bucket, Key="models/latest.json")["Body"].read())
    tmp = Path(tempfile.mkdtemp(prefix="model-"))
    for name in ("model.txt", "metadata.json", "lookups.json"):
        s3.download_file(bucket, f"{latest['prefix']}{name}", str(tmp / name))
    log.info("Modelo %s cargado desde s3://%s/%s", latest["version"], bucket, latest["prefix"])
    return load_from_dir(tmp)


def _public_message(e: Exception) -> str:
    """Mensaje para el cliente sin ARNs ni detalles internos (el detalle va a los logs)."""
    code = (
        getattr(e, "response", {}).get("Error", {}).get("Code") if hasattr(e, "response") else None
    )
    if code in ("NoSuchKey", "404"):
        return "No hay un modelo publicado todavía (models/latest.json no existe)."
    if code:
        return f"No se pudo leer el modelo desde S3 ({code})."
    if isinstance(e, FileNotFoundError | OSError):
        return "No se encontraron los artefactos del modelo."
    return f"Error al cargar el modelo ({type(e).__name__})."


_lock = threading.Lock()
_bundle: ModelBundle | None = None
_error: str | None = None


def get_bundle() -> ModelBundle | None:
    """Devuelve el modelo (cargándolo la primera vez) o None si no se pudo cargar."""
    global _bundle, _error
    if _bundle is not None:
        return _bundle
    with _lock:
        if _bundle is None:
            try:
                if os.environ.get("MODEL_DIR"):
                    _bundle = load_from_dir(Path(os.environ["MODEL_DIR"]))
                else:
                    _bundle = load_from_s3(os.environ["MODEL_BUCKET"])
                _error = None
            except Exception as e:  # se reporta en /health en vez de tumbar el proceso
                log.exception("No se pudo cargar el modelo")
                _error = _public_message(e)
    return _bundle


def last_error() -> str | None:
    return _error


def reset() -> None:
    """Solo para tests."""
    global _bundle, _error
    _bundle, _error = None, None
