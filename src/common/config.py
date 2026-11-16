"""Configuración compartida leída de variables de entorno (nunca hardcodeada)."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_BTS_URL_TEMPLATE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)

# Prefijos del bucket; cada capa vive en su propio prefijo.
LAYERS = ("landing", "bronze", "silver", "gold", "models", "reports")


@dataclass(frozen=True)
class Settings:
    bucket: str
    region: str = "us-east-1"
    bts_url_template: str = DEFAULT_BTS_URL_TEMPLATE

    def s3_uri(self, layer: str, *parts: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer!r}. Opciones: {LAYERS}")
        return "/".join([f"s3://{self.bucket}", layer, *parts])

    @staticmethod
    def bts_url(year: int, month: int, template: str = DEFAULT_BTS_URL_TEMPLATE) -> str:
        """El mes va sin cero a la izquierda (formato de BTS)."""
        if not 1 <= month <= 12:
            raise ValueError(f"Mes inválido: {month}")
        return template.format(year=year, month=month)

    @classmethod
    def from_env(cls) -> Settings:
        bucket = os.environ.get("FD_BUCKET")
        if not bucket:
            raise RuntimeError("Falta la variable de entorno FD_BUCKET")
        return cls(
            bucket=bucket,
            region=os.environ.get("AWS_REGION", "us-east-1"),
            bts_url_template=os.environ.get("FD_BTS_URL_TEMPLATE", DEFAULT_BTS_URL_TEMPLATE),
        )
