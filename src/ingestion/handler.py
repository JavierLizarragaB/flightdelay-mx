"""Lambda de ingesta: descarga un mes de BTS On-Time Performance y lo deja en landing/.

Flujo:
  1. Decide el mes: explícito en el evento ({"year": 2026, "month": 8}) o, si no viene,
     el mes publicado más reciente (prueba hacia atrás con HEAD hasta `lookback` meses).
  2. Idempotencia: si ya existe `_manifest.json` para ese mes, no hace nada (salvo force).
  3. Descarga el ZIP (~32 MB) a /tmp y sube el CSV *en streaming* a S3 (multipart), sin
     descomprimirlo a disco (el CSV pesa ~270 MB). De paso cuenta filas y bytes.
  4. Escribe `_manifest.json` con la URL de origen, conteos y hora de ingesta. Bronce
     usa ese manifiesto para validar que procesó el archivo completo.

Destino: s3://<bucket>/landing/year=YYYY/month=MM/bts_YYYY_MM.csv

El archivo es autocontenido (solo stdlib + boto3, que ya trae el runtime de Lambda) para
empaquetarlo como un ZIP sin dependencias.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, BinaryIO, Protocol

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_BTS_URL_TEMPLATE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
USER_AGENT = "flightdelay-mx-ingest/1.0"
HTTP_TIMEOUT_S = 60
DEFAULT_LOOKBACK_MONTHS = 6
MANIFEST_NAME = "_manifest.json"

# Catálogos de BTS (lookup tables). L_AIRPORT_ID se usa en vez de L_AIRPORT porque el ID
# numérico es estable; los códigos IATA se han reasignado a lo largo de los años.
LOOKUP_URL_TEMPLATE = "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72={code}"
LOOKUPS = {
    "L_UNIQUE_CARRIERS": "Y_haVdhR_PNeeVRef",
    "L_AIRPORT_ID": "Y_NVecbeg_VQ",
}


def lookup_key(name: str) -> str:
    return f"landing/lookups/{name}.csv"


class NotPublishedError(Exception):
    """BTS devolvió 404: el mes todavía no está publicado."""


@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"Mes inválido: {self.month}")
        if not 1987 <= self.year <= 2100:
            raise ValueError(f"Año inválido: {self.year}")

    def previous(self) -> YearMonth:
        if self.month == 1:
            return YearMonth(self.year - 1, 12)
        return YearMonth(self.year, self.month - 1)

    @property
    def tag(self) -> str:
        return f"{self.year}_{self.month:02d}"


def landing_prefix(ym: YearMonth) -> str:
    return f"landing/year={ym.year}/month={ym.month:02d}/"


def csv_key(ym: YearMonth) -> str:
    return f"{landing_prefix(ym)}bts_{ym.tag}.csv"


def manifest_key(ym: YearMonth) -> str:
    return f"{landing_prefix(ym)}{MANIFEST_NAME}"


def bts_url(ym: YearMonth, template: str = DEFAULT_BTS_URL_TEMPLATE) -> str:
    """BTS usa el mes sin cero a la izquierda."""
    return template.format(year=ym.year, month=ym.month)


def candidate_months(today: date, lookback: int) -> list[YearMonth]:
    """Meses a probar, del más reciente al más antiguo, empezando por el mes anterior."""
    ym = YearMonth(today.year, today.month).previous()
    out = []
    for _ in range(lookback):
        out.append(ym)
        ym = ym.previous()
    return out


# ---------------------------------------------------------------------------------------
# HTTP (inyectable para tests)
# ---------------------------------------------------------------------------------------
class Http(Protocol):
    def exists(self, url: str) -> bool: ...
    def download(self, url: str, dest: BinaryIO) -> int: ...


class UrllibHttp:
    def _request(self, url: str, method: str) -> urllib.request.Request:
        if not url.startswith("https://"):
            raise ValueError(f"Solo se permite HTTPS: {url}")
        return urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})

    def exists(self, url: str) -> bool:
        try:
            with urllib.request.urlopen(self._request(url, "HEAD"), timeout=HTTP_TIMEOUT_S):
                return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise

    def download(self, url: str, dest: BinaryIO) -> int:
        try:
            resp = urllib.request.urlopen(self._request(url, "GET"), timeout=HTTP_TIMEOUT_S)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NotPublishedError(url) from e
            raise
        total = 0
        with resp:
            while chunk := resp.read(1 << 20):
                dest.write(chunk)
                total += len(chunk)
        return total


# ---------------------------------------------------------------------------------------
# Lógica principal
# ---------------------------------------------------------------------------------------
class CountingReader:
    """Envuelve un stream binario y cuenta bytes y saltos de línea al leer."""

    def __init__(self, raw: BinaryIO) -> None:
        self._raw = raw
        self.bytes = 0
        self.newlines = 0
        self._last = b""

    def read(self, size: int = -1) -> bytes:
        data = self._raw.read(size)
        if data:
            self.bytes += len(data)
            self.newlines += data.count(b"\n")
            self._last = data[-1:]
        return data

    @property
    def lines(self) -> int:
        """Líneas totales (incluye encabezado), contando una última línea sin \\n."""
        return self.newlines + (1 if self._last and self._last != b"\n" else 0)


def already_ingested(s3: Any, bucket: str, ym: YearMonth) -> bool:
    # ListObjectsV2 en vez de HeadObject: el rol solo tiene ListBucket sobre landing/*.
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=manifest_key(ym), MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def pick_csv_member(zf: zipfile.ZipFile) -> zipfile.ZipInfo:
    csvs = [i for i in zf.infolist() if i.filename.lower().endswith(".csv")]
    if len(csvs) != 1:
        names = [i.filename for i in zf.infolist()]
        raise ValueError(f"Se esperaba exactamente un CSV en el ZIP, se encontraron: {names}")
    return csvs[0]


def ingest_month(
    s3: Any,
    http: Http,
    bucket: str,
    ym: YearMonth,
    url_template: str = DEFAULT_BTS_URL_TEMPLATE,
    tmp_dir: str | None = None,
) -> dict[str, Any]:
    url = bts_url(ym, url_template)
    logger.info("Descargando %s", url)

    with tempfile.TemporaryFile(dir=tmp_dir) as tmp:
        zip_bytes = http.download(url, tmp)
        tmp.seek(0)
        with zipfile.ZipFile(tmp) as zf:
            member = pick_csv_member(zf)
            with zf.open(member) as raw:
                reader = CountingReader(raw)
                s3.upload_fileobj(
                    reader,
                    bucket,
                    csv_key(ym),
                    ExtraArgs={"ContentType": "text/csv"},
                )

    if reader.bytes != member.file_size:
        raise RuntimeError(f"CSV truncado: se subieron {reader.bytes} bytes de {member.file_size}")

    manifest = {
        "year": ym.year,
        "month": ym.month,
        "source_url": url,
        "source_member": member.filename,
        "zip_bytes": zip_bytes,
        "csv_bytes": reader.bytes,
        "csv_rows": max(reader.lines - 1, 0),  # sin encabezado
        "s3_uri": f"s3://{bucket}/{csv_key(ym)}",
        "ingested_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    # El manifiesto se escribe al final: su existencia indica que el CSV está completo.
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key(ym),
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    logger.info("Ingesta completa: %s", json.dumps(manifest))
    return manifest


def resolve_target(
    event: dict[str, Any], http: Http, url_template: str, today: date
) -> YearMonth | None:
    if "year" in event or "month" in event:
        return YearMonth(int(event["year"]), int(event["month"]))
    lookback = int(event.get("lookback_months", DEFAULT_LOOKBACK_MONTHS))
    for ym in candidate_months(today, lookback):
        if http.exists(bts_url(ym, url_template)):
            return ym
    return None


def ingest_lookups(s3: Any, http: Http, bucket: str) -> dict[str, Any]:
    """Descarga los catálogos de BTS (pocos cientos de KB) y los sobrescribe siempre: BTS
    agrega aerolíneas/aeropuertos nuevos y queremos la versión vigente."""
    out = {}
    for name, code in LOOKUPS.items():
        url = LOOKUP_URL_TEMPLATE.format(code=code)
        with tempfile.TemporaryFile() as tmp:
            size = http.download(url, tmp)
            tmp.seek(0)
            head = tmp.read(64)
            if not head.startswith(b"Code,Description"):
                raise ValueError(f"{name}: respuesta inesperada de BTS ({head[:40]!r})")
            tmp.seek(0)
            s3.upload_fileobj(tmp, bucket, lookup_key(name), ExtraArgs={"ContentType": "text/csv"})
        out[name] = {"bytes": size, "s3_uri": f"s3://{bucket}/{lookup_key(name)}"}
    logger.info("Catálogos actualizados: %s", json.dumps(out))
    return {"status": "lookups_ingested", "lookups": out}


MAX_BACKFILL_MONTHS = 120


def _parse_ym(value: str) -> YearMonth:
    try:
        y, m = str(value).strip().split("-")
        return YearMonth(int(y), int(m))
    except ValueError as e:
        raise ValueError(f"Mes inválido {value!r}; se espera YYYY-MM") from e


def plan_backfill(plan: dict[str, Any]) -> dict[str, Any]:
    """Expande un rango inclusivo de meses. Step Functions no puede generar rangos de
    fechas por sí mismo; este modo le da la lista para su estado Map."""
    start, end = _parse_ym(plan["start"]), _parse_ym(plan["end"])
    if (start.year, start.month) > (end.year, end.month):
        raise ValueError(f"Rango invertido: {plan['start']} > {plan['end']}")
    months: list[YearMonth] = []
    ym = end
    while (ym.year, ym.month) >= (start.year, start.month):
        months.append(ym)
        ym = ym.previous()
    months.reverse()
    if len(months) > MAX_BACKFILL_MONTHS:
        raise ValueError(f"Rango de {len(months)} meses; máximo {MAX_BACKFILL_MONTHS}")
    return {
        "status": "planned",
        "months": [{"year": m.year, "month": m.month} for m in months],
        "count": len(months),
        "year_months": f"{start.year}-{start.month:02d}:{end.year}-{end.month:02d}",
    }


def run(
    event: dict[str, Any],
    s3: Any,
    http: Http,
    bucket: str,
    url_template: str = DEFAULT_BTS_URL_TEMPLATE,
    today: date | None = None,
) -> dict[str, Any]:
    """Devuelve siempre un dict con `status` para que Step Functions decida con un Choice:
    `ingested`, `already_ingested`, `not_published`, `lookups_ingested` o `planned`.

    Modos especiales:
      {"lookups": true}                              descarga los catálogos
      {"plan": {"start": "2023-01", "end": "2026-08"}}  lista de meses para el backfill
    """
    if event.get("lookups"):
        return ingest_lookups(s3, http, bucket)
    if "plan" in event:
        return plan_backfill(event["plan"])
    today = today or datetime.now(UTC).date()
    ym = resolve_target(event, http, url_template, today)
    if ym is None:
        return {"status": "not_published", "detail": "Ningún mes reciente publicado"}

    base = {
        "year": ym.year,
        "month": ym.month,
        "year_month": f"{ym.year}-{ym.month:02d}",  # formato de --year_months de los Glue jobs
        "s3_prefix": f"s3://{bucket}/{landing_prefix(ym)}",
    }
    if not event.get("force", False) and already_ingested(s3, bucket, ym):
        return {"status": "already_ingested", **base}

    try:
        manifest = ingest_month(s3, http, bucket, ym, url_template)
    except NotPublishedError:
        return {"status": "not_published", **base}
    return {"status": "ingested", **base, "csv_rows": manifest["csv_rows"]}


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    import boto3  # disponible en el runtime de Lambda

    bucket = os.environ["FD_BUCKET"]
    template = os.environ.get("FD_BTS_URL_TEMPLATE", DEFAULT_BTS_URL_TEMPLATE)
    return run(event or {}, boto3.client("s3"), UrllibHttp(), bucket, template)
