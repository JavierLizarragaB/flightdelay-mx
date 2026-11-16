"""Tests de la Lambda de ingesta con S3 y HTTP falsos (sin red ni AWS)."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from typing import Any, BinaryIO

import pytest

from src.common.config import DEFAULT_BTS_URL_TEMPLATE as CONFIG_TEMPLATE
from src.ingestion import handler as h

CSV = b'"Year","Month","FlightDate",\n"2026","8","2026-08-01",\n"2026","8","2026-08-02",\n'


def make_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


class FakeHttp:
    def __init__(self, published: dict[str, bytes]) -> None:
        self.published = published
        self.downloads: list[str] = []

    def exists(self, url: str) -> bool:
        return url in self.published

    def download(self, url: str, dest: BinaryIO) -> int:
        self.downloads.append(url)
        if url not in self.published:
            raise h.NotPublishedError(url)
        dest.write(self.published[url])
        return len(self.published[url])


class FakeS3:
    """Imita boto3; los argumentos en CamelCase son los de su API."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def upload_fileobj(self, fileobj: Any, bucket: str, key: str, ExtraArgs: Any = None) -> None:  # noqa: N803
        chunks = []
        while data := fileobj.read(7):  # lecturas pequeñas para ejercitar el contador
            chunks.append(data)
        self.objects[key] = b"".join(chunks)

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:  # noqa: N803
        self.objects[Key] = Body

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int) -> dict[str, int]:  # noqa: N803
        return {"KeyCount": sum(k.startswith(Prefix) for k in self.objects)}


AUG = h.YearMonth(2026, 8)
AUG_URL = h.bts_url(AUG)


def test_template_matches_shared_config() -> None:
    # La Lambda es autocontenida; este test evita que diverja de src/common/config.py.
    assert h.DEFAULT_BTS_URL_TEMPLATE == CONFIG_TEMPLATE


def test_keys_and_url() -> None:
    assert h.csv_key(AUG) == "landing/year=2026/month=08/bts_2026_08.csv"
    assert h.manifest_key(AUG) == "landing/year=2026/month=08/_manifest.json"
    assert AUG_URL.endswith("_2026_8.zip")  # sin cero a la izquierda


@pytest.mark.parametrize("month", [0, 13])
def test_invalid_month(month: int) -> None:
    with pytest.raises(ValueError):
        h.YearMonth(2026, month)


def test_candidate_months_cross_year() -> None:
    got = h.candidate_months(date(2026, 2, 10), 3)
    assert got == [h.YearMonth(2026, 1), h.YearMonth(2025, 12), h.YearMonth(2025, 11)]


def test_ingest_explicit_month_streams_csv_and_writes_manifest() -> None:
    s3, http = FakeS3(), FakeHttp({AUG_URL: make_zip({"data.csv": CSV, "readme.html": b"x"})})
    out = h.run({"year": 2026, "month": 8}, s3, http, "bkt")

    assert out["status"] == "ingested"
    assert out["csv_rows"] == 2
    assert s3.objects[h.csv_key(AUG)] == CSV
    manifest = json.loads(s3.objects[h.manifest_key(AUG)])
    assert manifest["csv_bytes"] == len(CSV)
    assert manifest["source_member"] == "data.csv"
    assert manifest["s3_uri"] == "s3://bkt/landing/year=2026/month=08/bts_2026_08.csv"


def test_row_count_without_trailing_newline() -> None:
    s3 = FakeS3()
    http = FakeHttp({AUG_URL: make_zip({"d.csv": CSV.rstrip(b"\n")})})
    assert h.run({"year": 2026, "month": 8}, s3, http, "bkt")["csv_rows"] == 2


def test_idempotent_skips_when_manifest_exists() -> None:
    s3, http = FakeS3(), FakeHttp({AUG_URL: make_zip({"d.csv": CSV})})
    h.run({"year": 2026, "month": 8}, s3, http, "bkt")
    out = h.run({"year": 2026, "month": 8}, s3, http, "bkt")
    assert out["status"] == "already_ingested"
    assert len(http.downloads) == 1


def test_force_reingests() -> None:
    s3, http = FakeS3(), FakeHttp({AUG_URL: make_zip({"d.csv": CSV})})
    h.run({"year": 2026, "month": 8}, s3, http, "bkt")
    out = h.run({"year": 2026, "month": 8, "force": True}, s3, http, "bkt")
    assert out["status"] == "ingested"
    assert len(http.downloads) == 2


def test_latest_picks_newest_published_month() -> None:
    jul_url = h.bts_url(h.YearMonth(2026, 7))
    http = FakeHttp({AUG_URL: make_zip({"d.csv": CSV}), jul_url: b""})
    out = h.run({}, FakeS3(), http, "bkt", today=date(2026, 11, 15))  # oct y sep: 404
    assert (out["status"], out["year"], out["month"]) == ("ingested", 2026, 8)


def test_latest_none_published() -> None:
    out = h.run({}, FakeS3(), FakeHttp({}), "bkt", today=date(2026, 11, 15))
    assert out["status"] == "not_published"


def test_explicit_month_not_published() -> None:
    out = h.run({"year": 2026, "month": 9}, FakeS3(), FakeHttp({}), "bkt")
    assert out["status"] == "not_published"


def test_zip_without_single_csv_fails() -> None:
    http = FakeHttp({AUG_URL: make_zip({"a.csv": CSV, "b.csv": CSV})})
    with pytest.raises(ValueError, match="exactamente un CSV"):
        h.run({"year": 2026, "month": 8}, FakeS3(), http, "bkt")


def test_only_https_allowed() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        h.UrllibHttp().exists("http://transtats.bts.gov/x.zip")


def _lookup_payloads(body: bytes) -> dict[str, bytes]:
    return {h.LOOKUP_URL_TEMPLATE.format(code=c): body for c in h.LOOKUPS.values()}


def test_lookups_ingested_to_landing() -> None:
    body = b'Code,Description\n"AA","American Airlines Inc."\n'
    s3 = FakeS3()
    out = h.run({"lookups": True}, s3, FakeHttp(_lookup_payloads(body)), "bkt")
    assert out["status"] == "lookups_ingested"
    assert s3.objects["landing/lookups/L_UNIQUE_CARRIERS.csv"] == body
    assert s3.objects["landing/lookups/L_AIRPORT_ID.csv"] == body


def test_lookups_reject_unexpected_payload() -> None:
    http = FakeHttp(_lookup_payloads(b"<html>error</html>"))
    with pytest.raises(ValueError, match="respuesta inesperada"):
        h.run({"lookups": True}, FakeS3(), http, "bkt")


def test_month_outputs_include_year_month() -> None:
    s3, http = FakeS3(), FakeHttp({AUG_URL: make_zip({"d.csv": CSV})})
    assert h.run({"year": 2026, "month": 8}, s3, http, "bkt")["year_month"] == "2026-08"


def test_plan_backfill_range() -> None:
    out = h.run({"plan": {"start": "2025-11", "end": "2026-02"}}, FakeS3(), FakeHttp({}), "bkt")
    assert out["status"] == "planned" and out["count"] == 4
    assert out["months"][0] == {"year": 2025, "month": 11}
    assert out["months"][-1] == {"year": 2026, "month": 2}
    assert out["year_months"] == "2025-11:2026-02"


def test_plan_backfill_full_project_period() -> None:
    out = h.plan_backfill({"start": "2023-01", "end": "2026-08"})
    assert out["count"] == 44


@pytest.mark.parametrize(
    "plan",
    [{"start": "2026-08", "end": "2026-01"}, {"start": "2026-8x", "end": "2026-09"},
     {"start": "2000-01", "end": "2026-08"}],
)  # fmt: skip
def test_plan_backfill_invalid(plan: dict) -> None:
    with pytest.raises(ValueError):
        h.plan_backfill(plan)
