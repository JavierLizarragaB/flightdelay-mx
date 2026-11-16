"""Fixtures compartidas. La sesión de Spark se crea una sola vez (tarda ~5 s)."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

_HOMEBREW_JAVA17 = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"


@pytest.fixture(scope="session")
def spark() -> Iterator:
    if "JAVA_HOME" not in os.environ and os.path.isdir(_HOMEBREW_JAVA17):
        os.environ["JAVA_HOME"] = _HOMEBREW_JAVA17
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("flightdelay-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
