"""Análisis exploratorio: consulta fd_silver en Athena y genera las gráficas del README.

Uso:  make eda        (o: python notebooks/eda_charts.py)
Salida: docs/img/eda_*.png  +  docs/img/eda_data.json (los números de cada gráfica)

Diseño: una medida por gráfica (sin doble eje), una sola tinta por serie, etiquetas
directas solo donde aportan, rejilla tenue.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ml.data import run_athena  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "docs" / "img"

# Paleta de referencia (modo claro): superficie, tintas y slots categóricos 1-2.
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
BLUE, ORANGE = "#2a78d6", "#eb6834"
MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
DAYS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]

QUERIES = {
    "monthly": """
        WITH f AS (SELECT month AS m, count(*) n, avg(arr_del15) r
                   FROM fd_silver.flights GROUP BY 1),
             d AS (SELECT month AS m, sum(cancelled) c, sum(diverted) dv,
                          sum(CASE WHEN cancellation_code = 'B' THEN 1 ELSE 0 END) cb
                   FROM fd_silver.flights_disrupted GROUP BY 1)
        SELECT f.m, f.r * 100, d.c * 100.0 / (f.n + d.c + d.dv), d.cb * 100.0 / d.c
        FROM f JOIN d ON f.m = d.m ORDER BY 1""",
    "hourly": """SELECT dep_hour, avg(arr_del15) * 100
                 FROM fd_silver.flights GROUP BY 1 ORDER BY 1""",
    "weekday": """SELECT day_of_week, avg(arr_del15) * 100
                  FROM fd_silver.flights GROUP BY 1 ORDER BY 1""",
    "causes": """SELECT sum(late_aircraft_delay), sum(carrier_delay), sum(nas_delay),
                        sum(weather_delay), sum(security_delay) FROM fd_silver.flights""",
    "airlines": """SELECT airline_name, avg(arr_del15) * 100, count(*) FROM fd_silver.flights
                   GROUP BY 1 HAVING count(*) > 300000 ORDER BY 2""",
    "overall": """SELECT avg(arr_del15) * 100 FROM fd_silver.flights""",
}


def query(athena, sql: str) -> list[list[str]]:
    qid = run_athena(athena, sql)
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    return rows[1:]  # sin encabezado


def style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left", fontsize=12, color=INK, fontweight="bold", pad=22)
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9, color=INK2)


def label(ax, x, y, text, **kw) -> None:
    ax.annotate(text, (x, y), xytext=(0, 4), textcoords="offset points", ha="center",
                fontsize=8.5, color=INK, **kw)  # fmt: skip


def chart_seasonality(data) -> None:
    m = [int(r[0]) - 1 for r in data]
    delay = [float(r[1]) for r in data]
    cancel = [float(r[2]) for r in data]
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(8, 6), sharex=True, facecolor=SURFACE)
    a1.bar(m, delay, color=BLUE, width=0.62)
    style(a1, "Verano: más retrasos", "% de vuelos que llegan ≥ 15 min tarde, por mes (2023–2026)")
    a1.set_ylim(0, 34)
    for i in (delay.index(max(delay)), delay.index(min(delay))):
        label(a1, m[i], delay[i], f"{delay[i]:.1f} %")
    a2.bar(m, cancel, color=ORANGE, width=0.62)
    style(
        a2,
        "Invierno: más cancelaciones",
        "% de vuelos cancelados, por mes (en enero, 76 % por clima)",
    )
    a2.set_ylim(0, 4)
    for i in (cancel.index(max(cancel)), cancel.index(min(cancel))):
        label(a2, m[i], cancel[i], f"{cancel[i]:.2f} %")
    a2.set_xticks(range(12), MONTHS)
    fig.tight_layout(h_pad=2.5)
    fig.savefig(OUT / "eda_estacionalidad.png", dpi=160)
    plt.close(fig)


def chart_hourly(data) -> None:
    h = [int(r[0]) for r in data]
    v = [float(r[1]) for r in data]
    fig, ax = plt.subplots(figsize=(8, 3.4), facecolor=SURFACE)
    ax.bar(h, v, color=BLUE, width=0.7)
    style(ax, "El retraso se acumula durante el día",
          "% de vuelos que llegan ≥ 15 min tarde, por hora programada de salida")  # fmt: skip
    ax.set_xticks(range(0, 24, 2), [f"{x}:00" for x in range(0, 24, 2)])
    ax.set_ylim(0, 36)
    for i in (v.index(min(v)), v.index(max(v))):
        label(ax, h[i], v[i], f"{v[i]:.1f} %")
    fig.tight_layout()
    fig.savefig(OUT / "eda_hora.png", dpi=160)
    plt.close(fig)


def chart_causes(row) -> None:
    names = ["Avión anterior llegó tarde", "Aerolínea (tripulación, mantenimiento)",
             "Control aéreo (NAS)", "Clima extremo", "Seguridad"]  # fmt: skip
    vals = [float(x) / 1e6 for x in row]
    total = sum(vals)
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    fig, ax = plt.subplots(figsize=(8, 3.2), facecolor=SURFACE)
    ax.barh([names[i] for i in order], [vals[i] for i in order], color=BLUE, height=0.55)
    style(ax, "El retraso es sobre todo un efecto dominó",
          "Millones de minutos de retraso por causa reportada (BTS, 2023–2026)")  # fmt: skip
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    for y, i in enumerate(order):
        ax.text(vals[i] + total * 0.006, y, f"{vals[i]:.1f} M ({vals[i] / total:.0%})",
                va="center", fontsize=8.5, color=INK)  # fmt: skip
    ax.set_xlim(0, max(vals) * 1.25)
    fig.tight_layout()
    fig.savefig(OUT / "eda_causas.png", dpi=160)
    plt.close(fig)


def chart_airlines(data, overall: float) -> None:
    names = [r[0].replace(" Inc.", "").replace(" Co.", "") for r in data]
    v = [float(r[1]) for r in data]
    fig, ax = plt.subplots(figsize=(8, 4.6), facecolor=SURFACE)
    ax.barh(names, v, color=BLUE, height=0.6, zorder=2)
    # La línea del promedio va detrás de las barras y su etiqueta abajo del eje, para no
    # chocar con los valores (que van dentro de cada barra).
    ax.axvline(overall, color=INK2, linewidth=1, linestyle=(0, (3, 3)), zorder=1)
    ax.text(overall + 0.3, -0.95, f"promedio {overall:.1f} %", color=INK2, fontsize=8.5,
            ha="left", va="top")  # fmt: skip
    style(ax, "Elegir aerolínea importa: casi el doble entre extremos",
          "% de vuelos que llegan ≥ 15 min tarde (aerolíneas con > 300 mil vuelos)")  # fmt: skip
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    for y, val in enumerate(v):
        ax.text(val - 0.4, y, f"{val:.1f} %", va="center", ha="right", fontsize=8,
                color="white", fontweight="bold", zorder=4)  # fmt: skip
    ax.set_xlim(0, max(v) * 1.05)
    ax.set_ylim(-1.6, len(names) - 0.4)
    fig.tight_layout()
    fig.savefig(OUT / "eda_aerolineas.png", dpi=160)
    plt.close(fig)


def main() -> None:
    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE", "flightdelay"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    athena = session.client("athena")
    data = {k: query(athena, sql) for k, sql in QUERIES.items()}
    OUT.mkdir(parents=True, exist_ok=True)
    chart_seasonality(data["monthly"])
    chart_hourly(data["hourly"])
    chart_causes(data["causes"][0])
    chart_airlines(data["airlines"], float(data["overall"][0][0]))
    (OUT / "eda_data.json").write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n")
    print("Gráficas en", OUT)


if __name__ == "__main__":
    main()
