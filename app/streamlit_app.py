"""FlightDelay-MX — app web. Consume SOLO la API publicada (API_URL); no carga ningún modelo."""

from __future__ import annotations

import os
from datetime import date, time, timedelta

import pandas as pd
import streamlit as st

from app.api_client import (
    ApiClient,
    ApiError,
    ApiValidationError,
    FlightIn,
)

st.set_page_config(page_title="FlightDelay-MX", page_icon="✈️", layout="centered")

FIELD_LABELS = {
    "airline": "Aerolínea",
    "origin": "Origen",
    "dest": "Destino",
    "flight_date": "Fecha",
    "crs_dep_time": "Hora de salida",
}


@st.cache_resource
def client() -> ApiClient:
    return ApiClient(os.environ.get("API_URL"))


@st.cache_data(ttl=600, show_spinner="Cargando catálogos desde la API…")
def catalogs() -> tuple[list[dict], list[dict]]:
    c = client()
    return [a.model_dump() for a in c.airlines()], [a.model_dump() for a in c.airports()]


def show_error(e: ApiError) -> None:
    if isinstance(e, ApiValidationError):
        label = FIELD_LABELS.get((e.field or "").split(".")[-1], e.field or "solicitud")
        st.error(f"**{label}:** {e.detail}")
    else:
        st.error(f"{e.user_message}\n\nDetalle: `{e.detail}`")


# --------------------------------------------------------------------------- encabezado
st.title("✈️ FlightDelay-MX")
st.caption(
    "¿Qué tan probable es que tu vuelo doméstico en EE. UU. llegue **15 minutos o más** tarde? "
    "Predicción de un modelo LightGBM publicado como API REST en AWS."
)

with st.sidebar:
    st.subheader("Estado del servicio")
    try:
        h = client().health()
        if h.model_loaded:
            st.success(f"API en línea · modelo `{h.model_version}`")
        else:
            st.warning(f"API en línea, modelo no cargado: {h.detail}")
    except ApiError as e:
        st.error(e.user_message)
    except ValueError as e:
        st.error(str(e))
    st.caption(f"API: `{os.environ.get('API_URL', '(sin configurar)')}`")
    st.caption("Timeout 5 s · 2 reintentos con backoff exponencial")

# --------------------------------------------------------------------------- catálogos
try:
    airlines, airports = catalogs()
except ApiError as e:
    show_error(e)
    if st.button("Reintentar"):
        st.cache_data.clear()
        st.rerun()
    st.stop()

airline_opts = {f"{a['name']} ({a['code']})": a["code"] for a in airlines}
airport_opts = {f"{a['code']} — {a['city'] or a['name']}": a["code"] for a in airports}
airport_labels = list(airport_opts)


def _index(code: str) -> int:
    return next((i for i, k in enumerate(airport_labels) if airport_opts[k] == code), 0)


# --------------------------------------------------------------------------- formulario
with st.form("vuelo"):
    airline_label = st.selectbox("Aerolínea", list(airline_opts))
    c1, c2 = st.columns(2)
    origin_label = c1.selectbox("Origen", airport_labels, index=_index("DFW"))
    dest_label = c2.selectbox("Destino", airport_labels, index=_index("LAX"))
    c3, c4 = st.columns(2)
    flight_date = c3.date_input(
        "Fecha del vuelo",
        value=date.today() + timedelta(days=14),
        min_value=date.today(),
        max_value=date.today() + timedelta(days=365),
    )
    dep_time = c4.time_input(
        "Hora de salida (local)", value=time(18, 30), step=timedelta(minutes=5)
    )
    compare = st.checkbox("Comparar con otros horarios del mismo día (/predict/batch)", value=True)
    submitted = st.form_submit_button("Predecir retraso", type="primary")

if submitted:
    flight = FlightIn(
        airline=airline_opts[airline_label],
        origin=airport_opts[origin_label],
        dest=airport_opts[dest_label],
        flight_date=flight_date,
        crs_dep_time=dep_time.strftime("%H:%M"),
    )
    try:
        with st.spinner("Consultando la API…"):
            pred = client().predict(flight)
    except ApiError as e:
        show_error(e)
        st.stop()

    pct = pred.probability * 100
    icon = {"bajo": "🟢", "medio": "🟡", "alto": "🔴"}[pred.risk_level]
    st.subheader(f"{icon} Riesgo {pred.risk_level}: {pct:.0f} % de probabilidad de retraso")
    st.progress(min(max(pred.probability, 0.0), 1.0))
    m1, m2, m3 = st.columns(3)
    m1.metric("Probabilidad", f"{pct:.1f} %")
    m2.metric("¿Retraso ≥ 15 min?", "Probable" if pred.delayed else "Poco probable")
    m3.metric("Umbral del modelo", f"{pred.threshold * 100:.0f} %")
    for w in pred.warnings:
        st.warning(w)
    st.caption(f"Modelo `{pred.model_version}`")

    if compare:
        hours = [time(h, 0) for h in range(5, 24, 2)]
        batch = [flight.model_copy(update={"crs_dep_time": t.strftime("%H:%M")}) for t in hours]
        try:
            out = client().predict_batch(batch)
            df = pd.DataFrame(
                {
                    "Hora de salida": [t.strftime("%H:%M") for t in hours],
                    "Probabilidad de retraso (%)": [p.probability * 100 for p in out.predictions],
                }
            ).set_index("Hora de salida")
            st.markdown("**Mismo vuelo a otras horas del día**")
            st.bar_chart(df)
        except ApiError as e:
            show_error(e)

with st.expander("¿Cómo funciona?"):
    st.markdown(
        "- Esta app **no contiene el modelo**: envía tu vuelo a la API REST publicada en AWS "
        "(Lambda + API Gateway) y muestra la respuesta.\n"
        "- El modelo solo usa datos conocidos al comprar el boleto: aerolínea, ruta, fecha, hora "
        "y el historial reciente de retrasos de la ruta, el aeropuerto y la aerolínea.\n"
        "- Datos: BTS On-Time Performance 2023 → 2026 (~24 M de vuelos)."
    )
