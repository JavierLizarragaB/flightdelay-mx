# App Streamlit. Build desde la raíz del repo:
#   docker build --platform linux/arm64 -f app/Dockerfile -t flightdelay-app .
# Solo contiene el cliente HTTP: el modelo vive detrás de la API (API_URL).
FROM python:3.11-slim

# PYTHONPATH: Streamlit solo agrega la carpeta del script; así `from app.api_client` funciona.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/srv \
    STREAMLIT_SERVER_HEADLESS=true STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /srv
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && useradd --create-home appuser

COPY app/__init__.py app/api_client.py app/streamlit_app.py /srv/app/
USER appuser

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=4)"

CMD ["streamlit", "run", "app/streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501"]
