"""Punto de entrada de AWS Lambda: API Gateway HTTP API (payload v2) -> FastAPI vía Mangum."""

from mangum import Mangum

from api.main import app

handler = Mangum(app, lifespan="off")
