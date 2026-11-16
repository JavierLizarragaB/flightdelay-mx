# Imagen de AWS Lambda (contenedor) para la API. Build desde la raíz del repo:
#   docker build --platform linux/arm64 -f api/Dockerfile -t flightdelay-api .
# Base python:3.12 (Amazon Linux 2023, glibc 2.34): las ruedas manylinux_2_28 de pandas 3 y
# LightGBM no instalan sobre la base 3.11 (Amazon Linux 2, glibc 2.26).
FROM public.ecr.aws/lambda/python:3.12

# LightGBM necesita OpenMP en tiempo de ejecución.
RUN dnf install -y libgomp && dnf clean all

COPY api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Solo el código que la API usa: fórmulas compartidas con oro y la matriz de features.
COPY src/__init__.py ${LAMBDA_TASK_ROOT}/src/__init__.py
COPY src/gold ${LAMBDA_TASK_ROOT}/src/gold
COPY src/ml/__init__.py src/ml/matrix.py ${LAMBDA_TASK_ROOT}/src/ml/
COPY api/__init__.py api/main.py api/schemas.py api/inference.py api/model_store.py api/lambda_handler.py ${LAMBDA_TASK_ROOT}/api/

CMD ["api.lambda_handler.handler"]
