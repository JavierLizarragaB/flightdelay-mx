# CLAUDE.md — FlightDelay-MX

Contexto del proyecto para Claude Code. Léelo completo antes de escribir código.

---

## 1. Qué es este proyecto

Proyecto final de la materia de Big Data / ingeniería de datos de la **Maestría en Ciencia de Datos e Innovación Tecnológica de Negocios (Universidad Panamericana)**.

**Pregunta de negocio:** al momento de comprar un boleto, ¿qué tan probable es que un vuelo doméstico de EE.UU. llegue con ≥ 15 minutos de retraso?

**Entregable:** un repositorio de GitHub cuyo `README.md` explica el proyecto. El `README.md` ya existe en la raíz del repo y describe el diseño objetivo. **Si la implementación se desvía del README, actualiza el README.** Hay campos `_[ ]_` pendientes de llenar con resultados reales.

**El desarrollador:** Javier, Senior Full Stack Developer con experiencia en datos. Comunícate en **español**. Prefiere salidas estructuradas y accionables (checklists, comandos concretos) sobre explicaciones abstractas.

---

## 2. Fechas (no negociables)

| Fecha | Hito |
|---|---|
| **Jue 20 nov 2026** | Demo 1 en clase: ejecución en vivo y explicación. Mínimo: pipeline de ingesta + bronce + plata corriendo en AWS, consultas en Athena, modelo preliminar |
| **Jue 27 nov 2026** | Demo 2: proyecto completo de punta a punta + README con resultados |

Prioriza siempre lo que se pueda **ejecutar en vivo** en la siguiente demo.

---

## 3. Rúbrica (guía todas las decisiones)

| Criterio | Peso | Qué exige |
|---|---|---|
| 1. Limpieza y transformación | **35%** | Tratamiento **justificado** de nulos, duplicados, outliers y tipos. Normalización y codificación. Arquitectura Medallion (bronce/plata/oro) |
| 2. Definición del problema y métricas | **10%** | Tipo de tarea + métrica principal justificada (F1 por desbalance) |
| 3. Publicación del modelo | **25%** | Modelo como servicio consultable (API REST / Docker / nube). **Swagger/OpenAPI** y **validación de entradas** |
| 4. Integración con la app | **30%** | La app consume el **endpoint publicado**, NO un modelo copiado localmente. Maneja **errores, timeouts y respuestas inválidas** |

Requisito adicional por ser un proyecto nuevo: **≥ 4 millones de registros** con pipelines/ETL/Medallion/jobs de Spark + una capa de resultados que consulte un modelo de ML publicado.

Cada decisión de limpieza debe quedar documentada **con su justificación** (en el código y en la tabla de la sección 4 del README). Eso vale el 35%.

---

## 4. Datos

- **Fuente:** BTS — *Reporting Carrier On-Time Performance (1987–present)*. Página: https://www.transtats.bts.gov/Tables.asp?QO_VQ=EFD
- **Descarga:** un ZIP por mes. Patrón conocido de archivo pre-empaquetado (**verificar que siga vigente** antes de depender de él):
  `https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YYYY}_{M}.zip` (mes sin cero a la izquierda).
- **Periodo:** enero 2023 → último mes publicado (~6–7 M vuelos/año → 15–20 M registros).
- **Objetivo:** `ArrDel15` (1 = llegó ≥ 15 min tarde). Desbalance aproximado de 80/20.
- **Catálogos:** `L_UNIQUE_CARRIERS`, `L_AIRPORT` (BTS lookup tables).

### ⚠️ Regla crítica: NO fuga de datos

Solo se pueden usar como features las variables conocidas **antes** del vuelo. **Prohibido** usar como feature:
`DepTime`, `DepDelay`, `DepDel15`, `TaxiOut`, `TaxiIn`, `WheelsOff`, `WheelsOn`, `ArrTime`, `ArrDelay`, `ActualElapsedTime`, `AirTime`, `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`, `Cancelled`, `Diverted`.

La lista de features permitidas y prohibidas vive en **un solo lugar** (`src/gold/feature_config.py` o equivalente), y hay un test que falla si una columna prohibida entra al modelo.

Las features históricas (`route_delay_rate_30d`, etc.) se calculan con **ventanas desfasadas** que solo ven el pasado. La validación es con **split temporal**, nunca aleatorio.

---

## 5. Arquitectura en AWS (decidida)

Región por defecto: **`us-east-1`**. Perfil de AWS CLI: **`flightdelay`**. Infraestructura en **Terraform**.

| Capa | Servicio |
|---|---|
| Almacenamiento | **S3**: `s3://<bucket>/landing/`, `bronze/`, `silver/`, `gold/`, `models/`, `reports/` |
| Formato de tablas | **Apache Iceberg** (bronce, plata, oro) |
| Catálogo y SQL | **Glue Data Catalog** + **Athena** (bases `fd_bronze`, `fd_silver`, `fd_gold`) |
| Ingesta | **Lambda (Python)**: descarga el ZIP mensual de BTS → descomprime → `landing/` en S3 |
| Transformación | **AWS Glue jobs (PySpark, Glue 4.0+ con soporte Iceberg)**: un job por capa |
| Orquestación / job programado | **Step Functions** (ingesta → bronce → plata → oro) disparado por **EventBridge Scheduler** mensual |
| Entrenamiento | LightGBM sobre la tabla oro (Glue Python Shell job o script local que lee de S3/Athena). El artefacto va a `s3://<bucket>/models/` |
| API del modelo | **FastAPI** en imagen Docker → **ECR** → **Lambda (container image)** + **API Gateway HTTP API** (Mangum como adaptador) |
| App web | **Streamlit** en Docker → **ECS Fargate** (encender para demos para ahorrar) |

**Descartado a propósito:** MWAA (Airflow administrado; demasiado caro para un proyecto escolar), SageMaker endpoints (costo fijo innecesario), servir el modelo con Spark MLlib (requiere sesión de Spark en la API).

### Costos

- Javier debe tener una **alerta de presupuesto** en AWS Budgets antes de desplegar.
- Usa los workers y DPUs mínimos en Glue (`G.1X`, pocos workers) y activa *auto scaling* donde aplique.
- Todo debe poder destruirse con `terraform destroy`. Documenta qué recursos generan costo fijo (Fargate, NAT Gateway si llegara a usarse; **evita NAT Gateway**).

---

## 6. Diseño Medallion (resumen; detalle en README sección 4)

**Bronce:** datos tal cual, todas las columnas como `string`, más `_source_file`, `_ingested_at`, `_year`, `_month`. Particionado por año y mes. Idempotente: re-procesar un mes sobrescribe solo esa partición.

**Plata:**
- Cast de tipos; `CRSDepTime` tipo `hhmm` como entero (`530`, `2400`) → hora y minuto (`2400` = 00:00 del día siguiente).
- Deduplicación por `FlightDate + Reporting_Airline + Flight_Number_Reporting_Airline + Origin + Dest + CRSDepTime`.
- Vuelos cancelados o desviados → tabla separada `silver.flights_disrupted`; se excluyen del modelo.
- Causas de retraso nulas → 0.
- `Distance <= 0` o `CRSElapsedTime <= 0` → se descartan y se cuentan.
- Outliers de `ArrDelay` se conservan; se marca `is_extreme_delay` (> P99.9).
- Join con catálogos.
- Reporte de calidad por corrida (`reports/quality_<yyyy_mm>.json`): conteos de entrada y salida, nulos por columna, duplicados y descartes por regla.

**Oro:**
- `gold.features_flights`: airline, origin, dest (target encoding con suavizado), hora y mes cíclicos (sin/cos), día de la semana, distancia, `days_to_holiday`, `route_delay_rate_30d`, `origin_delay_rate_30d`, `airline_delay_rate_30d`, objetivo `arr_del15`.
- `gold.lookup_route`, `lookup_origin`, `lookup_airline`: la última estadística histórica, usada por la API en inferencia.

---

## 7. Modelo y API

- **Tarea:** clasificación binaria. **Métrica principal:** F1 de la clase positiva. Secundarias: PR-AUC, ROC-AUC, matriz de confusión.
- **Modelos:** baseline "siempre a tiempo" → regresión logística → **LightGBM** (final).
- **Umbral:** el que maximiza F1 en validación; se guarda en `metadata.json`.
- **Split temporal:** entrenamiento 2023 → jun 2025; validación jul–sep 2025; prueba oct 2025 en adelante (ajustar a los datos disponibles).

**Endpoints:** `GET /health`, `GET /metadata`, `GET /catalog/airlines`, `GET /catalog/airports`, `POST /predict`, `POST /predict/batch` (máximo 100).

**Request de `/predict`:** `airline`, `origin`, `dest`, `flight_date` (`YYYY-MM-DD`), `crs_dep_time` (`HH:MM`).
**Validación (Pydantic):** códigos existentes en el catálogo, `origin ≠ dest`, fecha entre hoy y +365 días, hora válida. Ruta sin historial → fallback al promedio del aeropuerto + `warning`.
**Errores con formato uniforme:** `{"error", "detail", "field"}`.

## 8. App (Streamlit)

- Solo habla con la API vía HTTP (`API_URL` por variable de entorno). Los catálogos también se piden a la API.
- Timeout de 5 s, 2 reintentos con *backoff* exponencial.
- Manejo explícito de: API caída, timeout, `422` (muestra el campo), `5xx`, JSON inválido o campos faltantes (validación con Pydantic del lado cliente), probabilidad fuera de [0, 1].

---

## 9. Estructura objetivo del repo

```text
README.md  CLAUDE.md  Makefile  .gitignore  .env.example
infra/terraform/            # módulos: s3, iam, glue, athena, lambda_ingest, glue_jobs, stepfunctions, ecr, api, ecs_app
src/ingestion/              # lambda de descarga BTS
src/glue_jobs/              # bronze_job.py, silver_job.py, gold_job.py (PySpark)
src/common/                 # esquemas, config, utilidades compartidas
src/gold/feature_config.py  # features permitidas/prohibidas (fuente única)
src/ml/                     # train.py, evaluate.py, export.py
api/                        # FastAPI + Dockerfile + tests
app/                        # Streamlit + api_client.py + Dockerfile
orchestration/              # definición de Step Functions (ASL) si no vive en Terraform
notebooks/                  # EDA y experimentos
tests/                      # unit tests de transformaciones (pyspark local) y API
docs/img/                   # capturas para el README
```

`data/`, `models/` y `.env` van en `.gitignore`.

---

## 10. Convenciones

- Python 3.11, type hints, `ruff` para lint y formato, `pytest` para tests.
- Las transformaciones de Spark viven en **funciones puras** (`DataFrame -> DataFrame`) testeables con PySpark local; el script del Glue job solo orquesta.
- Configuración (bucket, rutas, años) en variables de entorno o parámetros del job, **nunca hardcodeada**.
- **Nunca** escribas credenciales de AWS en el código, en el repo ni en el chat. Usa el perfil `flightdelay`.
- Commits pequeños y descriptivos, en español.
- Antes de un `terraform apply` o de cualquier acción que cree recursos con costo, muestra el plan y pide confirmación.

---

## 11. Estado actual y primeros pasos

**Hecho:**
- [x] Elección del dataset (BTS On-Time Performance) y del problema.
- [x] `README.md` con el diseño completo (en la raíz del repo).
- [x] Arquitectura de AWS decidida (sección 5).

- [x] Repo en `~/Documents/flightdelay-mx`, estructura base, `Makefile`, `pyproject.toml`, `feature_config.py` + test (15 nov 2026).
- [x] README alineado con la arquitectura AWS (Iceberg, Glue, Step Functions).
- [x] Terraform base aplicado (15 nov 2026): bucket `flightdelay-mx-382563911929`, bases `fd_bronze/fd_silver/fd_gold`, workgroup de Athena `flightdelay-mx` (tope 10 GB/consulta), roles `flightdelay-mx-glue-job` y `flightdelay-mx-lambda-ingest`. Estado local en `infra/terraform/`. Presupuesto AWS: USD 20/mes.
- [x] Paso 5 — Lambda de ingesta `flightdelay-mx-ingest` desplegada y probada (15 nov 2026): ago 2026 en `landing/year=2026/month=08/` con `_manifest.json` (608,389 filas, 18 s). Idempotente vía manifiesto. `make ingest YM=…` / `make ingest-latest`.
- [x] Paso 6 — Glue job `flightdelay-mx-bronze` (Glue 5.0, 2×G.1X) desplegado (15 nov 2026): `fd_bronze.flights_raw` Iceberg con ago 2026 (608,389 filas = manifiesto, 18 MB). Idempotencia verificada (2.ª corrida = snapshot overwrite, sin duplicados). Lógica en `src/glue_jobs/bronze.py`; el paquete `src/` va como ZIP en `--extra-py-files`. `make bronze YM=…`.
- [x] Paso 7 — Glue job `flightdelay-mx-silver` desplegado (15 nov 2026): `fd_silver.flights` (594,207) + `flights_disrupted` (14,182) + `airlines`/`airports`; reporte `reports/quality_2026_08.json` con conciliación (cuadra). Catálogos vía Lambda `{"lookups": true}` (`make lookups`). Reglas en `src/glue_jobs/silver.py` (26 tests). `scheduled_dep_local` es TIMESTAMP_NTZ (hora local sin zona). Helpers compartidos en `src/glue_jobs/iceberg.py`. `make silver YM=…`, `make quality YM=…`.
- [x] Paso 8 — Step Function `flightdelay-mx-pipeline` (ASL en `orchestration/pipeline.asl.json`) + EventBridge Scheduler (día 15, 06:00 CDMX). Modos: `{}` mensual, `{"year","month","reprocess"}`, `{"backfill":{"start","end"}}` (Map de 4). Backfill 2023-01→2026-08 ejecutado (48 min): **25,657,311** filas en bronce = manifiestos; plata 25,196,995 vuelos + 460,286 interrumpidos; 30 descartes reales; conciliación OK en 44/44. Idempotencia de plata verificada.
- [x] Paso 9 — Glue job `flightdelay-mx-gold` (recalculo completo, 4×G.1X, 4.6 min): `fd_gold.features_flights` (25,196,995) + `lookup_route/origin/dest/airline/global`. TE expansivo por mes (ene 2023 = calentamiento) y tasas 30d con ventana [d-30, d-1]. Fórmulas compartidas con la API en `src/gold/features.py`.
- [x] Paso 10 — `make train` (local, Athena UNLOAD → Parquet en `data/gold_export/`): baseline / logística / LightGBM. Prueba (oct 2025–ago 2026, 6.3 M): **LightGBM F1 0.438, PR-AUC 0.384, ROC-AUC 0.679**; logística F1 0.433. Umbral 0.2367. Modelo `v20261116-022934` en `s3://…/models/` + `models/latest.json`. Métricas en `reports/model_metrics.json`.
- [x] Paso 11 — API FastAPI en Lambda (imagen arm64, base python:3.12 por glibc) + API Gateway HTTP API: `make api-url` (Swagger en `/docs`). Carga el modelo de S3 en el arranque (sin rebuild al reentrenar); 503 uniforme si no hay modelo. 24 tests en `api/tests`.
- [x] Paso 12 — App Streamlit en ECS Fargate (arm64, IP pública, sin ALB), `desired_count=0`: `make app-up` / `make app-url` / `make app-down`. Cliente con timeout 5 s, 2 reintentos, validación Pydantic (15 tests). Probada de punta a punta.
- [x] Paso 13 (parcial) — README con resultados reales, `reports/quality_summary.json`, capturas `docs/img/swagger.png` y `app.png`. Glue total ≈ USD 1.05.

**Hallazgos de la muestra BTS (ago 2026, verificados 15 nov 2026):**
- Último mes publicado: **ago 2026** (sep 2026 da 404). ZIP ~32 MB, CSV ~263 MB, 608,389 filas.
- 110 columnas: 109 con nombre + 1 vacía por coma final. Todo entre comillas; numéricos como `"0.00"`.
- `CRSDepTime` viene con 4 dígitos (`"0006"`–`"2359"`, sin `2400`); el parser debe aceptar también `530` y `2400`.
- Nulos de `ArrDel15` = cancelados + desviados exactamente. Causas de retraso no nulas solo cuando `ArrDel15 = 1`.
- Desbalance en agosto: 26.9 % positivos (no 20 %).
- Columnas con fuga adicionales a la lista de la sección 4: `DepDelayMinutes`, `DepartureDelayGroups`, `ArrDelayMinutes`, `ArrivalDelayGroups`, `CancellationCode`, `FirstDepTime`, `TotalAddGTime`, `LongestAddGTime`, `Div*` (ya en `feature_config.py`).
- Decisión propuesta: **Glue 5.0** (Python 3.11, Spark 3.5, Iceberg nativo) en lugar de 4.0.

**Pendiente de confirmar con Javier:**
- [ ] ¿Individual o en pareja? Nombre(s) y nombre de la materia (README líneas 5–6).
- [ ] Nombre final del repo de GitHub; el repo **aún no tiene commits** ni remoto.
- [x] Presupuesto: AWS Budgets USD 20/mes.

**Pendiente (no código):**
- [ ] Capturas desde la consola (Athena, grafo de Step Functions, reporte de calidad): ver `docs/img/README.md`.
- [ ] Primer(os) commit(s) y push a GitHub.
- [ ] Antes de cada demo: `make app-up` (y `make app-down` al terminar).

**Siguientes tareas:** todas las del plan original (1–13) están hechas; quedan los pendientes de arriba. Ideas si sobra tiempo: reentrenamiento como paso final de la Step Function, calibración de probabilidades, clima de NOAA como segunda fuente.
