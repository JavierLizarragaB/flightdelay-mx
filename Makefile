# Atajos del proyecto. Variables de entorno en .env (ver .env.example).
-include .env
export

# Perfil y región de AWS (sobreescribibles en .env); las credenciales nunca viven en el repo.
AWS_PROFILE ?= flightdelay
AWS_REGION  ?= us-east-1
export AWS_PROFILE AWS_REGION

PY      ?= python3.11
VENV    ?= .venv
BIN     := $(VENV)/bin
TF_DIR  := infra/terraform
YM      ?= 2026-08
# Java 17 de Homebrew (keg-only) para PySpark local; sobreescribible desde el entorno.
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

.DEFAULT_GOAL := help
.PHONY: help venv lint format test test-fast sample ingest ingest-latest lookups bronze silver gold quality pipeline backfill eda train train-compare ecr ecr-login api-image app-image api-url api-local app-local app-up app-down app-url tf-init tf-plan tf-apply tf-destroy clean

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Crea el entorno virtual con dependencias de desarrollo
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -q --upgrade pip
	$(BIN)/pip install -q -e ".[dev,spark,ml,api,app,eda]"

lint: ## Revisa estilo con ruff
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format: ## Formatea con ruff
	$(BIN)/ruff check --fix .
	$(BIN)/ruff format .

test: ## Corre todos los tests (los de Spark requieren Java 17)
	$(BIN)/pytest -q

test-fast: ## Corre tests sin Spark
	$(BIN)/pytest -q -m "not spark"

sample: ## Descarga un mes de BTS a data/ para EDA local (YM=2026-08)
	@y=$${YM%-*}; m=$$(echo $${YM#*-} | sed 's/^0//'); \
	mkdir -p data/sample_$${y}_$$(printf %02d $$m); \
	curl -fSL --max-time 300 -o data/sample_$${y}_$$(printf %02d $$m)/bts.zip \
	  "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_$${y}_$${m}.zip"

ingest: ## Invoca la Lambda de ingesta para un mes en AWS (YM=2026-08; FORCE=true re-descarga)
	@y=$${YM%-*}; m=$$(echo $${YM#*-} | sed 's/^0//'); \
	aws lambda invoke --function-name flightdelay-mx-ingest --cli-read-timeout 960 \
	  --cli-binary-format raw-in-base64-out \
	  --payload "{\"year\":$$y,\"month\":$$m,\"force\":$${FORCE:-false}}" /dev/stdout --query StatusCode

ingest-latest: ## Invoca la Lambda sin mes: ingiere el último mes publicado por BTS
	aws lambda invoke --function-name flightdelay-mx-ingest --cli-read-timeout 960 \
	  --cli-binary-format raw-in-base64-out --payload '{}' /dev/stdout --query StatusCode

lookups: ## Descarga los catálogos de BTS (aerolíneas, aeropuertos) a landing/lookups/
	aws lambda invoke --function-name flightdelay-mx-ingest --cli-read-timeout 300 \
	  --cli-binary-format raw-in-base64-out --payload '{"lookups":true}' /dev/stdout --query StatusCode

# Corre un Glue job y espera a que termine: $(call glue_run,<capa>)
define glue_run
	@rid=$$(aws glue start-job-run --job-name flightdelay-mx-$(1) \
	  --arguments '{"--year_months":"$(YM)"}' --query JobRunId --output text); \
	echo "flightdelay-mx-$(1) run $$rid"; \
	while :; do \
	  s=$$(aws glue get-job-run --job-name flightdelay-mx-$(1) --run-id $$rid --query JobRun.JobRunState --output text); \
	  case $$s in SUCCEEDED|FAILED|ERROR|TIMEOUT|STOPPED) break;; esac; sleep 15; \
	done; \
	aws glue get-job-run --job-name flightdelay-mx-$(1) --run-id $$rid \
	  --query 'JobRun.{estado:JobRunState,segundos:ExecutionTime,dpu_s:DPUSeconds,error:ErrorMessage}'; \
	test "$$s" = SUCCEEDED
endef

bronze: ## Glue job landing -> fd_bronze (YM=2026-08 o rango 2023-01:2026-08)
	$(call glue_run,bronze)

silver: ## Glue job fd_bronze -> fd_silver + reporte de calidad (YM=2026-08 o rango)
	$(call glue_run,silver)

gold: ## Glue job fd_silver -> fd_gold (features + lookups; recálculo completo)
	$(call glue_run,gold)

pipeline: ## Corre la Step Function mensual completa ({} = último mes publicado)
	aws stepfunctions start-execution --state-machine-arn $$(terraform -chdir=$(TF_DIR) output -raw state_machine_arn) \
	  --input '{}' --query executionArn --output text

backfill: ## Step Function en modo histórico (FROM=2023-01 TO=2026-08)
	aws stepfunctions start-execution --state-machine-arn $$(terraform -chdir=$(TF_DIR) output -raw state_machine_arn) \
	  --input '{"backfill":{"start":"$(FROM)","end":"$(TO)"}}' --query executionArn --output text

eda: ## Regenera las gráficas del análisis exploratorio (docs/img/eda_*.png) desde Athena
	$(BIN)/python notebooks/eda_charts.py

train: ## Entrena (baseline, logística, LightGBM) y publica el modelo en s3://…/models/
	$(BIN)/python -m src.ml.train --bucket $$(terraform -chdir=$(TF_DIR) output -raw bucket_name) --upload $(TRAIN_ARGS)

train-compare: ## Compara además XGBoost y CatBoost (no publica; escribe reports/model_comparison.json)
	$(BIN)/python -m src.ml.train --bucket $$(terraform -chdir=$(TF_DIR) output -raw bucket_name) --compare --out-dir data/compare_models $(TRAIN_ARGS)

quality: ## Muestra el reporte de calidad de plata de un mes (YM=2026-08)
	@y=$${YM%-*}; m=$${YM#*-}; aws s3 cp s3://$$(terraform -chdir=$(TF_DIR) output -raw bucket_name)/reports/quality_$${y}_$${m}.json -

# --- Imágenes y despliegue de la API y la app -------------------------------------
ECR_REGISTRY = $(shell AWS_PROFILE=$(AWS_PROFILE) aws sts get-caller-identity --query Account --output text 2>/dev/null).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_TAG   ?= latest

ecr: ## Crea solo los repositorios ECR (primer despliegue, antes de subir imágenes)
	terraform -chdir=$(TF_DIR) apply -target=module.ecr

ecr-login:
	aws ecr get-login-password | docker login --username AWS --password-stdin $(ECR_REGISTRY)

api-image: ecr-login ## Construye y sube la imagen de la API (Lambda arm64)
	docker build --platform linux/arm64 --provenance=false -f api/Dockerfile -t $(ECR_REGISTRY)/flightdelay-mx-api:$(IMAGE_TAG) .
	docker push $(ECR_REGISTRY)/flightdelay-mx-api:$(IMAGE_TAG)

app-image: ecr-login ## Construye y sube la imagen de la app Streamlit (Fargate arm64)
	docker build --platform linux/arm64 --provenance=false -f app/Dockerfile -t $(ECR_REGISTRY)/flightdelay-mx-app:$(IMAGE_TAG) .
	docker push $(ECR_REGISTRY)/flightdelay-mx-app:$(IMAGE_TAG)

api-url: ## Muestra la URL pública de la API (Swagger en /docs)
	@terraform -chdir=$(TF_DIR) output -raw api_url; echo

api-local: ## Corre la API localmente con el último modelo descargado (MODEL_DIR=models/<versión>)
	MODEL_DIR=$${MODEL_DIR:-$$(ls -d models/v* | tail -1)} $(BIN)/uvicorn api.main:app --reload --port 8000

app-local: ## Corre la app localmente contra la API publicada
	PYTHONPATH=. API_URL=$${API_URL:-$$(terraform -chdir=$(TF_DIR) output -raw api_url)} $(BIN)/streamlit run app/streamlit_app.py

app-up: ## Enciende la app en Fargate (cuesta ~USD 0.015/h mientras corre)
	@# Sin --force-new-deployment: forzarlo reemplaza la tarea recién creada y su IP cambia.
	aws ecs update-service --cluster flightdelay-mx --service flightdelay-mx-app --desired-count 1 --query 'service.desiredCount'
	@echo "Esperando a que el servicio quede estable (~1-2 min)…"
	aws ecs wait services-stable --cluster flightdelay-mx --services flightdelay-mx-app
	@$(MAKE) -s app-url

app-down: ## Apaga la app en Fargate (costo cero)
	aws ecs update-service --cluster flightdelay-mx --service flightdelay-mx-app --desired-count 0 --query 'service.desiredCount'

app-url: ## URL pública de la tarea de Fargate en ejecución
	@task=$$(aws ecs list-tasks --cluster flightdelay-mx --service-name flightdelay-mx-app --desired-status RUNNING --query 'taskArns[0]' --output text); \
	eni=$$(aws ecs describe-tasks --cluster flightdelay-mx --tasks $$task --query "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value" --output text); \
	ip=$$(aws ec2 describe-network-interfaces --network-interface-ids $$eni --query 'NetworkInterfaces[0].Association.PublicIp' --output text); \
	echo "http://$$ip:8501"

tf-init: ## terraform init
	terraform -chdir=$(TF_DIR) init

tf-plan: ## terraform plan (guarda el plan en tfplan)
	terraform -chdir=$(TF_DIR) plan -out=tfplan

tf-apply: ## Aplica el plan guardado (revisa tf-plan antes: crea recursos con costo)
	terraform -chdir=$(TF_DIR) apply tfplan

tf-destroy: ## Destruye toda la infraestructura
	terraform -chdir=$(TF_DIR) destroy

clean: ## Limpia cachés locales
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
