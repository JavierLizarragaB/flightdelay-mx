# Capturas

| Archivo | Estado | Cómo obtenerla |
|---|---|---|
| `swagger.png` | ✅ | `https://<api>/docs` (headless Chrome) |
| `app.png` | ✅ | App en Fargate tras `make app-up` (Playwright) |
| `eda_*.png` | ✅ | `make eda` (matplotlib sobre consultas de Athena) |
| `athena.png` | Pendiente | Consola de Athena, workgroup `flightdelay-mx`: `SELECT dep_hour, round(avg(arr_del15)*100,1) FROM fd_silver.flights GROUP BY 1 ORDER BY 1` |
| `stepfunctions.png` | Pendiente | Consola de Step Functions → `flightdelay-mx-pipeline` → ejecución `backfill-…` (vista de grafo, todo en verde) |
| `quality.png` | Pendiente | `make quality YM=2026-08` en la terminal, o el JSON en S3 |
