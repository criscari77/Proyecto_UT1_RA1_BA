---
title: "Definición de métricas y tablas oro (Caso 4: Encuestas)"
owner: "equipo-alumno"
periodicidad: "batch (según ejecución)"
version: "1.0.0"
---

# Modelo de negocio (Capa Oro)

## Tablas Oro
- **`clean_encuestas`** (Tabla Plata/Oro): Persistida en Parquet (fuente del reporte) y SQLite (fuente de la vista). Granularidad: una fila por `id_encuesta`.
- **`v_evolucion_mensual`** (Vista Oro): Creada en SQLite (`20_views.sql`). Granularidad: una fila por mes.

## Métricas (KPI)
- **Total Encuestas Válidas**: `COUNT(id_encuesta)` desde `clean_encuestas`.
- **Satisfacción General Media**: `AVG(satisfaccion_general)` desde `clean_encuestas` (excluye `NULL`s).

## Tablas Agregadas (Para el Reporte)
- **Distribución de Satisfacción**: `GROUP BY satisfaccion_general`
- **Evolución Mensual**: `GROUP BY mes_encuesta` (Implementada como la vista `v_evolucion_mensual`).

## Consultas base (SQL conceptual)
```sql
-- Vista de Evolución Mensual (como en 20_views.sql)
CREATE VIEW IF NOT EXISTS v_evolucion_mensual AS
SELECT
  strftime('%Y-%m', fecha) AS mes,
  AVG(satisfaccion_general) AS satisfaccion_media,
  COUNT(id_encuesta) AS total_encuestas
FROM
  clean_encuestas
GROUP BY
  mes
ORDER BY
  mes;