# Diseño de ingestión (Caso 4: Encuestas)

## Resumen
Se ingieren ficheros Excel (batch) con encuestas mensuales. Los datos se cargan, se validan, se limpian y se aplica una política de "último gana" (UPSERT) en la capa `clean` (SQLite) antes de generar el Parquet.

## Fuente
- **Origen:** `project/data/drops/*.xlsx` (Múltiples ficheros Excel).
- **Formato:** Excel (`.xlsx`).
- **Frecuencia:** Batch (una ejecución completa del script `run.py`).

## Estrategia
- **Modo:** `batch`.
- **Incremental:** No es incremental. Las tablas `raw_` y `quarantine_` se recargan (modo "replace"). La tabla `clean_` aplica un UPSERT para idempotencia.
- **Particionado:** No se aplica particionado en disco (Parquet) por simplicidad, aunque se podría añadir por fecha a futuro.

## Idempotencia y deduplicación
- **batch_id:** Se usa un valor estático "demo" para `_batch_id`.
- **clave natural:** `(id_encuesta)`.
- **Política:** **"Último gana por `_ingest_ts`"**. Implementado a nivel de SQL (`10_upserts.sql`) con un `ON CONFLICT(...) DO UPDATE SET... WHERE excluded._ingest_ts > c._ingest_ts`.

## Checkpoints y trazabilidad
- **checkpoints/offset:** No aplica (es batch).
- **trazabilidad:** `_ingest_ts`, `_source_file`, `_batch_id`.
- **DLQ/quarantine:**
    - Fichero: `project/output/quality/encuestas_invalidas.csv`
    - Tabla: `quarantine_encuestas` (en `ut1.db`)

## Riesgos / Antipatrones
- **Columnas opcionales:** El Caso 4 requería manejar ficheros Excel donde a veces faltan columnas. Se soluciona usando `df.reindex(columns=EXPECTED_COLUMNS...)` en `run.py`.