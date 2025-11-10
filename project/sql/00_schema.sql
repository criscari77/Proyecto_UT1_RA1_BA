-- Esquema para Caso 4: Encuestas

CREATE TABLE IF NOT EXISTS raw_encuestas(
  id_encuesta TEXT,
  fecha TEXT,
  satisfaccion_general TEXT,
  servicio_usado TEXT,
  comentarios TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);

CREATE TABLE IF NOT EXISTS clean_encuestas(
  id_encuesta TEXT PRIMARY KEY, -- Clave natural
  fecha TEXT, -- ISO Date
  satisfaccion_general INTEGER, -- 1-10
  servicio_usado TEXT,
  comentarios TEXT,
  _ingest_ts TEXT
);

-- CORRECCIÓN: Hacemos que la tabla 'quarantine' tenga
-- la misma estructura que 'raw' para guardar las filas malas.
CREATE TABLE IF NOT EXISTS quarantine_encuestas(
  id_encuesta TEXT,
  fecha TEXT,
  satisfaccion_general TEXT,
  servicio_usado TEXT,
  comentarios TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);