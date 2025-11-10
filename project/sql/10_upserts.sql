-- UPSERT para Caso 4: Encuestas
-- Inserta una fila; si la clave (id_encuesta) ya existe,
-- actualiza la fila SÓLO SI la nueva fila es más reciente (_ingest_ts)

INSERT INTO clean_encuestas AS c (
  id_encuesta,
  fecha,
  satisfaccion_general,
  servicio_usado,
  comentarios,
  _ingest_ts
)
VALUES (
  :id_encuesta,
  :fecha,
  :satisfaccion,
  :servicio,
  :comentarios,
  :ts
)
ON CONFLICT(id_encuesta) DO UPDATE SET
  fecha = excluded.fecha,
  satisfaccion_general = excluded.satisfaccion_general,
  servicio_usado = excluded.servicio_usado,
  comentarios = excluded.comentarios,
  _ingest_ts = excluded._ingest_ts
WHERE
  excluded._ingest_ts > c._ingest_ts;