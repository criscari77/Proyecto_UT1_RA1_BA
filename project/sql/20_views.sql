-- Vista para Caso 4: Evolución mensual de encuestas

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