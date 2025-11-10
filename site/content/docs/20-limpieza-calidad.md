# Reglas de limpieza y calidad (Caso 4: Encuestas)

## Tipos y formatos
- `id_encuesta`: TEXT. Obligatorio, no nulo.
- `fecha`: ISO (`YYYY-MM-DD`). Obligatorio.
- `satisfaccion_general`: Entero (o Nulo).

## Nulos
- **Campos obligatorios:** `id_encuesta`, `fecha`. Si faltan, la fila va a **quarantine**.
- **Mapeo de Nulos:** Los valores de texto `NS/NC`, `No contesta` o `no sabe` en `satisfaccion_general` se convierten a `NULL` (nulo) para ser excluidos de los cálculos de media.

## Rangos y dominios
- `satisfaccion_general`: Debe estar en el rango `1` a `10` (o ser `NULL`). Valores fuera de este rango (ej. "11") se envían a **quarantine**.

## Deduplicación
- **Clave natural:** `(id_encuesta)`
- **Política:** **"Último gana"** basado en `_ingest_ts`. Implementada vía `UPSERT` en `10_upserts.sql`.

## Estandarización de texto
- **Campos:** `servicio_usado`, `comentarios`.
- **Reglas:** Se aplica `trim()` (quitar espacios), normalización de tildes (ej. "Atención" -> "Atencion") y conversión a minúsculas.