# Reporte UT1 · Encuestas (Caso 4)
**Periodo:** 2024-10-01 a 2024-11-03 · **Fuente:** clean_encuestas (Parquet) · **Generado:** 2025-11-10T12:10:00.944962+00:00

## 1. KPIs Principales
- **Total Encuestas Válidas:** 7
- **Satisfacción General Media (1-10):** 6.80

## 2. Distribución de la Satisfacción General
|   Puntuación (1-10) |   Cantidad |   Porcentaje (%) |
|--------------------:|-----------:|-----------------:|
|                   4 |          1 |             14.3 |
|                   6 |          1 |             14.3 |
|                   7 |          1 |             14.3 |
|                   8 |          1 |             14.3 |
|                   9 |          1 |             14.3 |

## 3. Evolución Mensual
| mes_encuesta   |   Total_Encuestas |   Satisfaccion_Media |
|:---------------|------------------:|---------------------:|
| 2024-10        |                 3 |                 8.5  |
| 2024-11        |                 4 |                 5.67 |

## 4. Calidad y Cobertura
- Filas bronce (raw): 9 · Plata (clean): 7 · Cuarentena: 1

## 5. Persistencia
- Parquet: D:\ProyectoBeta\BDA_Proyecto_UT1_RA1\project\output\parquet\clean_encuestas.parquet
- SQLite : D:\ProyectoBeta\BDA_Proyecto_UT1_RA1\project\output\ut1.db (tablas: raw_encuestas, clean_encuestas, quarantine_encuestas; vista: v_evolucion_mensual)

## 6. Contexto y Definiciones
- **Satisfacción General Media:** Promedio de la columna 'satisfaccion_general' excluyendo valores nulos (como 'NS/NC') y valores fuera del rango 1-10.
- **Fuente:** Ficheros Excel procesados desde `project/data/drops/`.
- **Limpieza:** Se eliminaron duplicados por `id_encuesta` (política: 'último gana' por `_ingest_ts`).
