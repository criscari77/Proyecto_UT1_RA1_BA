---
title: "Documento del Proyecto - Caso 4: Encuestas Mensuales"
tags: ["UT1","RA1","docs", "Caso4", "Encuestas"]
version: "1.0.0"
owner: "Cristobal Soto Sanche"
status: "published"
---

# 1. Objetivo
El propósito de este documento es describir el pipeline ETL implementado para el **Caso 4: Encuestas Mensuales**. El objetivo es procesar ficheros Excel "sucios" (con columnas faltantes, dominios inválidos y valores nulos textuales), limpiarlos y persistirlos en un formato analítico (Parquet) y en una base de datos relacional (SQLite).

# 2. Alcance
**El pipeline cubre:**
* Ingesta de múltiples ficheros `.xlsx` desde `project/data/drops/`.
* Limpieza y estandarización de texto (tildes, espacios, mayúsculas).
* Mapeo de valores nulos textuales (ej. "NS/NC") a `NULL`.
* Validación de dominios (rango 1-10 para `satisfaccion_general`).
* Envío de filas inválidas a una tabla y fichero de cuarentena.
* Deduplicación de encuestas por `id_encuesta` con política "último gana".
* Persistencia en capas: `raw_encuestas`, `clean_encuestas` y `quarantine_encuestas` en SQLite.
* Persistencia de la capa limpia (`clean`) en Parquet.
* Creación de una vista analítica (`v_evolucion_mensual`) en SQLite.
* Generación de un reporte final en Markdown (`reporte.md`).

**El pipeline NO cubre:**
* Procesamiento en tiempo real (es un pipeline batch).
* Generación de visualizaciones gráficas.
* Carga de datos desde una API.

# 3. Decisiones / Reglas
-   **Estrategia de ingestión:** **Batch**. El script `run.py` procesa todos los ficheros `.xlsx` encontrados en `project/data/drops/` en cada ejecución.
-   **Clave natural:** `id_encuesta`. Se asume que es el identificador único para una respuesta de encuesta.
-   **Idempotencia:**
    * Tablas `raw_` y `quarantine_`: Se usa `if_exists="replace"` para recargar los datos en cada ejecución (estrategia simple de refresco total).
    * Tabla `clean_encuestas`: Se implementa una estrategia **UPSERT** (usando `10_upserts.sql`) basada en la `id_encuesta`.
-   **Política de Deduplicación:** **"Último gana"**. El `UPSERT` solo actualiza una fila existente si el `_ingest_ts` de la fila nueva es más reciente que el de la fila antigua.
-   **Validaciones de calidad (Quarantine):**
    * `id_encuesta` y `fecha` no pueden ser nulos.
    * `satisfaccion_general` debe estar en el rango `[1, 10]` (o ser `NULL`). Filas con "11" o valores fuera de rango se envían a cuarentena.
-   **Limpieza (Transformación):**
    * `servicio_usado` y `comentarios`: Se normalizan (minúsculas, sin tildes, sin espacios extra).
    * `satisfaccion_general`: Valores como "NS/NC" o "No contesta" se mapean a `NULL` (usando `pd.NA` en Pandas, que se convierte a `None` antes de la carga SQL) para no afectar los cálculos de la media.

# 4. Procedimiento / Pasos
1.  **Crear Entorno:** `python -m venv .venv`
2.  **Activar Entorno (PowerShell):** `.\.venv\Scripts\Activate.ps1`
    * (Si falla por permisos: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`)
3.  **Instalar Dependencias:** `pip install -r requirements.txt` (asegurándose de que `openpyxl` esté incluido).
4.  **Colocar Datos:** Añadir los ficheros `encuestas_*.xlsx` en la carpeta `project/data/drops/`.
5.  **Ejecutar Pipeline:** Desde la carpeta raíz del proyecto (`BDA_Proyecto_UT1_RA1`):
    ```bash
    python project/ingest/run.py
    ```

# 5. Evidencias
Tras la ejecución, se generan los siguientes artefactos:
* **Reporte:** `project/output/reporte.md` (con KPIs y tablas).
* **Parquet Limpio:** `project/output/parquet/clean_encuestas.parquet`
* **CSV Cuarentena:** `project/output/quality/encuestas_invalidas.csv`
* **Base de Datos:** `project/output/ut1.db`, la cual contiene:
    * Tabla `raw_encuestas` (9 filas).
    * Tabla `clean_encuestas` (7 filas).
    * Tabla `quarantine_encuestas` (1 fila).
    * Vista `v_evolucion_mensual`.

**Ejemplo de fila en Cuarentena (CSV):**
`id_encuesta,fecha,...,satisfaccion_general,...,_source_file`
`1003,2024-10-02,...,11,...,encuestas_202410.xlsx`

# 6. Resultados
* **KPIs:**
    * Total Encuestas Válidas: **7**
    * Satisfacción General Media: **6.80** (sobre 10)
* **Hallazgos:**
    * Se procesaron 9 filas en total. 1 fila fue a cuarentena (valor '11') y 1 fila se descartó como duplicada (la más antigua de `id_encuesta` 1002).
    * Se detectó un descenso en la satisfacción media de 8.50 (Octubre 2024) a 5.67 (Noviembre 2024).

# 7. Lecciones aprendidas
* **`pd.NA` vs `None`:** La librería `sqlite3` de Python no soporta el tipo `NAType` de Pandas. Fue necesario convertir explícitamente `pd.NA` a `None` antes de ejecutar el `UPSERT` en SQL para evitar el error `type 'NAType' is not supported`.
* **Poblar todas las capas:** Un pipeline completo debe escribir en todas las tablas de persistencia. Se corrigió el script para asegurar que las filas inválidas se guardaban tanto en el `.csv` de cuarentena como en la tabla `quarantine_encuestas` de SQLite.
* **PowerShell Policy:** La política de ejecución de PowerShell (`ExecutionPolicy`) es un paso de configuración inicial común en Windows para poder activar entornos virtuales.
* **Adaptación de Plantillas:** El ejercicio demostró la importancia de adaptar la lógica SQL (como `UPSERT` y `Vistas`) a las claves y necesidades del nuevo esquema de datos (Encuestas vs. Ventas).

# 8. Próximos pasos
* **Publicación:** Publicar la documentación y el reporte en `Quartz` / GitHub Pages (Bono opcional).
* **Análisis de Cuarentena:** Revisar las filas en `quarantine_encuestas` para identificar la causa raíz y solicitar correcciones en el origen de datos.
* **Monitorización:** Ejecutar el pipeline mensualmente para monitorizar la evolución de la `satisfaccion_media` (usando la vista `v_evolucion_mensual`).