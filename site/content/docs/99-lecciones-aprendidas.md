# Lecciones aprendidas

## Qué salió bien
- Se adaptó con éxito la plantilla del Caso 1 (Ventas) al Caso 4 (Encuestas).
- El pipeline maneja correctamente los requisitos del Caso 4: lee Excel, maneja columnas opcionales, limpia textos ("NS/NC") y valida dominios (rango 1-10).
- Se implementó correctamente la idempotencia usando la estrategia `UPSERT` de SQL (via `10_upserts.sql`).
- Se pobló correctamente la capa de Oro con una Vista SQL (`20_views.sql`).
- Se generó un reporte en Parquet y se publicó una web local con Quartz.

## Qué mejorar (Problemas resueltos)
- **Gestión de Entornos (venv):** Varios problemas surgieron por una mala configuración del entorno.
    - **`ModuleNotFoundError`:** Se solucionó ejecutando `pip install -r requirements.txt` dentro del `.venv` activado.
    - **`Missing openpyxl`:** Se solucionó añadiendo `openpyxl` a `requirements.txt` (o instalándolo manualmente).
    - **`venv` Roto:** Un `venv` antiguo apuntaba a un Python (`D:\Python`) que ya no existía. Se solucionó **borrando la carpeta `.venv`** y creándola de nuevo con `py -m venv .venv`.

- **Errores de Scripting (`run.py`):**
    - **`NAType not supported`:** Error crítico. La librería `sqlite3` no entiende el `pd.NA` de Pandas. Se solucionó convirtiéndolo a `None` de Python (`None if pd.isna(x) else x`) justo antes del `con.execute()`.
    - **`quarantine_encuestas` vacía:** El script original solo guardaba la cuarentena en un `.csv`. Se modificó `run.py` para añadir el bloque `quarantine_sql.to_sql(...)` y poblar también la tabla en SQLite.
    - **`SyntaxError: '(' was never closed`:** Un error de sintaxis en el string multi-línea del `reporte.md`. Se corrigió la sintaxis del string.

- **Configuración de Servidor (Quartz):**
    - **Puerto `8080` ocupado:** El puerto estaba en uso por el Listener de Oracle (`TNSLSNR.EXE`). Se solucionó usando otro puerto (`--port 8081`).
    - **Bucle Infinito de Quartz:** El servidor (`--serve`) se reiniciaba sin parar. La causa era la sincronización de **OneDrive** interfiriendo con el "vigilante" de Quartz.
    - **Solución al bucle:** Se implementó una solución de 2 pasos:
        1. Compilar la web una sola vez (sin vigilar): `npx quartz build`.
        2. Servir los archivos estáticos con un servidor de Python: `python -m http.server 8081 --directory .\site\public`.