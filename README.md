# Proyecto_UT1_RA1_BA · Solución de ingestión, almacenamiento y reporte (UT1 · RA1)

Este repositorio contiene:
- **project/**: código reproducible (ingesta → clean → oro → reporte Markdown).
- **site/**: web pública con **Quartz 4** (GitHub Pages). El reporte UT1 se publica en `site/content/reportes/`.

## Ejecución rápida
```bash
# 1) Dependencias (elige uno)
python -m venv .venv
.venv\Scripts\activate  # (o source .venv/bin/activate)
pip install -r project/requirements.txt
# o: conda env create -f project/environment.yml && conda activate ut1

# 2) (Opcional) Generar datos de ejemplo
python project/ingest/get_data.py

# 3) Pipeline fin-a-fin (ingesta→clean→oro→reporte.md)
python project/ingest/run.py

# 4) Copiar el reporte a la web Quartz
python project/tools/copy_report_to_site.py

# 5) (Opcional) Previsualizar la web en local
cd site
npx quartz build --serve   # abre http://localhost:8080
```

## Publicación web (GitHub Pages)
- En **Settings → Pages**, selecciona **Source = GitHub Actions**.
- El workflow `./.github/workflows/deploy-pages.yml` compila `site/` y despliega.

## Flujo de datos
Bronce (`raw`) → Plata (`clean`) → Oro (`analytics`).  
Idempotencia por `batch_id` (batch) o `event_id` (stream).  
Deduplicación “último gana” por `_ingest_ts`.  
Reporte Markdown: `project/output/reporte.md` → `site/content/reportes/reporte-UT1.md`.
# BDA_Proyecto_UT1_RA1


# Proyecto UT1/RA1: Pipeline de Datos (Caso 4: Encuestas)

Este repositorio contiene un pipeline ETL de Python para el **Caso 4: Encuestas Mensuales**.

El pipeline lee ficheros Excel (`.xlsx`) de una carpeta `drops/`, los limpia, valida y transforma. Los datos se cargan en una base de datos **SQLite** (aplicando una lógica `UPSERT` para idempotencia) y en un fichero **Parquet**. Finalmente, genera un reporte (`reporte.md`) con los KPIs resultantes.

## 💻 Requisitos del Sistema

Para ejecutar este proyecto, necesitarás tener instalado el siguiente software:

* **Git**: Para clonar el repositorio.
* **Python 3.11+**: Es fundamental.
    * *Nota para Windows:* Durante la instalación de Python, asegúrate de marcar la casilla "Add Python to PATH".

---

## 🚀 Guía de Instalación y Configuración

Sigue estos pasos en tu terminal para configurar el proyecto.

### 1. Clonar el Repositorio

```bash
# Clona el proyecto en tu máquina local
git clone https://github.com/criscari77/BDA_Proyecto_UT1_RA1.git
# Entra en la carpeta del proyecto
cd BDA_Proyecto_UT1_RA1

2. Configurar el Entorno Virtual (Venv)
Es crucial aislar las dependencias del proyecto. Usaremos py -m venv para asegurar que usamos un Python válido.

Bash

# 1. Crear el entorno virtual (crea una carpeta .venv)
py -m venv .venv

# 2. Activar el entorno virtual
# En Windows (PowerShell):
.\.venv\Scripts\Activate.ps1

# (Si PowerShell da un error de 'ExecutionPolicy', ejecuta esto primero
# y luego repite el comando 'Activate.ps1'):
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# En macOS/Linux:
# source .venv/bin/activate
Tu terminal debería ahora mostrar (.venv) al principio de la línea.

3. Instalar las Dependencias
Con el entorno activado, instala las librerías de Python necesarias.

Bash

# 1. Instala las librerías del proyecto
pip install -r requirements.txt

# 2. Instala la librería para leer Excel
pip install openpyxl
(Nota: openpyxl es requerido por Pandas para manejar ficheros .xlsx).

⚙️ Ejecución del Pipeline
Una vez instalado, sigue estos pasos para ejecutar el programa.

1. Preparar los Datos de Entrada
El pipeline necesita ficheros Excel para procesar.

Copia tus ficheros de encuestas (ej. encuestas_202410.xlsx, encuestas_202411.xlsx) en la siguiente carpeta: project/data/drops/

2. Ejecutar el Pipeline
Asegúrate de que tu entorno virtual (.venv) sigue activo. Ejecuta el script principal desde la carpeta raíz del proyecto:

Bash

python project/ingest/run.py
3. Verificar los Resultados
El script imprimirá su progreso en la consola. Si todo va bien, terminará con === PIPELINE FINALIZADO ===.

Puedes encontrar todos los artefactos generados en la carpeta project/output/:

reporte.md: El informe final en Markdown con los KPIs y tablas.

ut1.db: La base de datos SQLite que contiene las tablas raw_encuestas, clean_encuestas, quarantine_encuestas y la vista v_evolucion_mensual.

parquet/clean_encuestas.parquet: Los datos limpios en formato Parquet, listos para análisis.

quality/encuestas_invalidas.csv: Un fichero CSV con las filas que fallaron la validación (cuarentena).