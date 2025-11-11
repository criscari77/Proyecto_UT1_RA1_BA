# --- Importación de Librerías ---
import pandas as pd             # Para manipular datos (DataFrames)
import sqlite3                  # Para interactuar con la base de datos SQLite
import os                       # Funciones del sistema operativo (no se usa activamente aquí, pero 'pathlib' sí)
import re                       # Para expresiones regulares (limpieza de texto)
import unicodedata              # Para normalizar texto (ej. quitar tildes)
from pathlib import Path        # Para manejar rutas de archivos de forma moderna
from datetime import datetime, timezone # Para manejar fechas y horas con zona horaria

# --- 0. Configuración de Rutas (igual que el profesor) ---
# Define la raíz del proyecto (la carpeta 'project/')
ROOT = Path(__file__).resolve().parents[1]
# Define la carpeta de ingesta (donde dejas los Excel)
DATA = ROOT / "data" / "drops"
# Define la carpeta de salida (donde va todo lo generado)
OUT = ROOT / "output"
# Define la carpeta donde están los ficheros SQL
SQL_DIR = ROOT / "sql"

# Crea las carpetas de salida si no existen (¡importante!)
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "parquet").mkdir(parents=True, exist_ok=True)
(OUT / "quality").mkdir(parents=True, exist_ok=True)

# Define la ruta al fichero de la base de datos
DB = OUT / "ut1.db"

# --- Definiciones Específicas del Caso 4 ---
# Define las columnas que esperamos encontrar.
# Esto es CLAVE para el Caso 4, para manejar Excel a los que les falten columnas.
EXPECTED_COLUMNS = [
    'id_encuesta', 'fecha', 'satisfaccion_general', 
    'servicio_usado', 'comentarios'
]

# --- Funciones de Ayuda (Limpieza) ---
def normalize_text(text):
    """Limpia texto: quita tildes, pasa a minúsculas y quita espacios extra."""
    if text is None or not isinstance(text, str):
        return None
    # Normaliza para separar tildes (ej. "é" -> "e" + "´")
    text = unicodedata.normalize('NFD', text)
    # Elimina los caracteres de tilde
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    # Pasa a minúsculas y quita espacios al inicio/final
    text = text.lower().strip()
    # Reemplaza múltiples espacios por uno solo
    text = re.sub(r'\s+', ' ', text)
    return text

print("--- (0) Pipeline Caso 4 (Encuestas) Iniciado ---")

# --- 1) Ingesta ---
# Busca todos los ficheros que terminen en .xlsx en la carpeta DATA
files = sorted(DATA.glob("*.xlsx"))
raw = [] # Una lista para guardar los datos de cada fichero

if not files:
    # Si no se encuentran ficheros, informa y crea un DataFrame vacío
    # para que el resto del script no falle.
    print(f"No se encontraron ficheros .xlsx en: {DATA}")
    cols = EXPECTED_COLUMNS + ["_source_file", "_ingest_ts"]
    raw_df = pd.DataFrame(columns=cols)
else:
    # Si hay ficheros, itera sobre cada uno
    for f in files:
        try:
            # Lee el fichero Excel. dtype=str lee todo como texto para evitar errores
            df = pd.read_excel(f, engine='openpyxl', dtype=str)
            
            # --- TRAZABILIDAD ---
            # Añade el nombre del fichero a los datos
            df["_source_file"] = f.name
            # Añade la fecha y hora de ingesta (con zona horaria UTC)
            df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
            
            # --- LÓGICA CLAVE (Caso 4) ---
            # Reordena el DataFrame usando las columnas esperadas.
            # Si falta una columna (ej. 'comentarios'), la añade con valores NaN.
            df = df.reindex(columns=EXPECTED_COLUMNS + ['_source_file', '_ingest_ts'])
            
            raw.append(df) # Añade el DataFrame del fichero a la lista
            print(f"  > Leído: {f.name} ({len(df)} filas)")
        except Exception as e:
            print(f"Error leyendo {f.name}: {e}")

    if raw:
        # Si la lista 'raw' tiene datos, concatena todos los DataFrames en uno solo
        raw_df = pd.concat(raw, ignore_index=True)
    else:
        # Si hubo errores leyendo, crea un DataFrame vacío
        print("No se pudieron cargar datos.")
        cols = EXPECTED_COLUMNS + ["_source_file", "_ingest_ts"]
        raw_df = pd.DataFrame(columns=cols)

# Imprime un resumen de la ingesta
print(f"--- (1) Ingesta completada: {len(raw_df)} filas 'raw' ---")

# --- 2) Limpieza (coerción + validación + dedupe) ---
# Crea una copia del DataFrame 'raw' para empezar a limpiar
df = raw_df.copy()

# Aplica la función de limpieza de texto a las columnas de texto
df['servicio_usado'] = df['servicio_usado'].apply(normalize_text)
df['comentarios'] = df['comentarios'].apply(normalize_text)

# Define los textos que significan "Nulo"
nulos_map = ['NS/NC', 'No contesta', 'no sabe']
# Reemplaza esos textos por el valor especial pd.NA (Nulo de Pandas)
df['satisfaccion_general'] = df['satisfaccion_general'].replace(nulos_map, pd.NA)

# --- Coerción de Tipos ---
# Convierte la columna 'fecha' a tipo fecha. errors='coerce' convierte fechas inválidas en NaT (Nulo)
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
# Convierte 'satisfaccion_general' a número. errors='coerce' convierte textos en NaN
df['satisfaccion_general'] = pd.to_numeric(df['satisfaccion_general'], errors="coerce")

# --- Validación (Reglas de Calidad) ---
# Define una "máscara" booleana (True/False) para cada fila
valid = (
    df["fecha"].notna()  # La fecha no puede ser nula
    & df["id_encuesta"].notna() & (df["id_encuesta"] != "") # El ID no puede ser nulo
    # La satisfacción es un número entre 1 y 10, O es nula (que es válido, ej. "NS/NC")
    & (df['satisfaccion_general'].between(1, 10) | df['satisfaccion_general'].isna())
)

# --- Separación ---
# Se queda con las filas que NO (~) son válidas
quarantine = df.loc[~valid].copy()
# Se queda con las filas que SÍ son válidas
clean = df.loc[valid].copy()

# --- Deduplicación ---
if not clean.empty: # Solo si hay datos limpios...
    clean = (clean.sort_values("_ingest_ts") # Ordena por fecha de ingesta (de más viejo a más nuevo)
                  # Elimina duplicados por 'id_encuesta', quedándose con el último (keep="last")
                  .drop_duplicates(subset=["id_encuesta"], keep="last"))
    
    # Convierte la satisfacción a 'Int64' (Entero que soporta Nulos)
    clean["satisfaccion_general"] = clean["satisfaccion_general"].astype('Int64')

print(f"--- (2) Limpieza completada: {len(clean)} filas 'clean', {len(quarantine)} filas 'quarantine' ---")

# --- 3) Persistencia: Parquet (fuente de reporte) + SQLite (opcional integrado) ---

# Guarda el CSV de cuarentena
QUARANTINE_FILE = OUT / "quality" / "encuestas_invalidas.csv"
quarantine.to_csv(QUARANTINE_FILE, index=False)

# Guarda el Parquet (la "fuente de verdad" para el reporte)
PARQUET_FILE = OUT / "parquet" / "clean_encuestas.parquet"
if not clean.empty:
    # Selecciona solo las columnas de negocio (sin las de trazabilidad)
    clean_cols_parquet = ['id_encuesta', 'fecha', 'satisfaccion_general', 'servicio_usado', 'comentarios']
    clean[clean_cols_parquet].to_parquet(PARQUET_FILE, index=False)

# 3.3) SQLite (Bronce y Plata)
# Conecta con el fichero de la base de datos (lo crea si no existe)
con = sqlite3.connect(DB)

# DDL (Ejecuta el schema)
# Lee el fichero 00_schema.sql y lo ejecuta para crear las tablas
schema_sql = (SQL_DIR / "00_schema.sql").read_text(encoding="utf-8")
con.executescript(schema_sql)

# RAW (Bronce)
# Carga todas las filas 'raw' en la tabla 'raw_encuestas'
if not raw_df.empty:
    df_raw_sql = raw_df[EXPECTED_COLUMNS + ['_ingest_ts', '_source_file']].copy()
    df_raw_sql["_batch_id"] = "demo" # Añade el ID de lote
    # if_exists="replace" borra la tabla y la vuelve a crear (idempotencia)
    df_raw_sql.to_sql("raw_encuestas", con, if_exists="replace", index=False)

# (AÑADIDO) QUARANTINE (Guardamos las filas malas en su tabla)
if not quarantine.empty:
    quarantine_sql_cols = [col for col in EXPECTED_COLUMNS + ['_ingest_ts', '_source_file'] if col in quarantine.columns]
    quarantine_sql = quarantine[quarantine_sql_cols].copy()
    quarantine_sql["_batch_id"] = "demo"
    # Borra y recarga la tabla de cuarentena
    quarantine_sql.to_sql("quarantine_encuestas", con, if_exists="replace", index=False)

# CLEAN (Plata) - CON UPSERT (la lógica del profesor)
if not clean.empty:
    # Lee el fichero SQL que define la lógica del UPSERT
    upsert_sql = (SQL_DIR / "10_upserts.sql").read_text(encoding="utf-8")
    
    clean_cols_sql = ['id_encuesta', 'fecha', 'satisfaccion_general', 'servicio_usado', 'comentarios', '_ingest_ts']
    
    # Itera fila por fila sobre los datos limpios
    for _, r in clean[clean_cols_sql].iterrows():
        # Prepara los parámetros para la consulta SQL
        params = {
            "id_encuesta": r["id_encuesta"],
            "fecha": str(r["fecha"]),
            # --- CORRECCIÓN BUG 'NAType' ---
            # Convierte el pd.NA (Nulo de Pandas) a None (Nulo de Python),
            # porque la librería sqlite3 no entiende pd.NA
            "satisfaccion": None if pd.isna(r["satisfaccion_general"]) else r["satisfaccion_general"],
            "servicio": r["servicio_usado"],
            "comentarios": r["comentarios"],
            "ts": r["_ingest_ts"]
        }
        # Ejecuta el UPSERT (INSERT ON CONFLICT...) para esta fila
        con.execute(upsert_sql, params)

con.commit() # Confirma todos los cambios (UPSERTs) en la BBDD

# Vistas (Oro) - AÑADIDO
try:
    # Lee y ejecuta el fichero 20_views.sql para crear la vista
    views_sql = (SQL_DIR / "20_views.sql").read_text(encoding="utf-8")
    con.executescript(views_sql)
    print("  > Vista 'v_evolucion_mensual' creada/actualizada en SQLite.")
except Exception as e:
    print(f"Error al crear vistas SQL: {e}")

con.close() # Cierra la conexión a la BBDD

print(f"--- (3) Persistencia completada ---")
print(f"    > Cuarentena: {QUARANTINE_FILE}")
print(f"    > Parquet: {PARQUET_FILE}")
print(f"    > SQLite: {DB} (tablas: raw_encuestas, clean_encuestas, quarantine_encuestas; vista: v_evolucion_mensual)")

# --- 4) Reporte releído desde PARQUET ---
# El reporte se genera desde Parquet, la "fuente de verdad" analítica.
if PARQUET_FILE.exists():
    clean_rep = pd.read_parquet(PARQUET_FILE)
    clean_rep['fecha'] = pd.to_datetime(clean_rep['fecha']) # Convierte a fecha para agrupar
else:
    # Si no hay Parquet, crea un DataFrame vacío
    clean_rep = pd.DataFrame(columns=['id_encuesta', 'fecha', 'satisfaccion_general', 'servicio_usado', 'comentarios'])

# --- Cálculo de KPIs y Tablas para el Reporte ---
if not clean_rep.empty:
    # KPI 1
    kpi_total_encuestas = len(clean_rep)
    # KPI 2
    kpi_satisfaccion_media = clean_rep['satisfaccion_general'].mean()
    # Prepara la columna 'mes' para agrupar
    clean_rep['mes_encuesta'] = clean_rep['fecha'].dt.to_period('M')

    # Tabla 1: Distribución
    distribucion = clean_rep['satisfaccion_general'].value_counts().sort_index().reset_index()
    distribucion.columns = ['Puntuación (1-10)', 'Cantidad']
    distribucion['Porcentaje (%)'] = (distribucion['Cantidad'] / kpi_total_encuestas * 100).round(1)

    # Tabla 2: Evolución
    evolucion = clean_rep.groupby('mes_encuesta').agg(
        Total_Encuestas=('id_encuesta', 'count'),
        Satisfaccion_Media=('satisfaccion_general', 'mean')
    ).reset_index()
    evolucion['Satisfaccion_Media'] = evolucion['Satisfaccion_Media'].round(2)
    
    # Fechas para el título del reporte
    periodo_ini = str(clean_rep["fecha"].min().date())
    periodo_fin = str(clean_rep["fecha"].max().date())
else:
    # Valores por defecto si no hay datos limpios
    kpi_total_encuestas = 0
    kpi_satisfaccion_media = 0.0
    distribucion = pd.DataFrame(columns=['Puntuación (1-10)', 'Cantidad', 'Porcentaje (%)'])
    evolucion = pd.DataFrame(columns=['mes_encuesta', 'Total_Encuestas', 'Satisfaccion_Media'])
    periodo_ini = "—"; periodo_fin = "—"

# --- Creación del Reporte (String multi-línea) ---
# Se usa un f-string (f"...") para incrustar las variables en el texto
report = (
    f"# Reporte UT1 · Encuestas (Caso 4)\n"
    f"**Periodo:** {periodo_ini} a {periodo_fin} · **Fuente:** clean_encuestas (Parquet) · **Generado:** {datetime.now(timezone.utc).isoformat()}\n\n"
    "## 1. KPIs Principales\n"
    f"- **Total Encuestas Válidas:** {kpi_total_encuestas}\n"
    f"- **Satisfacción General Media (1-10):** {kpi_satisfaccion_media:.2f}\n\n"
    "## 2. Distribución de la Satisfacción General\n"
    # Convierte el DataFrame 'distribucion' a texto Markdown
    f"{(distribucion.to_markdown(index=False) if not distribucion.empty else '_(sin datos)_')}\n\n"
    "## 3. Evolución Mensual\n"
    # Convierte el DataFrame 'evolucion' a texto Markdown
    f"{(evolucion.to_markdown(index=False) if not evolucion.empty else '_(sin datos)_')}\n\n"
    "## 4. Calidad y Cobertura\n"
    f"- Filas bronce (raw): {len(raw_df)} · Plata (clean): {len(clean)} · Cuarentena: {len(quarantine)}\n\n"
    "## 5. Persistencia\n"
    f"- Parquet: {PARQUET_FILE}\n"
    f"- SQLite : {DB} (tablas: raw_encuestas, clean_encuestas, quarantine_encuestas; vista: v_evolucion_mensual)\n\n"
    "## 6. Contexto y Definiciones\n"
    "- **Satisfacción General Media:** Promedio de la columna 'satisfaccion_general' excluyendo valores nulos (como 'NS/NC') y valores fuera del rango 1-10.\n"
    "- **Fuente:** Ficheros Excel procesados desde `project/data/drops/`.\n"
    "- **Limpieza:** Se eliminaron duplicados por `id_encuesta` (política: 'último gana' por `_ingest_ts`).\n"
)

# --- Guardado del Reporte ---
REPORT_FILE = OUT / "reporte.md"
# Escribe el string 'report' en el fichero 'reporte.md'
REPORT_FILE.write_text(report, encoding="utf-8")

# --- Mensajes Finales ---
print(f"--- (4) Reporte completado ---")
print(f"OK · Reporte  :", REPORT_FILE)
print(f"OK · Parquet  :", PARQUET_FILE if PARQUET_FILE.exists() else "sin datos")
print(f"OK · SQLite   :", DB)
print("=== PIPELINE FINALIZADO ===")