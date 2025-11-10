import pandas as pd
import sqlite3
import os
import re
import unicodedata
from pathlib import Path
from datetime import datetime, timezone

# --- 0. Configuración de Rutas (igual que el profesor) ---
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "drops"
OUT = ROOT / "output"
SQL_DIR = ROOT / "sql"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "parquet").mkdir(parents=True, exist_ok=True)
(OUT / "quality").mkdir(parents=True, exist_ok=True)

DB = OUT / "ut1.db" # Usamos la misma DB

# --- Definiciones Específicas del Caso 4 ---
EXPECTED_COLUMNS = [
    'id_encuesta', 'fecha', 'satisfaccion_general', 
    'servicio_usado', 'comentarios'
]

# --- Funciones de Ayuda (Limpieza) ---
def normalize_text(text):
    """Limpia texto: quita tildes, pasa a minúsculas y quita espacios extra."""
    if text is None or not isinstance(text, str):
        return None
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    return text

print("--- (0) Pipeline Caso 4 (Encuestas) Iniciado ---")

# --- 1) Ingesta ---
files = sorted(DATA.glob("*.xlsx"))
raw = []

if not files:
    print(f"No se encontraron ficheros .xlsx en: {DATA}")
    cols = EXPECTED_COLUMNS + ["_source_file", "_ingest_ts"]
    raw_df = pd.DataFrame(columns=cols)
else:
    for f in files:
        try:
            df = pd.read_excel(f, engine='openpyxl', dtype=str)
            df["_source_file"] = f.name
            df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
            df = df.reindex(columns=EXPECTED_COLUMNS + ['_source_file', '_ingest_ts'])
            raw.append(df)
            print(f"  > Leído: {f.name} ({len(df)} filas)")
        except Exception as e:
            print(f"Error leyendo {f.name}: {e}")

    if raw:
        raw_df = pd.concat(raw, ignore_index=True)
    else:
        print("No se pudieron cargar datos.")
        cols = EXPECTED_COLUMNS + ["_source_file", "_ingest_ts"]
        raw_df = pd.DataFrame(columns=cols)

print(f"--- (1) Ingesta completada: {len(raw_df)} filas 'raw' ---")

# --- 2) Limpieza (coerción + validación + dedupe) ---
df = raw_df.copy()
df['servicio_usado'] = df['servicio_usado'].apply(normalize_text)
df['comentarios'] = df['comentarios'].apply(normalize_text)
nulos_map = ['NS/NC', 'No contesta', 'no sabe']
df['satisfaccion_general'] = df['satisfaccion_general'].replace(nulos_map, pd.NA)
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
df['satisfaccion_general'] = pd.to_numeric(df['satisfaccion_general'], errors="coerce")

valid = (
    df["fecha"].notna()
    & df["id_encuesta"].notna() & (df["id_encuesta"] != "")
    & (df['satisfaccion_general'].between(1, 10) | df['satisfaccion_general'].isna())
)

quarantine = df.loc[~valid].copy()
clean = df.loc[valid].copy()

if not clean.empty:
    clean = (clean.sort_values("_ingest_ts")
                  .drop_duplicates(subset=["id_encuesta"], keep="last"))
    clean["satisfaccion_general"] = clean["satisfaccion_general"].astype('Int64')

print(f"--- (2) Limpieza completada: {len(clean)} filas 'clean', {len(quarantine)} filas 'quarantine' ---")

# --- 3) Persistencia: Parquet (fuente de reporte) + SQLite (opcional integrado) ---
QUARANTINE_FILE = OUT / "quality" / "encuestas_invalidas.csv"
quarantine.to_csv(QUARANTINE_FILE, index=False)

PARQUET_FILE = OUT / "parquet" / "clean_encuestas.parquet"
if not clean.empty:
    clean_cols_parquet = ['id_encuesta', 'fecha', 'satisfaccion_general', 'servicio_usado', 'comentarios']
    clean[clean_cols_parquet].to_parquet(PARQUET_FILE, index=False)

# 3.3) SQLite (Bronce y Plata)
con = sqlite3.connect(DB)

# DDL (Ejecuta el schema)
schema_sql = (SQL_DIR / "00_schema.sql").read_text(encoding="utf-8")
con.executescript(schema_sql)

# RAW (Bronce)
if not raw_df.empty:
    df_raw_sql = raw_df[EXPECTED_COLUMNS + ['_ingest_ts', '_source_file']].copy()
    df_raw_sql["_batch_id"] = "demo"
    df_raw_sql.to_sql("raw_encuestas", con, if_exists="replace", index=False)

# (AÑADIDO) QUARANTINE (Guardamos las filas malas en su tabla)
if not quarantine.empty:
    quarantine_sql_cols = [col for col in EXPECTED_COLUMNS + ['_ingest_ts', '_source_file'] if col in quarantine.columns]
    quarantine_sql = quarantine[quarantine_sql_cols].copy()
    quarantine_sql["_batch_id"] = "demo"
    quarantine_sql.to_sql("quarantine_encuestas", con, if_exists="replace", index=False)

# CLEAN (Plata) - CON UPSERT
if not clean.empty:
    upsert_sql = (SQL_DIR / "10_upserts.sql").read_text(encoding="utf-8")
    
    clean_cols_sql = ['id_encuesta', 'fecha', 'satisfaccion_general', 'servicio_usado', 'comentarios', '_ingest_ts']
    
    for _, r in clean[clean_cols_sql].iterrows():
        params = {
            "id_encuesta": r["id_encuesta"],
            "fecha": str(r["fecha"]),
            # CORRECCIÓN: Convertir pd.NA (Pandas) a None (Python) para que SQLite entienda NULL
            "satisfaccion": None if pd.isna(r["satisfaccion_general"]) else r["satisfaccion_general"],
            "servicio": r["servicio_usado"],
            "comentarios": r["comentarios"],
            "ts": r["_ingest_ts"]
        }
        con.execute(upsert_sql, params)

con.commit() # Guardamos los cambios

# Vistas (Oro) - AÑADIDO
try:
    views_sql = (SQL_DIR / "20_views.sql").read_text(encoding="utf-8")
    con.executescript(views_sql)
    print("  > Vista 'v_evolucion_mensual' creada/actualizada en SQLite.")
except Exception as e:
    print(f"Error al crear vistas SQL: {e}")

con.close() # Cerramos la conexión

print(f"--- (3) Persistencia completada ---")
print(f"    > Cuarentena: {QUARANTINE_FILE}")
print(f"    > Parquet: {PARQUET_FILE}")
print(f"    > SQLite: {DB} (tablas: raw_encuestas, clean_encuestas, quarantine_encuestas; vista: v_evolucion_mensual)")

# --- 4) Reporte releído desde PARQUET ---
if PARQUET_FILE.exists():
    clean_rep = pd.read_parquet(PARQUET_FILE)
    clean_rep['fecha'] = pd.to_datetime(clean_rep['fecha'])
else:
    clean_rep = pd.DataFrame(columns=['id_encuesta', 'fecha', 'satisfaccion_general', 'servicio_usado', 'comentarios'])

if not clean_rep.empty:
    kpi_total_encuestas = len(clean_rep)
    kpi_satisfaccion_media = clean_rep['satisfaccion_general'].mean()
    clean_rep['mes_encuesta'] = clean_rep['fecha'].dt.to_period('M')

    distribucion = clean_rep['satisfaccion_general'].value_counts().sort_index().reset_index()
    distribucion.columns = ['Puntuación (1-10)', 'Cantidad']
    distribucion['Porcentaje (%)'] = (distribucion['Cantidad'] / kpi_total_encuestas * 100).round(1)

    evolucion = clean_rep.groupby('mes_encuesta').agg(
        Total_Encuestas=('id_encuesta', 'count'),
        Satisfaccion_Media=('satisfaccion_general', 'mean')
    ).reset_index()
    evolucion['Satisfaccion_Media'] = evolucion['Satisfaccion_Media'].round(2)
    
    periodo_ini = str(clean_rep["fecha"].min().date())
    periodo_fin = str(clean_rep["fecha"].max().date())
else:
    kpi_total_encuestas = 0
    kpi_satisfaccion_media = 0.0
    distribucion = pd.DataFrame(columns=['Puntuación (1-10)', 'Cantidad', 'Porcentaje (%)'])
    evolucion = pd.DataFrame(columns=['mes_encuesta', 'Total_Encuestas', 'Satisfaccion_Media'])
    periodo_ini = "—"; periodo_fin = "—"

# --- INICIO BLOQUE CORREGIDO ---
# El error de sintaxis estaba aquí.
report = (
    f"# Reporte UT1 · Encuestas (Caso 4)\n"
    f"**Periodo:** {periodo_ini} a {periodo_fin} · **Fuente:** clean_encuestas (Parquet) · **Generado:** {datetime.now(timezone.utc).isoformat()}\n\n"
    "## 1. KPIs Principales\n"
    f"- **Total Encuestas Válidas:** {kpi_total_encuestas}\n"
    f"- **Satisfacción General Media (1-10):** {kpi_satisfaccion_media:.2f}\n\n"
    "## 2. Distribución de la Satisfacción General\n"
    f"{(distribucion.to_markdown(index=False) if not distribucion.empty else '_(sin datos)_')}\n\n"
    "## 3. Evolución Mensual\n"
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
# --- FIN BLOQUE CORREGIDO ---

REPORT_FILE = OUT / "reporte.md"
REPORT_FILE.write_text(report, encoding="utf-8")

print(f"--- (4) Reporte completado ---")
print(f"OK · Reporte  :", REPORT_FILE)
print(f"OK · Parquet  :", PARQUET_FILE if PARQUET_FILE.exists() else "sin datos")
print(f"OK · SQLite   :", DB)
print("=== PIPELINE FINALIZADO ===")