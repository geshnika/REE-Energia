# 1 — Imports

import os
import time
import calendar
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 2. Conexiones

# ── Cargar variables de entorno ──────────────────────────────
load_dotenv()

DB_SERVER   = os.getenv("DB_SERVER")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

variables = {
    "DB_SERVER"   : DB_SERVER,
    "DB_NAME"     : DB_NAME,
    "DB_USER"     : DB_USER,
    "DB_PASSWORD" : DB_PASSWORD
}

faltantes = [k for k, v in variables.items() if not v]
if faltantes:
    raise ValueError(f"Variables de entorno faltantes: {faltantes}")

start  = time.time()
engine = create_engine(
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

with engine.connect() as conn:
    print(f"Conexión exitosa | Tiempo: {round(time.time() - start, 2)}s")

# ── Rango de fechas ───────────────────────────────────────────
hoy  = datetime.now().date()
ayer = hoy - timedelta(days = 1)

start_str = f"{ayer}T00:00"
end_str   = f"{hoy}T23:59"

print(f"Rango: {ayer} → {hoy}")

# 3 — Funciones auxiliares

def verificar_api(url, params):
    """ Verifica que el endpoint responde correctamente """
    response = requests.get(url, headers = {"Accept": "application/json"}, params = params)
    if response.status_code != 200:
        raise ConnectionError(f"API respondió con status {response.status_code}")
    if not response.text.strip():
        raise ValueError("API respondió con cuerpo vacío")
    return response.json()


def merge_tabla(df, tabla, claves, columnas):
    """
    Hace MERGE contra la tabla destino:
    - INSERT si la fila no existe
    - UPDATE si existe y cambió algún valor (dispara trigger → Historico)
    - Ignora si existe y no cambió
    """
    start = time.time()

    # Crear tabla temporal con los datos nuevos
    df.to_sql(f"##tmp_{tabla}", con = engine, if_exists = "replace", index = False)

    # Construir condición ON
    on_clause      = " AND ".join([f"T.{c} = S.{c}" for c in claves])

    # Construir condición WHEN MATCHED (solo actualiza si cambió algo)
    match_clause   = " OR ".join([f"T.{c} <> S.{c}" for c in columnas])

    # Construir SET
    set_clause     = ", ".join([f"T.{c} = S.{c}" for c in columnas])

    # Construir INSERT
    all_cols       = claves + columnas
    insert_cols    = ", ".join(all_cols)
    insert_vals    = ", ".join([f"S.{c}" for c in all_cols])

    merge_sql = f"""
        MERGE {tabla} AS T
        USING ##tmp_{tabla} AS S
        ON {on_clause}
        WHEN MATCHED AND ({match_clause})
            THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED
            THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    with engine.connect() as conn:
        conn.execute(text(merge_sql))
        conn.commit()

    elapsed = round(time.time() - start, 2)
    return elapsed
	
# 4 — Generacion

print("=" * 60)
print("GENERACION")
print("=" * 60)

start_tabla = time.time()
url         = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
params      = {
    "start_date" : start_str,
    "end_date"   : end_str,
    "time_trunc" : "day",
    "geo_limit"  : "peninsular",
    "geo_ids"    : "8741"
}

datos     = verificar_api(url, params).get("included", [])
registros = []

for fuente in datos:
    tipo = fuente["attributes"]["title"]
    for punto in fuente["attributes"]["values"]:
        registros.append({
            "Fecha"      : punto["datetime"][:10],
            "Fuente"     : tipo,
            "Valor_mwh"  : punto["value"],
            "Porcentaje" : punto["percentage"]
        })

df          = pd.DataFrame(registros)
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date

elapsed = merge_tabla(
    df       = df,
    tabla    = "Generacion",
    claves   = ["Fecha", "Fuente"],
    columnas = ["Valor_mwh", "Porcentaje"]
)

print(f"{len(df):,} filas procesadas | {elapsed}s")

# 5 — Demanda

print("=" * 60)
print("DEMANDA")
print("=" * 60)

start_tabla = time.time()
url         = "https://apidatos.ree.es/es/datos/demanda/evolucion"
params      = {
    "start_date" : start_str,
    "end_date"   : end_str,
    "time_trunc" : "day",
    "geo_limit"  : "peninsular",
    "geo_ids"    : "8741"
}

datos     = verificar_api(url, params).get("included", [])
registros = []

for item in datos:
    tipo = item["attributes"]["title"]
    for punto in item["attributes"]["values"]:
        registros.append({
            "Fecha"     : punto["datetime"][:10],
            "Tipo"      : tipo,
            "Valor_mwh" : punto["value"]
        })

df          = pd.DataFrame(registros)
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date

elapsed = merge_tabla(
    df       = df,
    tabla    = "Demanda",
    claves   = ["Fecha", "Tipo"],
    columnas = ["Valor_mwh"]
)

print(f"{len(df):,} filas procesadas | {elapsed}s")

# 6 — Emisiones

print("=" * 60)
print("EMISIONES")
print("=" * 60)

url    = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion-emisiones-asociadas"
params = {
    "start_date" : start_str,
    "end_date"   : end_str,
    "time_trunc" : "day",
    "geo_limit"  : "peninsular",
    "geo_ids"    : "8741"
}

datos     = verificar_api(url, params).get("included", [])
registros = []

for item in datos:
    tipo = item["attributes"]["title"]
    for punto in item["attributes"]["values"]:
        registros.append({
            "Fecha" : punto["datetime"][:10],
            "Tipo"  : tipo,
            "Valor" : punto["value"]
        })

df          = pd.DataFrame(registros)
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date

elapsed = merge_tabla(
    df       = df,
    tabla    = "Emisiones",
    claves   = ["Fecha", "Tipo"],
    columnas = ["Valor"]
)

print(f"{len(df):,} filas procesadas | {elapsed}s")

# 7 — Precios

print("=" * 60)
print("PRECIOS")
print("=" * 60)

url    = "https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real"
params = {
    "start_date" : start_str,
    "end_date"   : end_str,
    "time_trunc" : "hour"
}

datos     = verificar_api(url, params).get("included", [])
registros = []

for item in datos:
    tipo = item["attributes"]["title"]
    for punto in item["attributes"]["values"]:
        dt   = punto["datetime"]
        zona = dt[23:29]
        registros.append({
            "Fecha"         : dt[:10],
            "Hora"          : dt[11:19],
            "Zona"          : zona,
            "Tipo"          : tipo,
            "Valor_eur_mwh" : punto["value"]
        })

df          = pd.DataFrame(registros)
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
df["Hora"]  = pd.to_datetime(df["Hora"], format = "%H:%M:%S").dt.time

elapsed = merge_tabla(
    df       = df,
    tabla    = "Precios",
    claves   = ["Fecha", "Hora", "Zona", "Tipo"],
    columnas = ["Valor_eur_mwh"]
)

print(f"{len(df):,} filas procesadas | {elapsed}s")

# 8 — Intercambios

print("=" * 60)
print("INTERCAMBIOS")
print("=" * 60)

url    = "https://apidatos.ree.es/es/datos/intercambios/todas-fronteras-programados"
params = {
    "start_date" : start_str,
    "end_date"   : end_str,
    "time_trunc" : "day",
    "geo_limit"  : "peninsular",
    "geo_ids"    : "8741"
}

datos     = verificar_api(url, params).get("included", [])
registros = []

for pais_item in datos:
    pais    = pais_item["attributes"]["title"].capitalize()
    content = pais_item["attributes"].get("content", [])
    for flujo in content:
        tipo = flujo["attributes"]["title"]
        for punto in flujo["attributes"]["values"]:
            registros.append({
                "Fecha"     : punto["datetime"][:10],
                "Pais"      : pais,
                "Tipo"      : tipo,
                "Valor_mwh" : punto["value"]
            })

df          = pd.DataFrame(registros)
df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date

elapsed = merge_tabla(
    df       = df,
    tabla    = "Intercambios",
    claves   = ["Fecha", "Pais", "Tipo"],
    columnas = ["Valor_mwh"]
)

print(f"{len(df):,} filas procesadas | {elapsed}s")

# 9 — Resumen

print("=" * 60)
print("RESUMEN DAILY")
print("=" * 60)

start = time.time()

with engine.connect() as conn:
    resultado  = conn.execute(text("""
        SELECT
            'Generacion' AS Tabla
            ,COUNT(*) AS Filas
            ,MIN(Fecha) AS Desde
            ,MAX(Fecha) AS Hasta
        FROM Generacion
        UNION ALL SELECT 'Demanda', COUNT(*), MIN(Fecha), MAX(Fecha) FROM Demanda
        UNION ALL SELECT 'Emisiones', COUNT(*), MIN(Fecha), MAX(Fecha) FROM Emisiones
        UNION ALL SELECT 'Precios', COUNT(*), MIN(Fecha), MAX(Fecha) FROM Precios
        UNION ALL SELECT 'Intercambios', COUNT(*), MIN(Fecha), MAX(Fecha) FROM Intercambios
    """))
    df_resumen = pd.DataFrame(resultado.fetchall(), columns = ["Tabla", "Filas", "Desde", "Hasta"])

print(df_resumen.to_string(index = False))
print(f"\nTiempo consulta: {round(time.time() - start, 2)}s")
print(f"\nDaily completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")