# 1 — Imports

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 2 — Conexión

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

def crear_engine():
    return create_engine(
        f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
        "?driver=ODBC+Driver+17+for+SQL+Server",
        connect_args    = {"timeout": 180},
        fast_executemany = True
    )

for intento in range(3):
    try:
        start  = time.time()
        engine = crear_engine()
        with engine.connect() as conn:
            elapsed = round(time.time() - start, 2)
            print(f"Conexión exitosa | Intento {intento + 1} | Tiempo: {elapsed}s")
        break
    except Exception as e:
        print(f"Intento {intento + 1} fallido: {e}")
        if intento < 2:
            print("Reintentando en 30 segundos...")
            time.sleep(30)
        else:
            raise

# 3 — Rango dinámico desde SQL

hoy = datetime.now().date()

with engine.connect() as conn:
    result       = conn.execute(text("SELECT MAX(Fecha) FROM ree.Generacion"))
    ultima_fecha = result.scalar()

if ultima_fecha is None:
    desde = date(2014, 1, 1)
else:
    desde = ultima_fecha + timedelta(days=1)

hasta = hoy

if desde > hasta:
    print(f"Base de datos al día (última fecha: {ultima_fecha}). Nada que cargar.")
    exit()

start_str = f"{desde}T00:00"
end_str   = f"{hasta}T23:59"

print(f"Rango: {desde} → {hasta} ({(hasta - desde).days + 1} día/s)")

# 4 — Funciones auxiliares

def verificar_api(url, params):
    response = requests.get(url, headers = {"Accept": "application/json"}, params = params)
    if response.status_code != 200:
        raise ConnectionError(f"API respondió con status {response.status_code}")
    if not response.text.strip():
        raise ValueError("API respondió con cuerpo vacío")
    return response.json()

def cargar_tabla(df, tabla, claves):
    if df.empty:
        print("  Sin filas para procesar.")
        return 0.0

    start     = time.time()
    schema    = tabla.split(".")[0]
    base      = tabla.split(".")[1]
    all_cols  = list(df.columns)
    fecha_min = str(df["Fecha"].min())
    fecha_max = str(df["Fecha"].max())

    col_list     = ", ".join([f"[{c}]" for c in all_cols])
    placeholders = ", ".join(["?" for _ in all_cols])
    insert_sql   = f"INSERT INTO [{schema}].[{base}] ({col_list}) VALUES ({placeholders})"
    rows         = [tuple(r) for r in df.itertuples(index=False, name=None)]

    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM [{schema}].[{base}]
            WHERE Fecha BETWEEN :ini AND :fin
        """), {"ini": fecha_min, "fin": fecha_max})

        raw = conn.connection.cursor()
        raw.executemany(insert_sql, rows)

    return round(time.time() - start, 2)

# 5 — Generacion

print("=" * 60)
print("GENERACION")
print("=" * 60)

url    = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
params = {
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

if not registros:
    print(f"ADVERTENCIA: API devolvió 0 registros para {start_str} → {end_str}")
else:
    df          = pd.DataFrame(registros)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    elapsed     = cargar_tabla(df, "ree.Generacion", ["Fecha", "Fuente"])
    print(f"{len(df):,} filas procesadas | {elapsed}s")

# 6 — Demanda

print("=" * 60)
print("DEMANDA")
print("=" * 60)

url    = "https://apidatos.ree.es/es/datos/demanda/evolucion"
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
            "Fecha"     : punto["datetime"][:10],
            "Tipo"      : tipo,
            "Valor_mwh" : punto["value"]
        })

if not registros:
    print(f"ADVERTENCIA: API devolvió 0 registros para {start_str} → {end_str}")
else:
    df          = pd.DataFrame(registros)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    elapsed     = cargar_tabla(df, "ree.Demanda", ["Fecha", "Tipo"])
    print(f"{len(df):,} filas procesadas | {elapsed}s")

# 7 — Emisiones

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

if not registros:
    print(f"ADVERTENCIA: API devolvió 0 registros para {start_str} → {end_str}")
else:
    df          = pd.DataFrame(registros)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    elapsed     = cargar_tabla(df, "ree.Emisiones", ["Fecha", "Tipo"])
    print(f"{len(df):,} filas procesadas | {elapsed}s")

# 8 — Precios

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

if not registros:
    print(f"ADVERTENCIA: API devolvió 0 registros para {start_str} → {end_str}")
else:
    df          = pd.DataFrame(registros)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    df["Hora"]  = pd.to_datetime(df["Hora"], format = "%H:%M:%S").dt.time
    elapsed     = cargar_tabla(df, "ree.Precios", ["Fecha", "Hora", "Zona", "Tipo"])
    print(f"{len(df):,} filas procesadas | {elapsed}s")

# 9 — Intercambios

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

if not registros:
    print(f"ADVERTENCIA: API devolvió 0 registros para {start_str} → {end_str}")
else:
    df          = pd.DataFrame(registros)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    elapsed     = cargar_tabla(df, "ree.Intercambios", ["Fecha", "Pais", "Tipo"])
    print(f"{len(df):,} filas procesadas | {elapsed}s")

# 10 — Resumen

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
        FROM ree.Generacion
        UNION ALL SELECT 'Demanda',      COUNT(*), MIN(Fecha), MAX(Fecha) FROM ree.Demanda
        UNION ALL SELECT 'Emisiones',    COUNT(*), MIN(Fecha), MAX(Fecha) FROM ree.Emisiones
        UNION ALL SELECT 'Precios',      COUNT(*), MIN(Fecha), MAX(Fecha) FROM ree.Precios
        UNION ALL SELECT 'Intercambios', COUNT(*), MIN(Fecha), MAX(Fecha) FROM ree.Intercambios
    """))
    df_resumen = pd.DataFrame(resultado.fetchall(), columns = ["Tabla", "Filas", "Desde", "Hasta"])

print(df_resumen.to_string(index = False))
print(f"\nTiempo consulta: {round(time.time() - start, 2)}s")
print(f"\nDaily completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
