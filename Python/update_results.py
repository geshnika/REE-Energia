import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

print("Iniciando script...")

load_dotenv()
print("Variables cargadas")

# conexión
print("Conectando a BD...")
with engine.connect() as conn:
    print("Conexión exitosa, ejecutando query...")
    resultado = conn.execute(text("SELECT COUNT(*) FROM Generacion"))
    print(f"Test query OK: {resultado.fetchone()[0]} filas en Generacion")

start = time.time()

# ── Consulta principal ───────────────────────────────────────
with engine.connect() as conn:
    resultado = conn.execute(text("""
        WITH
          GeneracionBase AS (
            SELECT
                 YEAR(Fecha) AS Año
                ,ROW_NUMBER() OVER (
                    PARTITION BY YEAR(Fecha)
                    ORDER BY SUM(Valor_mwh) DESC
                    ) AS RNGeneracion
                ,CASE
                    WHEN Renovable = 1
                    THEN ROW_NUMBER() OVER (
                        PARTITION BY YEAR(Fecha)
                        ORDER BY
                             IIF(Renovable = 1, 0, 1)
                            ,SUM(Valor_mwh) DESC
                        )
                    ELSE 0
                    END AS RNRenovable
                ,Renovable
                ,Fuente
                ,SUM(Valor_mwh) AS Generacion
            FROM (
                SELECT
                     Fecha
                    ,CASE
                        WHEN Fuente IN (
                             'Eólica'
                            ,'Hidráulica'
                            ,'Solar térmica'
                            ,'Solar fotovoltaica'
                            ,'Otras renovables'
                            ,'Residuos renovables'
                            )
                        THEN 1
                        ELSE 0
                        END AS Renovable
                    ,Fuente
                    ,Valor_mwh
                FROM Generacion
                WHERE Fuente <> 'Generación total'
                ) AS ETL
            GROUP BY
                 YEAR(Fecha)
                ,Fuente
                ,Renovable
        ),

          GeneracionComparada AS (
            SELECT
                 ROW_NUMBER() OVER (ORDER BY Año ASC) AS Id
                ,Año
                ,SUM(IIF(Renovable = 1, Generacion, 0)) AS GeneracionRenovable
                ,SUM(IIF(Renovable = 1, Generacion, 0)) / SUM(Generacion) * 100.00 AS PctGeneracionRenovable
                ,SUM(Generacion) AS GeneracionTotal
                ,MAX(IIF(RNGeneracion = 1, Fuente, NULL)) AS TipoGeneracion_Top1
                ,MAX(IIF(RNGeneracion = 1, Generacion, NULL)) AS ValorGeneracion_Top1
                ,MAX(IIF(RNRenovable = 1, Fuente, NULL)) AS TipoRenovable_Top1
                ,MAX(IIF(RNRenovable = 1, Generacion, NULL)) AS ValorRenovable_Top1
            FROM GeneracionBase
            GROUP BY Año
        ),

          Consumo AS (
            SELECT
                 YEAR(Fecha) AS Año
                ,SUM(Valor_mwh) AS ConsumoTotal
            FROM Demanda
            GROUP BY YEAR(Fecha)
        )

        SELECT
             Gc.Id
            ,Gc.Año
            ,ROUND(Gc.GeneracionRenovable, 2) AS GeneracionRenovable
            ,ROUND(Gc.GeneracionTotal, 2) AS GeneracionTotal
            ,ROUND(Gc.PctGeneracionRenovable, 2) AS PctGeneracionRenovable
            ,ROUND(Cc.ConsumoTotal, 2) AS ConsumoTotal
            ,ROUND(100.00 * Gc.GeneracionRenovable / Cc.ConsumoTotal, 2) AS PctConsumoRenovable
            ,ROUND(100.00 * Cc.ConsumoTotal / Gc.GeneracionTotal, 2) AS PctGeneracionConsumida
            ,TipoGeneracion_Top1
            ,ROUND(ValorGeneracion_Top1, 2) AS ValorGeneracion_Top1
            ,TipoRenovable_Top1
            ,ROUND(ValorRenovable_Top1, 2) AS ValorRenovable_Top1
        FROM GeneracionComparada AS Gc
        LEFT JOIN Consumo AS Cc ON Cc.Año = Gc.Año
        ORDER BY Gc.Año ASC
    """))
    filas = resultado.fetchall()
    columnas = resultado.keys()

import pandas as pd
df = pd.DataFrame(filas, columns = columnas)

# ── Calcular valores narrativos ───────────────────────────────
anio_actual       = datetime.now().year
df_historico      = df[df["Año"] < anio_actual]
df_actual         = df[df["Año"] == anio_actual].iloc[0]
df_2014           = df[df["Año"] == 2014].iloc[0]
df_desde_2022     = df[df["Año"] >= 2022]

promedio_anual    = df_historico["GeneracionTotal"].mean() / 1_000_000
pct_demanda_2022  = df_desde_2022["PctGeneracionConsumida"].mean()
pct_renovable_ini = df_2014["PctGeneracionRenovable"]
pct_renovable_act = df_actual["PctGeneracionRenovable"]
fuente_top_2014   = df_2014["TipoGeneracion_Top1"]
fuente_top_act    = df_actual["TipoGeneracion_Top1"]

# ── Construir tabla markdown ──────────────────────────────────
tabla_md = "| Id | Año | Gen. Renovable (MWh) | Gen. Total (MWh) | % Renovable | Consumo Total (MWh) | % Consumo Renovable | % Gen. Consumida | Top Fuente | Top Renovable |\n"
tabla_md += "|---|---|---|---|---|---|---|---|---|---|\n"

for _, row in df.sort_values("Año", ascending = False).iterrows():
    tabla_md += (
        f"| {int(row['Id'])} "
        f"| {int(row['Año'])} "
        f"| {row['GeneracionRenovable']:,.2f} "
        f"| {row['GeneracionTotal']:,.2f} "
        f"| {row['PctGeneracionRenovable']:.2f}% "
        f"| {row['ConsumoTotal']:,.2f} "
        f"| {row['PctConsumoRenovable']:.2f}% "
        f"| {row['PctGeneracionConsumida']:.2f}% "
        f"| {row['TipoGeneracion_Top1']} "
        f"| {row['TipoRenovable_Top1']} |\n"
    )

# ── Construir RESULTS.md ──────────────────────────────────────
contenido = f"""# Results — Spanish Electrical System

_Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_

---

## Overview

Spain produces an average of **{promedio_anual:.0f} million** MWh per year and has demanded, since 2022, **{pct_demanda_2022:.1f}%** of that production.

Renewable energy generation has grown steadily since 2014, rising from **{pct_renovable_ini:.1f}%** to **{pct_renovable_act:.1f}%**. Today, renewables are the leading source of electricity generation in the country, replacing **{fuente_top_2014}** with **{fuente_top_act}**.

---

## Annual Breakdown

{tabla_md}

---

_Data sourced from the public API of [Red Eléctrica de España (REE)](https://www.ree.es)._
"""

with open("RESULTS.md", "w", encoding = "utf-8") as f:
    f.write(contenido)

elapsed = round(time.time() - start, 2)
print(f"RESULTS.md actualizado correctamente | Tiempo: {elapsed}s")