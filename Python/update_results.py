import os
import time
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Cargar variables de entorno ──────────────────────────────
load_dotenv()

engine = create_engine(
    f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

start = time.time()

# ── Consulta: Generacion ─────────────────────────────────────
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
                ,SUM(IIF(Renovable = 1, Generacion, 0)) / SUM(Generacion) * 100 AS PctGeneracionRenovable
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
    filas    = resultado.fetchall()
    columnas = resultado.keys()

df_gen = pd.DataFrame(filas, columns = columnas)

# ── Consulta: Precios históricos ─────────────────────────────
with engine.connect() as conn:
    resultado = conn.execute(text("""
        WITH
          PreciosDiarios AS (
            SELECT
                 YEAR(Fecha) AS Año
                ,MONTH(Fecha) AS Mes
                ,CAST(AVG(Valor_eur_mwh) AS DECIMAL(10, 2)) AS AVG_Valor
                ,CAST(MIN(Valor_eur_mwh) AS DECIMAL(10, 2)) AS MIN_Valor
                ,CAST(MAX(Valor_eur_mwh) AS DECIMAL(10, 2)) AS MAX_Valor
            FROM [dbo].[Precios]
            GROUP BY
                 YEAR(Fecha)
                ,MONTH(Fecha)
        )

        SELECT
             ROW_NUMBER() OVER (ORDER BY Año ASC, TipoPrecio DESC) AS Id
            ,*
                                  
        FROM (
            SELECT
                 Año
                ,'AVG' AS TipoPrecio
                ,CAST(AVG(IIF(Mes = 1,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Enero
                ,CAST(AVG(IIF(Mes = 2,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Febrero
                ,CAST(AVG(IIF(Mes = 3,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Marzo
                ,CAST(AVG(IIF(Mes = 4,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Abril
                ,CAST(AVG(IIF(Mes = 5,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Mayo
                ,CAST(AVG(IIF(Mes = 6,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Junio
                ,CAST(AVG(IIF(Mes = 7,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Julio
                ,CAST(AVG(IIF(Mes = 8,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Agosto
                ,CAST(AVG(IIF(Mes = 9,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Septiembre
                ,CAST(AVG(IIF(Mes = 10, AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Octubre
                ,CAST(AVG(IIF(Mes = 11, AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Noviembre
                ,CAST(AVG(IIF(Mes = 12, AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Diciembre
                ,CAST(AVG(AVG_Valor) AS DECIMAL(5, 2)) AS Total
            FROM PreciosDiarios
            GROUP BY Año

            UNION ALL

            SELECT
                 Año
                ,'MAX' AS TipoPrecio
                ,MAX(IIF(Mes = 1,  MAX_Valor, NULL)) AS Enero
                ,MAX(IIF(Mes = 2,  MAX_Valor, NULL)) AS Febrero
                ,MAX(IIF(Mes = 3,  MAX_Valor, NULL)) AS Marzo
                ,MAX(IIF(Mes = 4,  MAX_Valor, NULL)) AS Abril
                ,MAX(IIF(Mes = 5,  MAX_Valor, NULL)) AS Mayo
                ,MAX(IIF(Mes = 6,  MAX_Valor, NULL)) AS Junio
                ,MAX(IIF(Mes = 7,  MAX_Valor, NULL)) AS Julio
                ,MAX(IIF(Mes = 8,  MAX_Valor, NULL)) AS Agosto
                ,MAX(IIF(Mes = 9,  MAX_Valor, NULL)) AS Septiembre
                ,MAX(IIF(Mes = 10, MAX_Valor, NULL)) AS Octubre
                ,MAX(IIF(Mes = 11, MAX_Valor, NULL)) AS Noviembre
                ,MAX(IIF(Mes = 12, MAX_Valor, NULL)) AS Diciembre
                ,CAST(AVG(MAX_Valor) AS DECIMAL(5, 2)) AS Total
            FROM PreciosDiarios
            GROUP BY Año

            UNION ALL

            SELECT
                 Año
                ,'MIN' AS TipoPrecio
                ,MIN(IIF(Mes = 1,  MIN_Valor, NULL)) AS Enero
                ,MIN(IIF(Mes = 2,  MIN_Valor, NULL)) AS Febrero
                ,MIN(IIF(Mes = 3,  MIN_Valor, NULL)) AS Marzo
                ,MIN(IIF(Mes = 4,  MIN_Valor, NULL)) AS Abril
                ,MIN(IIF(Mes = 5,  MIN_Valor, NULL)) AS Mayo
                ,MIN(IIF(Mes = 6,  MIN_Valor, NULL)) AS Junio
                ,MIN(IIF(Mes = 7,  MIN_Valor, NULL)) AS Julio
                ,MIN(IIF(Mes = 8,  MIN_Valor, NULL)) AS Agosto
                ,MIN(IIF(Mes = 9,  MIN_Valor, NULL)) AS Septiembre
                ,MIN(IIF(Mes = 10, MIN_Valor, NULL)) AS Octubre
                ,MIN(IIF(Mes = 11, MIN_Valor, NULL)) AS Noviembre
                ,MIN(IIF(Mes = 12, MIN_Valor, NULL)) AS Diciembre
                ,CAST(MIN(MIN_Valor) AS DECIMAL(5, 2)) AS Total
            FROM PreciosDiarios
            GROUP BY Año
            ) AS Ev
        ORDER BY Id DESC
    """))
    filas    = resultado.fetchall()
    columnas = resultado.keys()

df_precios = pd.DataFrame(filas, columns = columnas)

# ── Consulta: Precios por tramo horario ──────────────────────
with engine.connect() as conn:
    resultado = conn.execute(text("""
        SELECT
             ROW_NUMBER() OVER (ORDER BY Año ASC, TipoPrecio DESC) AS Id
            ,*
                                  
        FROM (
            SELECT
                 YEAR(Fecha) AS Año
                ,'AVG' AS TipoPrecio
                ,CAST(AVG(IIF(Hora BETWEEN '00:00:00' AND '07:59:00', Valor_eur_mwh, NULL)) AS DECIMAL(5, 2)) AS [00:00 - 07:59]
                ,CAST(AVG(IIF(Hora BETWEEN '08:00:00' AND '09:59:00', Valor_eur_mwh, NULL)) AS DECIMAL(5, 2)) AS [08:00 - 09:59]
                ,CAST(AVG(IIF(Hora BETWEEN '10:00:00' AND '13:59:00', Valor_eur_mwh, NULL)) AS DECIMAL(5, 2)) AS [10:00 - 13:59]
                ,CAST(AVG(IIF(Hora BETWEEN '14:00:00' AND '17:59:00', Valor_eur_mwh, NULL)) AS DECIMAL(5, 2)) AS [14:00 - 17:59]
                ,CAST(AVG(IIF(Hora BETWEEN '18:00:00' AND '21:59:00', Valor_eur_mwh, NULL)) AS DECIMAL(5, 2)) AS [18:00 - 21:59]
                ,CAST(AVG(IIF(Hora BETWEEN '22:00:00' AND '23:59:00', Valor_eur_mwh, NULL)) AS DECIMAL(5, 2)) AS [22:00 - 23:59]
            FROM Precios
            GROUP BY YEAR(Fecha)

            UNION ALL

            SELECT
                 YEAR(Fecha) AS Año
                ,'MAX' AS TipoPrecio
                ,MAX(IIF(Hora BETWEEN '00:00:00' AND '07:59:00', Valor_eur_mwh, NULL)) AS [00:00 - 07:59]
                ,MAX(IIF(Hora BETWEEN '08:00:00' AND '09:59:00', Valor_eur_mwh, NULL)) AS [08:00 - 09:59]
                ,MAX(IIF(Hora BETWEEN '10:00:00' AND '13:59:00', Valor_eur_mwh, NULL)) AS [10:00 - 13:59]
                ,MAX(IIF(Hora BETWEEN '14:00:00' AND '17:59:00', Valor_eur_mwh, NULL)) AS [14:00 - 17:59]
                ,MAX(IIF(Hora BETWEEN '18:00:00' AND '21:59:00', Valor_eur_mwh, NULL)) AS [18:00 - 21:59]
                ,MAX(IIF(Hora BETWEEN '22:00:00' AND '23:59:00', Valor_eur_mwh, NULL)) AS [22:00 - 23:59]
            FROM Precios
            GROUP BY YEAR(Fecha)

            UNION ALL

            SELECT
                 YEAR(Fecha) AS Año
                ,'MIN' AS TipoPrecio
                ,MIN(IIF(Hora BETWEEN '00:00:00' AND '07:59:00', Valor_eur_mwh, NULL)) AS [00:00 - 07:59]
                ,MIN(IIF(Hora BETWEEN '08:00:00' AND '09:59:00', Valor_eur_mwh, NULL)) AS [08:00 - 09:59]
                ,MIN(IIF(Hora BETWEEN '10:00:00' AND '13:59:00', Valor_eur_mwh, NULL)) AS [10:00 - 13:59]
                ,MIN(IIF(Hora BETWEEN '14:00:00' AND '17:59:00', Valor_eur_mwh, NULL)) AS [14:00 - 17:59]
                ,MIN(IIF(Hora BETWEEN '18:00:00' AND '21:59:00', Valor_eur_mwh, NULL)) AS [18:00 - 21:59]
                ,MIN(IIF(Hora BETWEEN '22:00:00' AND '23:59:00', Valor_eur_mwh, NULL)) AS [22:00 - 23:59]
            FROM Precios
            GROUP BY YEAR(Fecha)
            ) AS SegmentacionHoraria

        ORDER BY Id DESC
    """))
    filas    = resultado.fetchall()
    columnas = resultado.keys()

df_tramos = pd.DataFrame(filas, columns = columnas)

# ── Calcular valores narrativos: Generacion ──────────────────
anio_actual               = datetime.now().year
df_historico              = df_gen[df_gen["Año"] < anio_actual]
df_actual                 = df_gen[df_gen["Año"] == anio_actual].iloc[0]
df_2014                   = df_gen[df_gen["Año"] == 2014].iloc[0]
df_desde_2022             = df_gen[df_gen["Año"] >= 2022]

promedio_anual            = df_historico["GeneracionTotal"].mean() / 1_000_000
pct_demanda_2022          = df_desde_2022["PctGeneracionConsumida"].mean()
pct_renovable_ini         = df_2014["PctGeneracionRenovable"]
pct_renovable_act         = df_actual["PctGeneracionRenovable"]
pct_consumo_renovable_act = df_actual["PctConsumoRenovable"]
fuente_top_2014           = df_2014["TipoGeneracion_Top1"]
fuente_top_act            = df_actual["TipoGeneracion_Top1"]

# ── Calcular valores narrativos: Precios históricos ──────────
df_avg                    = df_precios[df_precios["TipoPrecio"] == "AVG"]
df_max                    = df_precios[df_precios["TipoPrecio"] == "MAX"]

avg_2022                  = df_avg[df_avg["Año"] == 2022]["Total"].iloc[0]
avg_2014                  = df_avg[df_avg["Año"] == 2014]["Total"].iloc[0]
avg_actual                = df_avg[df_avg["Año"] == anio_actual]["Total"].iloc[0]

max_2022                  = df_max[df_max["Año"] == 2022]["Total"].iloc[0]
max_2014                  = df_max[df_max["Año"] == 2014]["Total"].iloc[0]
max_actual                = df_max[df_max["Año"] == anio_actual]["Total"].iloc[0]

pct_avg_vs_2022           = round(avg_actual / avg_2022 * 100, 1)
pct_avg_vs_2014           = round(avg_actual / avg_2014 * 100, 1)
pct_max_vs_2022           = round(max_actual / max_2022 * 100, 1)
pct_max_vs_2014           = round(max_actual / max_2014 * 100, 1)

# ── Calcular valores narrativos: Precios por tramo ───────────
df_tramos_avg_act         = df_tramos[(df_tramos["Año"] == anio_actual) & (df_tramos["TipoPrecio"] == "AVG")].iloc[0]
df_tramos_max_act         = df_tramos[(df_tramos["Año"] == anio_actual) & (df_tramos["TipoPrecio"] == "MAX")].iloc[0]

avg_punta                 = df_tramos_avg_act["18:00 - 21:59"]
avg_valle                 = df_tramos_avg_act["14:00 - 17:59"]
max_punta                 = df_tramos_max_act["18:00 - 21:59"]
max_valle                 = df_tramos_max_act["14:00 - 17:59"]

pct_avg_punta_vs_valle    = round(avg_punta / avg_valle * 100, 1)
pct_max_punta_vs_valle    = round(max_punta / max_valle * 100, 1)

# ── Construir tabla generacion ────────────────────────────────
tabla_gen  = "| Id | Año | Gen. Renovable (MWh) | Gen. Total (MWh) | % Renovable | Consumo Total (MWh) | % Consumo Renovable | % Gen. Consumida | Top Fuente | Top Renovable |\n"
tabla_gen += "|---|---|---|---|---|---|---|---|---|---|\n"

for _, row in df_gen.sort_values("Año", ascending = False).iterrows():
    tabla_gen += (
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

# ── Construir tabla precios históricos ───────────────────────
tabla_precios  = "| Id | Año | Tipo | Enero | Febrero | Marzo | Abril | Mayo | Junio | Julio | Agosto | Septiembre | Octubre | Noviembre | Diciembre | Total |\n"
tabla_precios += "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n"

for _, row in df_precios.iterrows():
    def fmt(v):
        return f"{v:.2f}" if pd.notna(v) else "—"

    tabla_precios += (
        f"| {int(row['Id'])} "
        f"| {int(row['Año'])} "
        f"| {row['TipoPrecio']} "
        f"| {fmt(row['Enero'])} "
        f"| {fmt(row['Febrero'])} "
        f"| {fmt(row['Marzo'])} "
        f"| {fmt(row['Abril'])} "
        f"| {fmt(row['Mayo'])} "
        f"| {fmt(row['Junio'])} "
        f"| {fmt(row['Julio'])} "
        f"| {fmt(row['Agosto'])} "
        f"| {fmt(row['Septiembre'])} "
        f"| {fmt(row['Octubre'])} "
        f"| {fmt(row['Noviembre'])} "
        f"| {fmt(row['Diciembre'])} "
        f"| {fmt(row['Total'])} |\n"
    )

# ── Construir tabla tramos horarios ──────────────────────────
tabla_tramos  = "| Id | Año | Tipo | 00:00-07:59 | 08:00-09:59 | 10:00-13:59 | 14:00-17:59 | 18:00-21:59 | 22:00-23:59 |\n"
tabla_tramos += "|---|---|---|---|---|---|---|---|---|\n"

for _, row in df_tramos.iterrows():
    tabla_tramos += (
        f"| {int(row['Id'])} "
        f"| {int(row['Año'])} "
        f"| {row['TipoPrecio']} "
        f"| {fmt(row['00:00 - 07:59'])} "
        f"| {fmt(row['08:00 - 09:59'])} "
        f"| {fmt(row['10:00 - 13:59'])} "
        f"| {fmt(row['14:00 - 17:59'])} "
        f"| {fmt(row['18:00 - 21:59'])} "
        f"| {fmt(row['22:00 - 23:59'])} |\n"
    )

# ── Construir RESULTS.md ──────────────────────────────────────
contenido = f"""# Results — Spanish Electrical System

_Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC_

---

## Overview

Spain produces an average of **{promedio_anual:.0f} million** MWh per year and has demanded, since 2022, **{pct_demanda_2022:.1f}%** of that production.

Renewable energy generation has grown steadily since 2014, rising from **{pct_renovable_ini:.1f}%** to **{pct_renovable_act:.1f}%**.

Today, renewables are the leading source of both generation and consumption in the country. As renewable output grows, so does its share of demand — not because consumption has increased, but because fossil and nuclear sources are being replaced. Renewables now account for **{pct_renovable_act:.1f}%** of generation and **{pct_consumo_renovable_act:.1f}%** of consumption, having replaced **{fuente_top_2014}** with **{fuente_top_act}** as the country's top energy source.

---

## Generation — Annual Breakdown

{tabla_gen}

---

## Pricing

In terms of pricing, the market is evaluated at both micro and macro levels (hourly and monthly/annually). The most striking feature at a glance is the sharp spike in average prices (EUR/MWh) in 2022 — a particularly unusual year driven by:

- Russia's invasion of Ukraine and the subsequent cut in gas supply
- Surging natural gas prices across European markets
- An exceptional drought that severely reduced hydroelectric generation
- Post-COVID demand recovering at full force

Today, average prices stand at **{pct_avg_vs_2022:.1f}%** of that peak, though they still represent **{pct_avg_vs_2014:.1f}%** of what electricity cost in 2014 (excluding wage growth or inflation adjustments).

However, while average prices have come down, maximum prices remain historically high — sitting at **{pct_max_vs_2022:.1f}%** of their 2022 peak and **{pct_max_vs_2014:.1f}%** above 2014 levels, suggesting that price volatility has not returned to pre-crisis norms.

---

## Pricing — Historical Breakdown

{tabla_precios}

---

## Pricing — Hourly Pattern

On an hourly basis, during the current year there are clear price peaks in the **18:00–21:59** window. At their highest, peak prices reach **{pct_max_punta_vs_valle:.1f}%** of valley lows. Even at average levels, the gap remains significant — peak hours average **{pct_avg_punta_vs_valle:.1f}%** above the cheapest valley window (**14:00–17:59**), reflecting a structural pattern in daily electricity demand.

---

## Pricing — Hourly Breakdown

{tabla_tramos}

---

_Data sourced from the public API of [Red Eléctrica de España (REE)](https://www.ree.es)._
"""

with open("RESULTS.md", "w", encoding = "utf-8") as f:
    f.write(contenido)

elapsed = round(time.time() - start, 2)
print(f"RESULTS.md actualizado correctamente | Tiempo: {elapsed}s")