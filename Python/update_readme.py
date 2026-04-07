import os
import re
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# ── Consulta: resumen de tablas ──────────────────────────────
with engine.connect() as conn:
    resultado = conn.execute(text("""
        SELECT
             'Generacion' AS Tabla
            ,COUNT(*) AS Filas
            ,SUM(IIF(FechaAlta = CAST(GETDATE() AS DATE), 1, 0)) AS DailyChange
            ,MIN(Fecha) AS Desde
            ,MAX(Fecha) AS Hasta
        FROM ree.Generacion
        UNION ALL SELECT 'Demanda', COUNT(*), SUM(IIF(FechaAlta = CAST(GETDATE() AS DATE), 1, 0)), MIN(Fecha), MAX(Fecha) FROM ree.Demanda
        UNION ALL SELECT 'Emisiones', COUNT(*), SUM(IIF(FechaAlta = CAST(GETDATE() AS DATE), 1, 0)), MIN(Fecha), MAX(Fecha) FROM ree.Emisiones
        UNION ALL SELECT 'Precios', COUNT(*), SUM(IIF(FechaAlta = CAST(GETDATE() AS DATE), 1, 0)), MIN(Fecha), MAX(Fecha) FROM ree.Precios
        UNION ALL SELECT 'Intercambios', COUNT(*), SUM(IIF(FechaAlta = CAST(GETDATE() AS DATE), 1, 0)), MIN(Fecha), MAX(Fecha) FROM ree.Intercambios
    """))
    filas = resultado.fetchall()

# ── Construir tabla markdown ──────────────────────────────────
tabla_md  = "| Table | Rows | Daily Change | From | To |\n"
tabla_md += "|---|---|---|---|---|\n"

for row in filas:
    tabla_md += f"| `{row[0]}` | {row[1]:,} | {row[2]:,} | {row[3]} | {row[4]} |\n"

tabla_md += f"\n_Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC_"

# ── Reemplazar sección en README ──────────────────────────────
with open("README.md", "r", encoding = "utf-8") as f:
    contenido = f.read()

nuevo_contenido = re.sub(
    r"(## Data Coverage\n)(.*?)(\n---)",
    rf"\1\n{tabla_md}\n\3",
    contenido,
    flags = re.DOTALL
)

with open("README.md", "w", encoding = "utf-8") as f:
    f.write(nuevo_contenido)

print("README actualizado correctamente")