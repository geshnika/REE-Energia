import os
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# ── Consultar Azure SQL ───────────────────────────────────────
with engine.connect() as conn:
    resultado = conn.execute(text("""
        SELECT
             'Generacion'    AS Tabla
            ,COUNT(*)        AS Filas
            ,MIN(Fecha)      AS Desde
            ,MAX(Fecha)      AS Hasta
        FROM Generacion
        UNION ALL SELECT 'Demanda',      COUNT(*), MIN(Fecha), MAX(Fecha) FROM Demanda
        UNION ALL SELECT 'Emisiones',    COUNT(*), MIN(Fecha), MAX(Fecha) FROM Emisiones
        UNION ALL SELECT 'Precios',      COUNT(*), MIN(Fecha), MAX(Fecha) FROM Precios
        UNION ALL SELECT 'Intercambios', COUNT(*), MIN(Fecha), MAX(Fecha) FROM Intercambios
    """))
    filas = resultado.fetchall()

# ── Construir tabla markdown ──────────────────────────────────
tabla_md = "| Table | Rows | From | To |\n"
tabla_md += "|---|---|---|---|\n"

for row in filas:
    tabla_md += f"| `{row[0]}` | {row[1]:,} | {row[2]} | {row[3]} |\n"

tabla_md += f"\n_Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_"

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

