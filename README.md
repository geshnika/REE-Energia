# REE Energy Analysis — Spain Electrical System

Analysis of the Spanish electrical system using data from Red Eléctrica de España (REE) public API.
Covers generation, demand, emissions, market prices and cross-border exchanges from 2014 to present.

---

## What This Project Does

- Extracts historical and daily data from the REE API (apidatos.ree.es)
- Loads and stores it in an Azure SQL database with full audit tracking
- Designed for automated daily updates via GitHub Actions
- Ready for visualization in Power BI

---

## Stack

| Layer | Technology |
|---|---|
| Data source | REE API (apidatos.ree.es) |
| Language | Python 3.14 |
| Database | Azure SQL (Free Tier) |
| Orchestration | GitHub Actions |
| Visualization | Power BI |
| Version control | Git + GitHub |

---

## Database Model

Five fact tables loaded from the REE API:

| Table | Description | Granularity |
|---|---|---|
| `Generacion` | Electricity generation by source | Daily |
| `Demanda` | Electricity demand (real vs forecast) | Daily |
| `Emisiones` | CO₂ emissions by generation type | Daily |
| `Precios` | Market prices (PVPC + spot) | Hourly |
| `Intercambios` | Cross-border exchanges by country | Daily |

All tables include `FechaAlta` and `HoraAlta` audit columns.
All changes are tracked in a central `Historico` table via SQL triggers.

---

## Repository Structure
```
REE-Energia/
├── .gitignore
├── README.md
├── sql/
│   ├── Tablas.sql          # Table and audit table definitions
│   └── Triggers.sql        # Audit triggers for all tables
└── python/
    └── REE_Import.ipynb    # Historical data load (2014 → present)
```

---

## How to Reproduce

### 1. Prerequisites

- Python 3.14+
- Azure SQL database (free tier)
- ODBC Driver 17 for SQL Server

### 2. Install dependencies
```bash
python -m pip install requests pandas sqlalchemy pyodbc python-dotenv
```

### 3. Configure credentials

Create a `.env` file in the `python/` folder:
```
DB_SERVER=your_server.database.windows.net
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password
```

### 4. Set up the database

Run the SQL scripts in order:
```
sql/Tablas.sql      → creates all tables
sql/Triggers.sql    → creates audit triggers
```

### 5. Run the historical load

Open `python/REE_Import.ipynb` and run all cells.
Full load takes approximately 15 minutes and inserts ~330,000 rows.

---

## Data Coverage

| Table | Rows | From | To |
|---|---|---|---|
| `Generacion` | 54,209 | 2014-01-01 | 2026-03-26 |
| `Demanda` | 4,468 | 2014-01-01 | 2026-03-26 |
| `Emisiones` | 49,741 | 2014-01-01 | 2026-03-26 |
| `Precios` | 181,873 | 2014-01-01 | 2026-03-26 |
| `Intercambios` | 43,598 | 2014-01-01 | 2026-03-26 |

_Last updated: 2026-03-26 07:07 UTC_

---

## Roadmap

- [x] Historical data load
- [x] Audit tracking via SQL triggers
- [x] Automated daily updates via GitHub Actions
- [ ] Power BI dashboard

---

## Author

Juan Pablo Riesgo — [linkedin.com/in/riesgo-juan-pablo](https://linkedin.com/in/riesgo-juan-pablo)