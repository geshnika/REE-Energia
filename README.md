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

## Analytical Framework

### Generation
- How has the energy mix evolved by source since 2014?
- How much has the share of renewables grown?
- Which source leads generation each year?
- How has total consumption evolved (overall and renewable)?
- What percentage of generation (overall and renewable) is consumed?

### Pricing
- How have prices evolved historically and seasonally?
- How did 2022 impact current price levels?
- What is the price distribution across hourly windows?

### Cross-border Exchanges
- Is Spain a net exporter or importer?
- Which countries does Spain exchange the most energy with?

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
├── RESULTS.md
├── .github/
│   └── workflows/
│       └── daily_load.yml                          # Automated daily workflow
├── SQL/
│   ├── Tables.sql                                  # Table and audit table definitions
│   ├── Triggers.sql                                # Audit triggers for all tables
│   ├── RESULTS_AnnualBreakdown.sql                 # Generation and consumption annual summary
│   ├── RESULTS_Pricing_HistoricBreakdown.sql       # Historical price breakdown by month
│   ├── RESULTS_Pricing_HoursBreakdown.sql          # Price breakdown by hourly window
│   └── RESULTS_CrossBorder_ExchangesBreakdown.sql  # Cross-border exchange summary
└── Python/
    ├── REE_Import.ipynb                            # Historical data load (2014 → present)
    ├── REE_Daily.py                                # Daily incremental load
    ├── update_readme.py                            # Automated README update
    └── update_results.py                           # Automated RESULTS.md update
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

| Table | Rows | Daily Change | From | To |
|---|---|---|---|---|
| `Generacion` | 54,352 | 13 | 2014-01-01 | 2026-04-06 |
| `Demanda` | 4,479 | 1 | 2014-01-01 | 2026-04-06 |
| `Emisiones` | 49,873 | 12 | 2014-01-01 | 2026-04-06 |
| `Precios` | 183,188 | 120 | 2014-01-01 | 2026-04-06 |
| `Intercambios` | 43,708 | 10 | 2014-01-01 | 2026-04-06 |

_Last updated: 2026-04-06 07:28 UTC_

---

## Roadmap

- [x] Historical data load
- [x] Audit tracking via SQL triggers
- [x] Automated daily updates via GitHub Actions
- [ ] Power BI dashboard

---

## Author

Juan Pablo Riesgo — [linkedin.com/in/riesgo-juan-pablo](https://linkedin.com/in/riesgo-juan-pablo)