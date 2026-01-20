# NHS A-E-Analytics-Project-December 2025
This project builds an end-to-end SQL Server Data Warehouse for NHS Accident &amp; Emergency (A&amp;E) performance analysis.

It demonstrates real-world concepts used in NHS, Deloitte, NHS Digital, EY & BI/Data Engineering teams:

Multi-layer data modelling (Bronze/Silver/Gold)

Data quality validation

Transformations using T-SQL

KPI modelling

Window functions, ranking, benchmarking

Analytics-ready views for Power BI

🟤 Bronze Layer — Raw Ingestion
Tasks performed:

Created schemas (Bronze, Silver, Gold)

Bulk Insert of monthly NHS A&E CSV

Raw table created exactly as source

No data modification (true staging zone)

Key skills used:

BULK INSERT, schema design, raw ingestion patterns

⚪ Silver Layer — Cleaned, Structured, Validated
Transformations applied:

✔ Trimmed spaces (LTRIM/RTRIM)
✔ Converted numeric text → INT using TRY_CAST
✔ Replaced blanks with NULL (NULLIF)
✔ Null proofing checks
✔ Duplicate checks
✔ Negative value checks
✔ Logical validation (Over4hrs ≤ Attendances)

Focus:

A fully trusted, cleaned dataset ready for analytics.

🟡 Gold Layer — KPI Modelling & Analytics
Gold Views created:

1️⃣ provider_kpis

Total attendances

Total 4+ hours waits

Emergency admissions

A&E performance ratio

Clean, aggregated table for BI tools

2️⃣ providers_Over4hrs_Ratio

Best / Worst performers

Dense ranking per period

3️⃣ provider_benchmarking

Performance bands

Excellent ≤ 5%

Good ≤ 10%

Need Improvement ≤ 15%

Critical > 15%

National benchmark using window function

Uniform provider comparison

4️⃣ provider_pressure_metrices

4–12 hour waits per 1,000 attendances

12+ hour waits per 1,000 attendances

Severe pressure indicators

🚀 How to Run

Clone the repo

Create schemas:

CREATE SCHEMA Bronze;
CREATE SCHEMA Silver;
CREATE SCHEMA Gold;


Run Bronze ingestion

Run Silver transformation

Run Gold analytical views

Connect Power BI to Gold views
