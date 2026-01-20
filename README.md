# 🏥 NHS A&E Performance Analytics (Bronze → Silver → Gold) — SQL Server
This project builds an end-to-end SQL Server analytics pipeline for NHS Accident & Emergency (A&E) performance reporting using a modern layered architecture:

Bronze = Raw CSV ingestion (staging)

Silver = Cleaning, datatype conversion, data quality validation

Gold = Analytics-ready KPI views for Power BI

The goal is to produce reliable provider-level KPIs, rankings, benchmarking, and pressure metrics that can be used directly in Power BI dashboards.

# 🎯 Project Objectives

✅ Ingest NHS A&E monthly CSV data into SQL Server
✅ Clean + standardize raw data into typed tables
✅ Run data quality checks (nulls, duplicates, negatives, logic rules)
✅ Create analytical views for reporting:

Over 4 hours performance ratio

Best/Worst provider ranking

National benchmark comparison

Waiting pressure metrics per 1,000 attendances

✅ Provide outputs that are Power BI ready.

🛠 Tech Stack

SQL Server

SSMS (SQL Server Management Studio)

T-SQL

BULK INSERT

TRY_CAST, NULLIF, LTRIM/RTRIM

CASE WHEN

Window functions: AVG() OVER, DENSE_RANK() OVER

Power BI (for dashboarding)

# 🟤 Bronze Layer (Raw Ingestion)
What happens here?

Creates a raw staging table: Bronze.ae_raw

All columns stored as NVARCHAR to match CSV

Loads CSV using BULK INSERT

Why Bronze?

To keep the dataset exactly as it arrives from the source before transformations.

# ⚪ Silver Layer (Transform + Clean + Validate)
What happens here?

Creates typed table: Silver.ae_transform

Cleans values:

trims spaces (LTRIM/RTRIM)

blanks → NULL (NULLIF)

numeric conversion using TRY_CAST

Data Quality Checks Included

✅ Null check for key columns
✅ Duplicate check using business key (Period, Org_Code, Org_Name)
✅ Negative values check (invalid for this dataset)
✅ Logical rule check (Over 4 hours cannot exceed attendances)

# 🟡 Gold Layer (Analytics-ready Views)

Gold contains reporting-ready views designed for Power BI.

✅ 1) Gold.provider_kpis

Core KPI dataset per provider and period:

Total_AE_Attendances

Total_Attendaces_over_4_hours

Total_Emergency_Admissions

Over4hrs_Ratio = % of 4+ hour waits

✅ 2) Gold.providers_Over4hrs_Ratio

Ranks providers per month:

Best_Performance_Rank (lowest ratio)

Worst_Performance_Rank (highest ratio)

✅ 3) Gold.provider_benchmarking

Adds:

Performance_Banding

Excellent (≤ 5)

Good (≤ 10)

Need Improvement (≤ 15)

Critical (> 15)

No Data

National_Average_Over4hrs_Ratio per month using:

AVG(Over4hrs_Ratio) OVER (PARTITION BY Period)

✅ 4) Gold.provider_pressure_metrices

Pressure indicators per provider:

Wait 4–12 hours per 1,000 attendances

Wait 12+ hours per 1,000 attendances

# ▶️ How to Run (SSMS)

Open the SQL script:

``sql/NHS_AE_Dec_2025.sql

Update the CSV path inside BULK INSERT:

FROM 'C:\YourPath\December-2025-CSV.csv'


Execute the script in SSMS (top to bottom)

Connect Power BI to the Gold views:

Gold.provider_kpis

Gold.provider_benchmarking

Gold.provider_pressure_metrices

Gold.providers_Over4hrs_Ratio

# Example Query Outputs (Validation)
Monthly Summary KPI

Total attendances across all providers

Total 4+ hours waits

Monthly % ratio

Best & Worst Providers

DENSE_RANK() per period for provider comparison
