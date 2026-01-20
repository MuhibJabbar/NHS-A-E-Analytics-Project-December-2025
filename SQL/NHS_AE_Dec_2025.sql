/*==============================================================
  Project: NHS A&E Analytics (Bronze → Silver → Gold)
  Author: Muhib
  Purpose:
    - Ingest NHS A&E CSV data into Bronze (raw)
    - Clean & typecast into Silver (validated)
    - Publish KPI views into Gold (analytics-ready)
  Notes:
    - Update the BULK INSERT file path before running.
    - Run in SQL Server Management Studio (SSMS).
==============================================================*/

---------------------------------------------------------------
-- 0) CREATE SCHEMAS (Bronze, Silver, Gold)
-- Why? Layered architecture keeps raw data separate from cleaned
-- and analytics-ready outputs (industry standard).
---------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Bronze') EXEC('CREATE SCHEMA Bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Silver') EXEC('CREATE SCHEMA Silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Gold')   EXEC('CREATE SCHEMA Gold');
GO


/*==============================================================
  1) BRONZE LAYER: RAW INGESTION
  - Store raw CSV exactly as-is (all columns NVARCHAR)
  - No transformations here
==============================================================*/

---------------------------------------------------------------
-- 1.1 Create Bronze Raw Table (drop & recreate)
---------------------------------------------------------------
IF OBJECT_ID('Bronze.ae_raw','U') IS NOT NULL
    DROP TABLE Bronze.ae_raw;
GO

CREATE TABLE Bronze.ae_raw (
    Period nvarchar(255) NULL,
    Org_Code nvarchar(255) NULL,
    Parent_Org nvarchar(255) NULL,
    Org_name nvarchar(255) NULL,
    AE_attendances_Type_1 nvarchar(255) NULL,
    AE_attendances_Type_2 nvarchar(255) NULL,
    AE_attendances_Other_AE_Department nvarchar(255) NULL,
    AE_attendances_Booked_Appointments_Type_1 nvarchar(255) NULL,
    AE_attendances_Booked_Appointments_Type_2 nvarchar(255) NULL,
    AE_attendances_Booked_Appointments_Other_Department nvarchar(255) NULL,
    Attendances_over_4hrs_Type_1 nvarchar(255) NULL,
    Attendances_over_4hrs_Type_2 nvarchar(255) NULL,
    Attendances_over_4hrs_Other_Department nvarchar(255) NULL,
    Attendances_over_4hrs_Booked_Appointments_Type_1 nvarchar(255) NULL,
    Attendances_over_4hrs_Booked_Appointments_Type_2 nvarchar(255) NULL,
    Attendances_over_4hrs_Booked_Appointment_Other_Department nvarchar(255) NULL,
    Patients_who_have_waited_4_12_hs_from_DTA_to_admission nvarchar(255) NULL,
    Patients_who_have_waited_12_plus_hrs_from_DTA_to_admission nvarchar(255) NULL,
    Emergency_admissions_via_AE_Type_1 nvarchar(255) NULL,
    Emergency_admissions_via_AE_Type_2 nvarchar(255) NULL,
    Emergency_admissions_via_AE_Other_AE_department nvarchar(255) NULL,
    Other_emergency_admissions nvarchar(255) NULL
);
GO

---------------------------------------------------------------
-- 1.2 Load CSV into Bronze using BULK INSERT
-- NOTE: Change the file path to your local machine location.
---------------------------------------------------------------
TRUNCATE TABLE Bronze.ae_raw;

BULK INSERT Bronze.ae_raw
FROM 'C:\Users\muhib\OneDrive\Desktop\Datasets\NHS A&E project\December-2025-CSV.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

-- Quick sanity check
SELECT TOP 100 * FROM Bronze.ae_raw;
GO


/*==============================================================
  2) SILVER LAYER: CLEAN + TYPECAST + VALIDATE
  - Convert numeric NVARCHAR → INT
  - Trim spaces, convert blanks → NULL
  - Run data quality checks (nulls, duplicates, negatives, logic)
==============================================================*/

---------------------------------------------------------------
-- 2.1 Create Silver Transform Table (typed columns)
---------------------------------------------------------------
IF OBJECT_ID('Silver.ae_transform','U') IS NOT NULL
    DROP TABLE Silver.ae_transform;
GO

CREATE TABLE Silver.ae_transform (
    Period nvarchar(250) NULL,
    Org_Code NVARCHAR(20) NULL,
    Parent_Org NVARCHAR(255) NULL,
    Org_name NVARCHAR(255) NULL,

    AE_attendances_Type_1 INT NULL,
    AE_attendances_Type_2 INT NULL,
    AE_attendances_Other_AE_Department INT NULL,

    AE_attendances_Booked_Appointments_Type_1 INT NULL,
    AE_attendances_Booked_Appointments_Type_2 INT NULL,
    AE_attendances_Booked_Appointments_Other_Department INT NULL,

    Attendances_over_4hrs_Type_1 INT NULL,
    Attendances_over_4hrs_Type_2 INT NULL,
    Attendances_over_4hrs_Other_Department INT NULL,

    Attendances_over_4hrs_Booked_Appointments_Type_1 INT NULL,
    Attendances_over_4hrs_Booked_Appointments_Type_2 INT NULL,
    Attendances_over_4hrs_Booked_Appointment_Other_Department INT NULL,

    Patients_who_have_waited_4_12_hs_from_DTA_to_admission INT NULL,
    Patients_who_have_waited_12_plus_hrs_from_DTA_to_admission INT NULL,

    Emergency_admissions_via_AE_Type_1 INT NULL,
    Emergency_admissions_via_AE_Type_2 INT NULL,
    Emergency_admissions_via_AE_Other_AE_department INT NULL,
    Other_emergency_admissions INT NULL
);
GO

---------------------------------------------------------------
-- 2.2 Load Silver from Bronze with cleaning + TRY_CAST
---------------------------------------------------------------
TRUNCATE TABLE Silver.ae_transform;

INSERT INTO Silver.ae_transform (
    Period, Org_Code, Parent_Org, Org_name,
    AE_attendances_Type_1, AE_attendances_Type_2, AE_attendances_Other_AE_Department,
    AE_attendances_Booked_Appointments_Type_1, AE_attendances_Booked_Appointments_Type_2, AE_attendances_Booked_Appointments_Other_Department,
    Attendances_over_4hrs_Type_1, Attendances_over_4hrs_Type_2, Attendances_over_4hrs_Other_Department,
    Attendances_over_4hrs_Booked_Appointments_Type_1, Attendances_over_4hrs_Booked_Appointments_Type_2, Attendances_over_4hrs_Booked_Appointment_Other_Department,
    Patients_who_have_waited_4_12_hs_from_DTA_to_admission, Patients_who_have_waited_12_plus_hrs_from_DTA_to_admission,
    Emergency_admissions_via_AE_Type_1, Emergency_admissions_via_AE_Type_2, Emergency_admissions_via_AE_Other_AE_department,
    Other_emergency_admissions
)
SELECT
    NULLIF(LTRIM(RTRIM(Period)), '') AS Period,
    NULLIF(LTRIM(RTRIM(Org_Code)), '') AS Org_Code,
    NULLIF(LTRIM(RTRIM(Parent_Org)), '') AS Parent_Org,
    NULLIF(LTRIM(RTRIM(Org_name)), '') AS Org_name,

    TRY_CAST(NULLIF(LTRIM(RTRIM(AE_attendances_Type_1)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(AE_attendances_Type_2)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(AE_attendances_Other_AE_Department)), '') AS INT),

    TRY_CAST(NULLIF(LTRIM(RTRIM(AE_attendances_Booked_Appointments_Type_1)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(AE_attendances_Booked_Appointments_Type_2)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(AE_attendances_Booked_Appointments_Other_Department)), '') AS INT),

    TRY_CAST(NULLIF(LTRIM(RTRIM(Attendances_over_4hrs_Type_1)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Attendances_over_4hrs_Type_2)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Attendances_over_4hrs_Other_Department)), '') AS INT),

    TRY_CAST(NULLIF(LTRIM(RTRIM(Attendances_over_4hrs_Booked_Appointments_Type_1)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Attendances_over_4hrs_Booked_Appointments_Type_2)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Attendances_over_4hrs_Booked_Appointment_Other_Department)), '') AS INT),

    TRY_CAST(NULLIF(LTRIM(RTRIM(Patients_who_have_waited_4_12_hs_from_DTA_to_admission)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Patients_who_have_waited_12_plus_hrs_from_DTA_to_admission)), '') AS INT),

    TRY_CAST(NULLIF(LTRIM(RTRIM(Emergency_admissions_via_AE_Type_1)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Emergency_admissions_via_AE_Type_2)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Emergency_admissions_via_AE_Other_AE_department)), '') AS INT),

    TRY_CAST(NULLIF(LTRIM(RTRIM(Other_emergency_admissions)), '') AS INT)
FROM Bronze.ae_raw;
GO

-- Quick check
SELECT TOP 100 * FROM Silver.ae_transform;
GO


---------------------------------------------------------------
-- 2.3 DATA QUALITY CHECKS (Silver)
-- These checks prove data reliability & impress in interviews.
---------------------------------------------------------------

-- 2.3.1 Null proofing of key fields
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Period IS NULL THEN 1 ELSE 0 END) AS null_period,
    SUM(CASE WHEN Org_Code IS NULL THEN 1 ELSE 0 END) AS null_org_code,
    SUM(CASE WHEN Parent_Org IS NULL THEN 1 ELSE 0 END) AS null_parent_org,
    SUM(CASE WHEN Org_name IS NULL THEN 1 ELSE 0 END) AS null_org_name
FROM Silver.ae_transform;
GO

-- 2.3.2 Duplicate check by business key (Period + Org_Code + Org_name)
SELECT
    Period,
    Org_name,
    Org_Code,
    COUNT(*) AS duplicate_count
FROM Silver.ae_transform
GROUP BY Period, Org_name, Org_Code
HAVING COUNT(*) > 1;
GO

-- 2.3.3 Negative value check (should be none)
SELECT *
FROM Silver.ae_transform
WHERE
    AE_attendances_Type_1 < 0 OR AE_attendances_Type_2 < 0 OR AE_attendances_Other_AE_Department < 0
 OR Attendances_over_4hrs_Type_1 < 0 OR Attendances_over_4hrs_Type_2 < 0 OR Attendances_over_4hrs_Other_Department < 0
 OR Emergency_admissions_via_AE_Type_1 < 0 OR Emergency_admissions_via_AE_Type_2 < 0 OR Emergency_admissions_via_AE_Other_AE_department < 0
 OR Other_emergency_admissions < 0;
GO

-- 2.3.4 Logical check: over 4 hours cannot exceed attendances
SELECT *
FROM Silver.ae_transform
WHERE
    (Attendances_over_4hrs_Type_1 > AE_attendances_Type_1)
 OR (Attendances_over_4hrs_Type_2 > AE_attendances_Type_2)
 OR (Attendances_over_4hrs_Other_Department > AE_attendances_Other_AE_Department);
GO


/*==============================================================
  3) GOLD LAYER: ANALYTICS-READY VIEWS
  - Provide KPIs, benchmarking, rankings, and pressure metrics
  - Designed for direct Power BI connection
==============================================================*/

---------------------------------------------------------------
-- 3.1 gold.provider_kpis
-- KPI view: total attendances, total 4+ waits, admissions, ratio
---------------------------------------------------------------
CREATE OR ALTER VIEW Gold.provider_kpis AS
SELECT
    t.*,
    CAST(
        CASE 
            WHEN t.Total_AE_Attendances = 0 THEN NULL
            ELSE 100.0 * t.Total_Attendaces_over_4_hours / t.Total_AE_Attendances
        END
    AS DECIMAL(10,2)) AS Over4hrs_Ratio
FROM (
    SELECT 
        Period,
        Org_Code,
        Parent_Org,
        Org_name,

        -- Total A&E Attendances (all departments)
        ISNULL(AE_attendances_Type_1,0) +
        ISNULL(AE_attendances_Type_2,0) +
        ISNULL(AE_attendances_Other_AE_Department,0) AS Total_AE_Attendances,

        -- Total Attendances over 4 hours (booked appointment measures)
        ISNULL(Attendances_over_4hrs_Booked_Appointments_Type_1,0) +
        ISNULL(Attendances_over_4hrs_Booked_Appointments_Type_2,0) +
        ISNULL(Attendances_over_4hrs_Booked_Appointment_Other_Department,0) AS Total_Attendaces_over_4_hours,

        -- Total Emergency Admissions via A&E
        ISNULL(Emergency_admissions_via_AE_Type_1,0) +
        ISNULL(Emergency_admissions_via_AE_Type_2,0) +
        ISNULL(Emergency_admissions_via_AE_Other_AE_department,0) +
        ISNULL(Other_emergency_admissions,0) AS Total_Emergency_Admissions
    FROM Silver.ae_transform
) t;
GO


---------------------------------------------------------------
-- 3.2 gold.providers_Over4hrs_Ratio
-- Ranks providers per month (best & worst)
---------------------------------------------------------------
CREATE OR ALTER VIEW Gold.providers_Over4hrs_Ratio AS
SELECT *
FROM (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY Period ORDER BY Over4hrs_Ratio ASC)  AS Best_Performance_Rank,
        DENSE_RANK() OVER (PARTITION BY Period ORDER BY Over4hrs_Ratio DESC) AS Worst_Performance_Rank
    FROM Gold.provider_kpis
) t;
GO


---------------------------------------------------------------
-- 3.3 gold.provider_benchmarking
-- Adds performance banding + national benchmark per month
---------------------------------------------------------------
CREATE OR ALTER VIEW Gold.provider_benchmarking AS
SELECT 
    *,
    CASE 
        WHEN Over4hrs_Ratio <= 5  THEN 'Excellent'
        WHEN Over4hrs_Ratio <= 10 THEN 'Good'
        WHEN Over4hrs_Ratio <= 15 THEN 'Need Improvement'
        WHEN Over4hrs_Ratio IS NULL THEN 'No Data'
        ELSE 'Critical'
    END AS Performance_Banding,

    CAST(AVG(Over4hrs_Ratio) OVER (PARTITION BY Period) AS DECIMAL(10,2))
        AS National_Average_Over4hrs_Ratio
FROM Gold.provider_kpis;
GO


---------------------------------------------------------------
-- 3.4 gold.provider_pressure_metrices
-- Calculates severe pressure indicators per 1,000 attendances
---------------------------------------------------------------
CREATE OR ALTER VIEW Gold.provider_pressure_metrices AS
SELECT
    t.Period,
    t.Org_Code,
    t.Org_name,
    t.Parent_Org,
    t.Total_AE_Attendances,
    ISNULL(s.Patients_who_have_waited_4_12_hs_from_DTA_to_admission,0)  AS wait_4_12_hour,
    ISNULL(s.Patients_who_have_waited_12_plus_hrs_from_DTA_to_admission,0) AS wait_12_plus_hour,

    CAST(
        CASE 
            WHEN t.Total_AE_Attendances = 0 THEN NULL
            ELSE 1000.0 * ISNULL(s.Patients_who_have_waited_4_12_hs_from_DTA_to_admission,0) / t.Total_AE_Attendances
        END AS DECIMAL(12,2)
    ) AS Wait_4_12_per_1000,

    CAST(
        CASE 
            WHEN t.Total_AE_Attendances = 0 THEN NULL
            ELSE 1000.0 * ISNULL(s.Patients_who_have_waited_12_plus_hrs_from_DTA_to_admission,0) / t.Total_AE_Attendances
        END AS DECIMAL(12,2)
    ) AS Wait_12_hour_plus_per_1000
FROM Gold.provider_kpis t
JOIN Silver.ae_transform s
    ON s.Period = t.Period
   AND s.Org_Code = t.Org_Code;
GO


/*==============================================================
  4) OPTIONAL: EXPLORATION QUERIES (for analysis/testing)
  You can keep these in a separate file or comment them out.
==============================================================*/

---------------------------------------------------------------
-- 4.1 Overall totals per month
---------------------------------------------------------------
SELECT
    Period,
    SUM(Total_AE_Attendances) AS Total_Attendances_All_Providers,
    SUM(Total_Attendaces_over_4_hours) AS Total_Over4hrs_All_Providers,
    CAST(100.0 * SUM(Total_Attendaces_over_4_hours) / NULLIF(SUM(Total_AE_Attendances),0) AS DECIMAL(10,2)) 
        AS Total_Over4hrs_Ratio
FROM Gold.provider_kpis
GROUP BY Period
ORDER BY Period;
GO

---------------------------------------------------------------
-- 4.2 Top 10 best performers (lowest Over4hrs_Ratio)
---------------------------------------------------------------
SELECT TOP 10 *
FROM (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY Period ORDER BY Over4hrs_Ratio ASC) AS Best_Performance_Rank
    FROM Gold.provider_kpis
) t
ORDER BY Period, Best_Performance_Rank;
GO

---------------------------------------------------------------
-- 4.3 Top 10 worst performers (highest Over4hrs_Ratio)
---------------------------------------------------------------
SELECT TOP 10 *
FROM (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY Period ORDER BY Over4hrs_Ratio DESC) AS Worst_Performance_Rank
    FROM Gold.provider_kpis
) t
ORDER BY Period, Worst_Performance_Rank;
GO
