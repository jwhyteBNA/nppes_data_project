# SQL Scripts Execution Guide

This directory contains SQL scripts for setting up the NPPES healthcare provider database. **Execute these scripts in the exact order listed below** for proper database setup.

## Execution Order

Run these scripts in numerical order to ensure all dependencies are met:

### 1. Core Data Tables
```sql
01_create_nppes_providers.sql
```
- Creates the main staging table for raw NPPES provider data
- Primary table that receives data from the ETL pipeline

### 2. Reference Tables (Dependencies)
```sql
02_create_nucc_taxonomy.sql
03_create_census_population.sql
04_create_zip_county.sql
05_create_ssa_fips_state_county.sql
```
- **02**: Healthcare provider taxonomy classifications and specialties
- **03**: Census population data for county analysis
- **04**: ZIP code to county crosswalk mapping
- **05**: FIPS code to county name mapping

### 3. Helper Functions
```sql
06_function_get_primary_taxonomy_code.sql
```
- Creates helper function to extract primary taxonomy from 15 taxonomy fields
- **Required before running the stored procedure**

### 4. Clean Data Table
```sql
07_create_nppes_providers_clean.sql
```
- Creates the processed/clean data table
- Target table for transformed and enriched data

### 5. Data Processing
```sql
08_sp_clean_nppes_data.sql
```
- Main data cleaning and transformation stored procedure
- Processes raw data and populates the clean table
- Includes county enrichment and data quality scoring

### 6. Reporting Views
```sql
09_create_nppes_final_report_view.sql
```
- Creates views for final data export and reporting

## Quick Setup

To run all scripts in order:

```bash
# Using psql command line
psql -d your_database -f 01_create_nppes_providers.sql
psql -d your_database -f 02_create_nucc_taxonomy.sql
psql -d your_database -f 03_create_census_population.sql
psql -d your_database -f 04_create_zip_county.sql
psql -d your_database -f 05_create_ssa_fips_state_county.sql
psql -d your_database -f 06_function_get_primary_taxonomy_code.sql
psql -d your_database -f 07_create_nppes_providers_clean.sql
psql -d your_database -f 08_sp_clean_nppes_data.sql
psql -d your_database -f 09_create_nppes_final_report_view.sql
```

## Dependencies

- **Scripts 02-05** must run before **Script 08** (stored procedure needs reference tables)
- **Script 06** must run before **Script 08** (stored procedure calls the helper function)
- **Script 07** must run before **Script 08** (stored procedure populates the clean table)
- **Script 09** should run after **Script 07** (views depend on clean table structure)

## Common Issues

❌ **Running stored procedure (08) before helper function (06)**: Will fail with "function does not exist"

❌ **Running stored procedure (08) before reference tables (02-05)**: Will fail on foreign key constraints or missing joins

❌ **Running views (09) before clean table (07)**: Will fail with "relation does not exist"

## After Setup

1. Load reference data using the ETL pipeline
2. Load raw NPPES data using the ETL pipeline
3. Run data cleaning: `CALL clean_and_populate_nppes_data();`
4. Query final results from the reporting views
