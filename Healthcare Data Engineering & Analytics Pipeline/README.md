# Healthcare Data Engineering & Analytics Pipeline (HIPAA-Compliant)

## Overview
This project simulates a real-world healthcare data platform handling PHI (Protected Health Information) with HIPAA-compliant de-identification, transformation, validation, and orchestration.

It demonstrates end-to-end data engineering skills including:
- Data generation (synthetic PHI)
- De-identification (HIPAA Safe Harbor)
- Data warehousing (PostgreSQL)
- Transformations using dbt
- Data quality checks (dbt tests + Great Expectations)
- Workflow orchestration using Prefect

## Architecture

Raw (PHI)
↓
De-identification (HIPAA Safe Harbor)
↓
PostgreSQL Warehouse (raw + deidentified schemas)
↓
dbt (Staging → Marts)
↓
Curated Layer (Analytics-ready)
↓
Data Quality (dbt + Great Expectations)
↓
Consumption (BI / ML)


## Project Structure


Healthcare_Analytics/
│
├── data/
│ ├── raw/ # PHI data
│ ├── deidentified/ # HIPAA-compliant data
│ └── curated/ # Final analytics dataset
│
├── healthcare_dbt/ # dbt project
| ├── dbt_project.yml
| ├── packages.yml
│ ├── models/
| | ├── schema.yml
│ │ ├── staging/
│ │ └── marts/
│
├── generate_healthcare_data.py
├── deidentify_pipeline.py
├── load_psql.py
├── build_curated.py
├── ge_validate.py
├── prefect_pipeline.py # Orchestration pipeline
│
└── README.md

## What This Project Demonstrates

- Designing HIPAA-compliant data pipelines
- Building layered data architecture (raw → staging → curated)
- Writing production-grade dbt models and tests
- Implementing data quality frameworks
- Orchestrating pipelines with retries and failure handling
- Working with healthcare-style data (claims, encounters, providers)

## Environment Configuration

Sensitive values and paths are managed using `.env`:

- Base project path
- Database credentials
- Script paths

This ensures:
- Security
- Portability
- Clean codebase (no hardcoded values)

## HIPAA Compliance (Safe Harbor Implementation)

This project implements HIPAA Safe Harbor de-identification:

### Direct Identifiers Removed
- Name
- SSN
- Email
- Phone number
- Address

### Transformations Applied
- DOB → Age + Age Groups (to prevent re-identification)
- Patient ID → SHA256 hashed surrogate key (`patient_key`)

### Data Segregation
- `raw` schema → Restricted PHI access
- `deidentified` schema → Analytics-safe data

## Technologies Used

- Python 3.11
- PostgreSQL
- dbt (Data Build Tool)
- Great Expectations (Data Validation)
- Prefect (Workflow Orchestration)
- Pandas / NumPy
- Faker (Synthetic Data)

## Pipeline Execution
The pipeline can be executed either step-by-step or fully orchestrated.

1️⃣ Generate PHI Data
```bash
python3 generate_healthcare_data.py
```
2️⃣ De-identify Data
```bash
python3 deidentify_pipeline.py
```
3️⃣ Load into PostgreSQL
```bash
python3 load_psql.py
```
4️⃣ Run dbt Transformations
```bash
cd healthcare_dbt
dbt run
```
5️⃣ Run dbt Tests
```bash
dbt test
```
6️⃣ Export Curated Data
```bash
python3 build_curated.py
```
7️⃣ Data Validation (Great Expectations)
```bash
python3 ge_validate.py
```
#### Orchestrated Pipeline (Prefect)

Run entire pipeline:
```bash
python3 prefect_pipeline.py
```
---

Features:

Task retries
Sequential execution
Failure handling
Modular tasks
Data Models
1. Staging Layer
  - stg_patients
  - stg_encounters
2. Mart Layer
  - fct_patient_utilization

--- 

Includes:

- Visit count
- Average cost
- Total cost
- Length of stay
- Risk segmentation
- Data Quality Checks
   - dbt Tests
   - Not null
   - Unique
- Relationships
- Accepted ranges
- Great Expectations
- Date validation
- Cost validation
- Length of stay checks
- Risk segment validation
- Business Logic

## Business Logic

Risk Segmentation

- High Risk
  - visit_count > 10 OR avg_cost > 8000 OR avg_los > 7

- Medium Risk
  - visit_count between 5–10 OR avg_cost between 4000–8000

- Low Risk
  - low utilization and cost
 
 ---
 
Data Quality Framework

dbt Tests (Schema-Level)
- Not null constraints
- Unique constraints
- Referential integrity
- Accepted value ranges

---

Great Expectations (Business-Level)
- Date consistency (admit vs discharge)
- Non-negative cost validation
- Length of stay constraints
- Risk segment validation
- Aggregation sanity checks

---

Key Highlights
- End-to-end healthcare pipeline
- HIPAA-compliant data handling
- Real-world data modeling
- Production-style orchestration
- Multi-layer data validation

---

Future Improvements
- Add Airflow orchestration
- Deploy on AWS/GCP
- Add dashboard (Tableau / Power BI)
- Add ML model for readmission prediction

## Author

Vaishnavi Patil
