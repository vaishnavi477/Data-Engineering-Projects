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

---

## Architecture


  RAW (PHI Data)
  ↓
  De-identification (HIPAA Safe Harbor)
  ↓
  PostgreSQL (Data Warehouse)
  ↓
  dbt (Staging + Transformations)
  ↓
  Curated Layer (Analytics-ready)
  ↓
  Data Quality (dbt + Great Expectations)
  ↓
  Export for BI / ML


---

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
├── ge_validate.py
├── build_curated.py
├── prefect_pipeline.py # Orchestration pipeline
│
└── README.md


---

## HIPAA Compliance (Key Feature)

This project demonstrates **Safe Harbor de-identification**:

Removed fields:
- Name
- SSN
- Email
- Phone
- Address

Transformed:
- DOB → Age + Age Group
- Patient ID → Hashed surrogate key

---

## Technologies Used

- Python 3.11
- PostgreSQL
- dbt (Data Build Tool)
- Great Expectations (Data Validation)
- Prefect (Workflow Orchestration)
- Pandas / NumPy
- Faker (Synthetic Data)

---

## Pipeline Execution

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
6️⃣ Data Validation (Great Expectations)
```bash
python3 ge_validate.py
```
7️⃣ Export Curated Data
```bash
python3 build_curated.py
```
#### Orchestrated Pipeline (Prefect)

Run entire pipeline:
```bash
python3 pipeline_prefect.py
```
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

Risk segmentation:

- High Risk
  - visit_count > 10 OR avg_cost > 8000 OR avg_los > 7
- Medium Risk
  - moderate utilization
- Low Risk
  - low utilization & cost

Key Highlights
- End-to-end healthcare pipeline
- HIPAA-compliant data handling
- Real-world data modeling
- Production-style orchestration
- Multi-layer data validation

Future Improvements
- Add Airflow orchestration
- Deploy on AWS/GCP
- Add dashboard (Tableau / Power BI)
- Add ML model for readmission prediction

---

Author

Vaishnavi Patil
