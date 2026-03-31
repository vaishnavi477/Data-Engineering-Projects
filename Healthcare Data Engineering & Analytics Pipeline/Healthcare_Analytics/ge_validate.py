import os
from datetime import datetime
import pandas as pd
import great_expectations as ge
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

BASE_PATH = os.getenv("BASE_PATH")
DEID_DATA_PATH = os.getenv("DEID_DATA_PATH")
CURATED_DATA_PATH = os.getenv("CURATED_DATA_PATH")
GE_EXPECTATIONS_PATH = os.getenv("GE_EXPECTATIONS_PATH")

DEID_PATH = os.path.join(BASE_PATH, DEID_DATA_PATH)
CURATED_PATH = os.path.join(BASE_PATH, CURATED_DATA_PATH)
EXPECTATIONS_DIR = os.path.join(BASE_PATH, GE_EXPECTATIONS_PATH)

os.makedirs(EXPECTATIONS_DIR, exist_ok=True)

# ---------------------------
# FILE PATHS
# ---------------------------
patients_path = os.path.join(DEID_PATH, "patients_deid.csv")
encounters_path = os.path.join(DEID_PATH, "encounters_deid.csv")
curated_path = os.path.join(CURATED_PATH, "fct_patient_utilization.csv")

# ---------------------------
# LOAD DATA AS GE DATASETS
# ---------------------------
patients_df = ge.from_pandas(pd.read_csv(patients_path))
encounters_df = ge.from_pandas(pd.read_csv(encounters_path))
curated_df = ge.from_pandas(pd.read_csv(curated_path))

# ---------------------------
# PATIENTS EXPECTATIONS
# ---------------------------
patients_df.expect_column_values_to_not_be_null("patient_key")
patients_df.expect_column_values_to_be_unique("patient_key")
patients_df.expect_column_values_to_be_between("age", min_value=0, max_value=120)
patients_df.expect_column_values_to_not_be_null("age_group")
patients_df.expect_column_values_to_be_in_set(
    "age_group", ["0-17", "18-39", "40-59", "60+"]
)

patients_suite_path = os.path.join(EXPECTATIONS_DIR, "patients_suite.json")
patients_df.save_expectation_suite(patients_suite_path)
print(f"Patients expectations saved to {patients_suite_path}")

# ---------------------------
# ENCOUNTERS EXPECTATIONS
# ---------------------------
encounters_df["admit_date"] = pd.to_datetime(encounters_df["admit_date"])
encounters_df["discharge_date"] = pd.to_datetime(encounters_df["discharge_date"])
encounters_df["length_of_stay"] = (
    encounters_df["discharge_date"] - encounters_df["admit_date"]
).dt.days
encounters_df["length_of_stay"] = encounters_df["length_of_stay"].apply(lambda x: max(x, 0))

encounters_df.expect_column_values_to_not_be_null("encounter_id")
encounters_df.expect_column_values_to_be_unique("encounter_id")
encounters_df.expect_column_values_to_not_be_null("patient_key")
encounters_df.expect_column_values_to_be_in_set(
    "visit_type", ["inpatient", "outpatient", "emergency"]
)
encounters_df.expect_column_values_to_be_between("total_cost", min_value=0)
encounters_df.expect_column_values_to_not_be_null("admit_date")
encounters_df.expect_column_values_to_be_between(
    "admit_date", min_value="2018-01-01", max_value=datetime.today().date()
)
encounters_df.expect_column_values_to_not_be_null("discharge_date")
encounters_df.expect_column_pair_values_A_to_be_greater_than_B("discharge_date", "admit_date")
encounters_df.expect_column_values_to_be_between("length_of_stay", min_value=0, max_value=365)

encounters_suite_path = os.path.join(EXPECTATIONS_DIR, "encounters_suite.json")
encounters_df.save_expectation_suite(encounters_suite_path)
print(f"Encounters expectations saved to {encounters_suite_path}")

# ---------------------------
# CURATED EXPECTATIONS
# ---------------------------
curated_df.expect_column_values_to_not_be_null("patient_key")
curated_df.expect_column_values_to_be_unique("patient_key")
curated_df.expect_column_values_to_be_between("visit_count", min_value=1)
curated_df.expect_column_values_to_be_between("avg_cost", min_value=0)
curated_df.expect_column_values_to_be_between("total_cost", min_value=0)
curated_df.expect_column_values_to_be_between("avg_los", min_value=0, max_value=30)
curated_df.expect_column_values_to_be_in_set(
    "risk_segment", ["low_risk", "medium_risk", "high_risk"]
)
curated_df.expect_column_values_to_be_between("max_cost", min_value=0)
curated_df.expect_column_values_to_be_between("min_cost", min_value=0)

curated_suite_path = os.path.join(EXPECTATIONS_DIR, "curated_suite.json")
curated_df.save_expectation_suite(curated_suite_path)
print(f"Curated expectations saved to {curated_suite_path}")

# ---------------------------
# VALIDATE
# ---------------------------
print("\n--- VALIDATING PATIENTS ---")
patients_results = patients_df.validate()
print(patients_results["statistics"])

print("\n--- VALIDATING ENCOUNTERS ---")
encounters_results = encounters_df.validate()
print(encounters_results["statistics"])

print("\n--- VALIDATING CURATED DATA ---")
curated_results = curated_df.validate()
print(curated_results["statistics"])

print("\nValidation complete for all datasets!")
