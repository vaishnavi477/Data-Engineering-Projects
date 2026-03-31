import os
import pandas as pd
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

BASE_PATH = os.getenv("BASE_PATH")
DEID_DATA_PATH = os.getenv("DEID_DATA_PATH")
CURATED_DATA_PATH = os.getenv("CURATED_DATA_PATH")

DEID_PATH = os.path.join(BASE_PATH, DEID_DATA_PATH)
CURATED_PATH = os.path.join(BASE_PATH, CURATED_DATA_PATH)

os.makedirs(CURATED_PATH, exist_ok=True)

# ---------------------------
# Input / Output files
# ---------------------------
ENCOUNTERS_FILE = os.path.join(DEID_PATH, "encounters_deid.csv")
OUTPUT_FILE = os.path.join(CURATED_PATH, "fct_patient_utilization.csv")

# ---------------------------
# Load Data
# ---------------------------
print("Loading de-identified encounters data...")
encounters = pd.read_csv(ENCOUNTERS_FILE)

# ---------------------------
# Data Type Fixes
# ---------------------------
encounters["admit_date"] = pd.to_datetime(encounters["admit_date"])
encounters["discharge_date"] = pd.to_datetime(encounters["discharge_date"])

# ---------------------------
# Feature Engineering
# ---------------------------
print("Calculating length_of_stay...")
encounters["length_of_stay"] = (encounters["discharge_date"] - encounters["admit_date"]).dt.days

# Remove negative LOS
encounters["length_of_stay"] = encounters["length_of_stay"].apply(lambda x: max(x, 0))

# Remove bad cost records
encounters = encounters[encounters["total_cost"] >= 0]

# ---------------------------
# Aggregation (Curated Layer)
# ---------------------------
print("Building patient utilization fact table...")
fct = encounters.groupby("patient_key").agg(
    visit_count=("encounter_id", "count"),
    avg_cost=("total_cost", "mean"),
    total_cost=("total_cost", "sum"),
    max_cost=("total_cost", "max"),
    min_cost=("total_cost", "min"),
    avg_los=("length_of_stay", "mean"),
).reset_index()

# ---------------------------
# Risk Segmentation Logic
# ---------------------------
print("Applying risk segmentation...")

def classify_risk(row):
    if row["visit_count"] > 10 or row["avg_cost"] > 8000 or row["avg_los"] > 7:
        return "high_risk"
    elif 5 <= row["visit_count"] <= 10 or 4000 <= row["avg_cost"] <= 8000:
        return "medium_risk"
    else:
        return "low_risk"

fct["risk_segment"] = fct.apply(classify_risk, axis=1)

# ---------------------------
# Save Output
# ---------------------------
fct.to_csv(OUTPUT_FILE, index=False)
print(f"Curated dataset saved to: {OUTPUT_FILE}")
print("Curated pipeline complete!")
