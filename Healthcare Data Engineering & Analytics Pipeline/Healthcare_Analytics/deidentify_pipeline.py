import pandas as pd
import hashlib
import os
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

BASE_PATH = os.getenv("BASE_PATH")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
DEID_DATA_PATH = os.getenv("DEID_DATA_PATH")

RAW_PATH = os.path.join(BASE_PATH, RAW_DATA_PATH)
DEID_PATH = os.path.join(BASE_PATH, DEID_DATA_PATH)

os.makedirs(DEID_PATH, exist_ok=True)

# ---------------------------
# Hashing (Tokenization)
# ---------------------------
def hash_id(val):
    return hashlib.sha256(str(val).encode()).hexdigest()

# ---------------------------
# De-identify Patients
# ---------------------------
def deidentify_patients():
    df = pd.read_csv(os.path.join(RAW_PATH, "patients.csv"))

    # Create surrogate key
    df["patient_key"] = df["patient_id"].apply(hash_id)

    # Age calculation (already exists but recompute for consistency)
    df["age"] = pd.to_datetime("today").year - pd.to_datetime(df["dob"]).dt.year

    # STANDARDIZED age groups (IMPORTANT: match GE + dbt)
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 17, 39, 59, 120],
        labels=["0-17", "18-39", "40-59", "60+"],
        right=True
    )

    # Drop PHI (HIPAA Safe Harbor)
    df = df.drop(columns=[
        "name",
        "ssn",
        "address",
        "email",
        "phone",
        "dob",
        "patient_id"
    ])

    df.to_csv(os.path.join(DEID_PATH, "patients_deid.csv"), index=False)

# ---------------------------
# De-identify Encounters
# ---------------------------
def deidentify_encounters():
    enc = pd.read_csv(os.path.join(RAW_PATH, "encounters.csv"))
    pat = pd.read_csv(os.path.join(RAW_PATH, "patients.csv"))

    # Create mapping
    pat["patient_key"] = pat["patient_id"].apply(hash_id)
    mapping = pat[["patient_id", "patient_key"]]

    enc = enc.merge(mapping, on="patient_id", how="left")

    # Drop PHI reference
    enc = enc.drop(columns=["patient_id"])

    enc.to_csv(os.path.join(DEID_PATH, "encounters_deid.csv"), index=False)

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    print("Running de-identification...")

    deidentify_patients()
    deidentify_encounters()

    print(f"De-identified data saved at: {DEID_PATH}")
