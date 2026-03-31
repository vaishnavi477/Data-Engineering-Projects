import pandas as pd
import numpy as np
from faker import Faker
import uuid
import random
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

BASE_PATH = os.getenv("BASE_PATH")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")

# Construct full raw directory path
RAW_DIR = os.path.join(BASE_PATH, RAW_DATA_PATH)

# ---------------------------
# INIT
# ---------------------------
fake = Faker()
np.random.seed(42)
random.seed(42)

NUM_PATIENTS = 2000
NUM_PROVIDERS = 100
MAX_ENCOUNTERS_PER_PATIENT = 15

# ---------------------------
# Helper Functions
# ---------------------------
def random_date(start_year=2018, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_icd_code():
    return f"{random.choice(['E11', 'I10', 'J45', 'K21', 'M54'])}.{random.randint(0,9)}"

def generate_cpt_code():
    return str(random.randint(10000, 69999))

# ---------------------------
# Patients
# ---------------------------
def generate_patients(n):
    patients = []
    for _ in range(n):
        dob = fake.date_of_birth(minimum_age=0, maximum_age=90)
        age = (datetime.today().date() - dob).days // 365

        if age < 18:
            age_group = '0-17'
        elif age < 40:
            age_group = '18-39'
        elif age < 60:
            age_group = '40-59'
        else:
            age_group = '60+'

        patients.append({
            "patient_id": str(uuid.uuid4()),
            "name": fake.name(),
            "ssn": fake.ssn(),
            "address": fake.address().replace("\n", ", "),
            "dob": dob,
            "age": age,
            "age_group": age_group,
            "gender": random.choice(["M", "F"]),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "created_at": random_date()
        })

    return pd.DataFrame(patients)

# ---------------------------
# Providers
# ---------------------------
def generate_providers(n):
    specialties = ["Cardiology", "Dermatology", "General", "Orthopedics", "Pediatrics"]
    providers = []

    for _ in range(n):
        providers.append({
            "provider_id": str(uuid.uuid4()),
            "provider_name": fake.name(),
            "specialty": random.choice(specialties),
            "npi": str(random.randint(1000000000, 9999999999))
        })

    return pd.DataFrame(providers)

# ---------------------------
# Encounters
# ---------------------------
def generate_encounters(patients, providers):
    encounters = []

    for _, patient in patients.iterrows():
        num_encounters = random.randint(1, MAX_ENCOUNTERS_PER_PATIENT)

        for _ in range(num_encounters):
            admit_date = random_date()
            discharge_date = admit_date + timedelta(days=random.randint(0, 30))
            total_cost = max(round(np.random.normal(5000, 2000), 2), 0)

            encounters.append({
                "encounter_id": str(uuid.uuid4()),
                "patient_id": patient["patient_id"],
                "provider_id": providers.sample(1).iloc[0]["provider_id"],
                "admit_date": admit_date,
                "discharge_date": discharge_date,
                "visit_type": random.choice(["inpatient", "outpatient", "emergency"]),
                "total_cost": total_cost
            })

    return pd.DataFrame(encounters)

# ---------------------------
# Diagnoses / Procedures / Claims (unchanged)
# ---------------------------
def generate_diagnoses(encounters):
    diagnoses = []
    for _, enc in encounters.iterrows():
        for _ in range(random.randint(1, 3)):
            diagnoses.append({
                "diagnosis_id": str(uuid.uuid4()),
                "encounter_id": enc["encounter_id"],
                "icd_code": generate_icd_code(),
                "is_primary": random.choice([True, False])
            })
    return pd.DataFrame(diagnoses)

def generate_procedures(encounters):
    procedures = []
    for _, enc in encounters.iterrows():
        for _ in range(random.randint(0, 2)):
            procedures.append({
                "procedure_id": str(uuid.uuid4()),
                "encounter_id": enc["encounter_id"],
                "cpt_code": generate_cpt_code(),
                "procedure_cost": max(round(np.random.normal(1500, 500), 2), 0)
            })
    return pd.DataFrame(procedures)

def generate_claims(encounters):
    claims = []
    for _, enc in encounters.iterrows():
        paid_amount = enc["total_cost"] * random.uniform(0.7, 1.0)
        claims.append({
            "claim_id": str(uuid.uuid4()),
            "encounter_id": enc["encounter_id"],
            "claim_amount": enc["total_cost"],
            "paid_amount": paid_amount,
            "insurance": random.choice(["Aetna", "BlueCross", "United", "Medicare"]),
            "claim_status": random.choice(["paid", "denied", "pending"])
        })
    return pd.DataFrame(claims)

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    print("Generating data...")

    patients = generate_patients(NUM_PATIENTS)
    providers = generate_providers(NUM_PROVIDERS)
    encounters = generate_encounters(patients, providers)
    diagnoses = generate_diagnoses(encounters)
    procedures = generate_procedures(encounters)
    claims = generate_claims(encounters)

    # Ensure directory exists
    os.makedirs(RAW_DIR, exist_ok=True)

    # Save files
    patients.to_csv(os.path.join(RAW_DIR, "patients.csv"), index=False)
    providers.to_csv(os.path.join(RAW_DIR, "providers.csv"), index=False)
    encounters.to_csv(os.path.join(RAW_DIR, "encounters.csv"), index=False)
    diagnoses.to_csv(os.path.join(RAW_DIR, "diagnoses.csv"), index=False)
    procedures.to_csv(os.path.join(RAW_DIR, "procedures.csv"), index=False)
    claims.to_csv(os.path.join(RAW_DIR, "claims.csv"), index=False)

    print(f"Dataset generated successfully at {RAW_DIR}")
