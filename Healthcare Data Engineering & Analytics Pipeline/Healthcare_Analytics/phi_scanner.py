import pandas as pd
import re

def detect_ssn(val):
    return bool(re.match(r"\d{3}-\d{2}-\d{4}", str(val)))

def scan_for_phi(file):
    df = pd.read_csv(file)

    for col in df.columns:
        if df[col].astype(str).apply(detect_ssn).any():
            print(f"🚨 Potential SSN detected in column: {col}")

scan_for_phi("data/deidentified/patients_deid.csv")
