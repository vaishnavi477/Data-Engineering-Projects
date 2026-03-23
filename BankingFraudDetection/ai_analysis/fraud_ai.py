import os
import requests
import psycopg2
import json
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL")

# Connect to Postgres
conn = psycopg2.connect(
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT
)
cur = conn.cursor()

# ----------------------------------------
# Fetch unexplained frauds and call AI
# ----------------------------------------
cur.execute("""
SELECT transaction_id, amount, country
FROM fraud_alerts
WHERE transaction_id NOT IN (SELECT transaction_id FROM fraud_explained)
""")
frauds = cur.fetchall()

for txn in frauds:
    transaction_id, amount, country = txn
    prompt = (
        f"Explain in one short sentence (max 15 words), grammatically correct, "
        f"why this banking transaction might be fraudulent:\n"
        f"Amount: {amount}\nCountry: {country}\n"
        f"Be clear, concise, and professional."
    )

    response = requests.post(
        OLLAMA_API_URL,
        json={"model": OLLAMA_MODEL, "prompt": prompt}
    )

    # parse AI response (same as before)
    explanation = ""
    try:
        raw_text = response.text.strip()
        lines = raw_text.splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if 'response' in obj:
                    explanation += obj['response'].strip() + " "
            except json.JSONDecodeError:
                explanation += line + " "
        import re
        explanation = re.sub(r'\s+', ' ', explanation).strip()
        if explanation:
            explanation = explanation[0].upper() + explanation[1:]
            if not explanation.endswith('.'):
                explanation += '.'
        else:
            explanation = "No explanation generated."
    except Exception as e:
        explanation = f"Failed to parse AI response: {e}"

    # Insert into DB
    cur.execute("""
        INSERT INTO fraud_explained (transaction_id, explanation)
        VALUES (%s, %s)
        ON CONFLICT (transaction_id) DO NOTHING
    """, (transaction_id, explanation))
    conn.commit()
    print(f"Inserted explanation for transaction_id: {transaction_id} -> {explanation}")

cur.close()
conn.close()
