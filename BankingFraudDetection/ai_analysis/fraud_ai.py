import requests
import psycopg2
import json

# Connect to Postgres
conn = psycopg2.connect(
    dbname="banking",
    user="nikitapatel",
    password="Akash29",
    host="localhost"
)
cur = conn.cursor()

# ----------------------------------------
# 1️⃣ Ensure tables exist
# ----------------------------------------
cur.execute("""
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    transaction_id INT,
    user_id INT,
    amount INT,
    country VARCHAR(10),
    risk_level VARCHAR(20),
    timestamp TIMESTAMP
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS fraud_explained (
    id SERIAL PRIMARY KEY,
    transaction_id INT UNIQUE,
    explanation TEXT
)
""")
conn.commit()

# ----------------------------------------
# 2️⃣ Fetch unexplained frauds
# ----------------------------------------
cur.execute("""
SELECT transaction_id, amount, country
FROM fraud_alerts
WHERE transaction_id NOT IN (SELECT transaction_id FROM fraud_explained)
""")
frauds = cur.fetchall()

# ----------------------------------------
# 3️⃣ Call AI for concise explanation
# ----------------------------------------
for txn in frauds:
    transaction_id, amount, country = txn

    prompt = (
        f"Explain in 10 words or less why this banking transaction might be fraudulent:\n"
        f"Amount: {amount}\nCountry: {country}\n"
        f"Be concise and precise."
    )

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": "llama3", "prompt": prompt}
    )

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
        # Clean whitespace and punctuation
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
