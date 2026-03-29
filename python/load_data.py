import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ── 1. READ FILE ───────────────────────────────────────────
print("Reading file...")
df = pd.read_csv(
    r'C:\Users\Asus\Downloads\online_retail_II.csv',
    dtype=str,
    encoding='latin1'
)

print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print(df.head(3))

# ── 2. CLEAN ───────────────────────────────────────────────
print("\nCleaning data...")
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
df = df.dropna(subset=['customer_id'])
df = df[df['quantity'].str.strip() != '']
df = df[~df['invoice'].str.startswith('C')]

print("Clean shape:", df.shape)

# ── 3. LOAD INTO POSTGRESQL ────────────────────────────────
print("\nConnecting to PostgreSQL...")
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce_db",
    user="postgres",
    password="root12345"
)
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS transactions;")
cur.execute("""
    CREATE TABLE transactions (
        invoice       VARCHAR(20),
        stock_code    VARCHAR(20),
        description   TEXT,
        quantity      INT,
        invoice_date  VARCHAR(30),
        price         NUMERIC(10,2),
        customer_id   VARCHAR(20),
        country       VARCHAR(50)
    );
""")

print("Loading data into PostgreSQL...")

# rename customer id column explicitly
df = df.rename(columns={'customer_id': 'customer_id', 'stock_code': 'stock_code'})
print("Final columns:", df.columns.tolist())

rows = [
    (
        row['invoice'],
        row['stockcode'],
        row['description'],
        int(float(row['quantity'])),
        row['invoicedate'],
        float(row['price']),
        row['customer_id'],
        row['country']
    )
    for _, row in df.iterrows()
]

execute_values(cur, """
    INSERT INTO transactions
    (invoice, stock_code, description, quantity, invoice_date, price, customer_id, country)
    VALUES %s
""", rows)

conn.commit()
cur.close()
conn.close()

print("Done! Rows loaded:", len(rows))