import pandas as pd
import duckdb   # !pip install duckdb -q

# Upload sales.csv, customers.csv, inventory.csv to the Colab file panel first,
# or mount Drive / adjust paths below.

SALES     = "sales.csv"
CUSTOMERS = "customers.csv"
INVENTORY = "inventory.csv"


# ============================================================
# PART 1 — ETL (transform BEFORE loading)
# ============================================================

# Extract: read all 3 source files
sales_raw     = pd.read_csv(SALES)
customers_raw = pd.read_csv(CUSTOMERS)
inventory_raw = pd.read_csv(INVENTORY)

# Transform in pandas: clean sales, then enrich with customer + product info
sales_clean = sales_raw.replace("", pd.NA).dropna(subset=["customer_id"])
sales_clean = sales_clean.drop_duplicates(subset=["order_id"])
sales_clean = sales_clean[sales_clean["revenue"] >= 0]
sales_clean = sales_clean[pd.to_datetime(sales_clean["order_date"]) <= pd.Timestamp.now()]

# Join customers → get customer name and region
sales_clean = sales_clean.merge(customers_raw, on="customer_id", how="left")

# Join inventory → get product name and category
sales_clean = sales_clean.merge(inventory_raw, on="product_id", how="left")

print(f"ETL: raw rows = {len(sales_raw)}, enriched clean rows = {len(sales_clean)}")
print("Columns:", sales_clean.columns.tolist())

# Load the already-clean, enriched data into the warehouse
con = duckdb.connect(r"C:\Users\USER\Downloads\ETL_ELT\warehouse\warehouse.duckdb")
con.execute("CREATE OR REPLACE TABLE sales_etl AS SELECT * FROM sales_clean")
print(con.execute("SELECT * FROM sales_etl LIMIT 5").df())


# ============================================================
# PART 2 — ELT (load raw first, transform INSIDE the warehouse)
# ============================================================

# Load: dump all 3 raw tables into the warehouse as-is — no cleaning yet
con.execute(f"CREATE OR REPLACE TABLE sales_raw     AS SELECT * FROM read_csv_auto('{SALES}')")
con.execute(f"CREATE OR REPLACE TABLE customers_raw AS SELECT * FROM read_csv_auto('{CUSTOMERS}')")
con.execute(f"CREATE OR REPLACE TABLE inventory_raw AS SELECT * FROM read_csv_auto('{INVENTORY}')")

print(f"ELT: loaded {con.execute('SELECT COUNT(*) FROM sales_raw').fetchone()[0]} raw sales rows")
print(f"ELT: loaded {con.execute('SELECT COUNT(*) FROM customers_raw').fetchone()[0]} raw customer rows")
print(f"ELT: loaded {con.execute('SELECT COUNT(*) FROM inventory_raw').fetchone()[0]} raw inventory rows")

# Transform: clean + join all 3 tables INSIDE the warehouse using SQL
con.execute("""
    CREATE OR REPLACE TABLE sales_elt AS
    SELECT
        s.order_id,
        s.order_date::DATE      AS order_date,
        s.revenue,
        c.customer_id,
        c.name                  AS customer_name,
        c.region,
        i.product_id,
        i.product_name,
        i.category
    FROM sales_raw s
    LEFT JOIN customers_raw c ON s.customer_id = c.customer_id
    LEFT JOIN inventory_raw i ON s.product_id  = i.product_id
    WHERE s.customer_id IS NOT NULL AND s.customer_id != ''
      AND s.revenue >= 0
      AND s.order_date <= CURRENT_DATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.order_date) = 1
""")

print(con.execute("SELECT COUNT(*) FROM sales_elt").fetchone()[0], "enriched clean rows after in-warehouse transform")
print(con.execute("SELECT * FROM sales_elt LIMIT 5").df())


# PART 3 — Incremental load (full load vs. updated_at watermark)

# order_date as the watermark column.

# --- Full load: reload everything, every time ---
con.execute("CREATE OR REPLACE TABLE sales_full_load AS SELECT * FROM sales_raw")
full_count = con.execute("SELECT COUNT(*) FROM sales_full_load").fetchone()[0]
print(f"Full load: {full_count} rows reloaded (every run, regardless of what changed)")

# --- Incremental load: only rows newer than the last successful run ---
last_run_timestamp = "2026-07-14"  # pretend this was stored somewhere after the last run

con.execute(f"""
    CREATE OR REPLACE TABLE sales_incremental AS
    SELECT * FROM sales_raw
    WHERE order_date > '{last_run_timestamp}'
""")
incr_count = con.execute("SELECT COUNT(*) FROM sales_incremental").fetchone()[0]
print(f"Incremental load: only {incr_count} new rows since {last_run_timestamp}")

con.close()


# PART 4 — Error handling (transient vs. permanent failure)



from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import random

class TransientAPIError(Exception):
    pass

class PermanentSchemaError(Exception):
    pass

@retry(
    retry=retry_if_exception_type(TransientAPIError),
    stop=stop_after_attempt(4),
    wait=wait_fixed(2),
    reraise=True,
)
def call_pos_api():
    roll = random.random()
    if roll < 0.6:
        print("POS API timeout — transient, retrying...")
        raise TransientAPIError("timeout")
    print("POS API responded successfully.")
    return {"status": "ok"}

def extract_inventory_feed():
    # Simulate a permanent failure: upstream renamed a required column
    raise PermanentSchemaError("Expected column 'sku' not found in inventory feed")

# Transient — will retry and (usually) succeed within 4 attempts
call_pos_api()

# Permanent — should NOT be retried, should surface immediately
try:
    extract_inventory_feed()
except PermanentSchemaError as e:
    print(f"ALERT: permanent failure, no retry — {e}")


