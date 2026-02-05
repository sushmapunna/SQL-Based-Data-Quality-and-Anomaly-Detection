import sqlite3
import pandas as pd

# 1. Setup Database
conn = sqlite3.connect(':memory:')
df = pd.read_csv('cleaned_receipts.csv')
df.to_sql('receipts', conn, index=False, if_exists='replace')

# 2. Run the FIXED Volume Anomaly Query
# We use a CTE (Common Table Expression) named 'CalculatedData' 
# to calculate the LAG first. Then we filter it.
query = """
WITH DailyCounts AS (
    -- Step 1: Count transactions per day
    SELECT 
        merchant, 
        transaction_date, 
        COUNT(*) as daily_txn_count
    FROM receipts
    WHERE valid = 'Yes'
    GROUP BY merchant, transaction_date
),
CalculatedData AS (
    -- Step 2: Calculate the Previous Day's Count
    SELECT
        merchant,
        transaction_date,
        daily_txn_count,
        LAG(daily_txn_count) OVER (PARTITION BY merchant ORDER BY transaction_date) as prev_day_count
    FROM DailyCounts
)
-- Step 3: Filter for the spikes/drops
SELECT 
    merchant,
    transaction_date,
    daily_txn_count,
    prev_day_count,
    (daily_txn_count - prev_day_count) as volume_change
FROM CalculatedData
WHERE ABS(daily_txn_count - prev_day_count) >= 2;
"""

print("--- DETECTING VOLUME ANOMALIES ---")
try:
    result = pd.read_sql_query(query, conn)
    print(result)
except Exception as e:
    print(f"Error: {e}")