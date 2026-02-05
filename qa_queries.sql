-- 2. VOLUME ANOMALY DETECTION (Fixed Logic)
-- We use a Subquery to calculate the LAG first, then filter.
SELECT * FROM (
    SELECT 
        merchant,
        transaction_date,
        daily_txn_count,
        LAG(daily_txn_count) OVER (PARTITION BY merchant ORDER BY transaction_date) as prev_day_count,
        (daily_txn_count - LAG(daily_txn_count) OVER (PARTITION BY merchant ORDER BY transaction_date)) as volume_change
    FROM (
        SELECT merchant, transaction_date, COUNT(*) as daily_txn_count
        FROM receipts
        WHERE valid = 'Yes'
        GROUP BY merchant, transaction_date
    ) inner_subquery
) final_result
WHERE ABS(volume_change) >= 2;