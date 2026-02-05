import pandas as pd
import re

# ---------------------------
# 1. Load raw data
# ---------------------------
df = pd.read_csv('raw_data.csv', sep='|', header=0, names=['raw_text'])



# ---------------------------
# 2. Regex patterns (permissive & robust)
# ---------------------------
patterns = {
    'amazon': r'Order #([0-9-]+).*?Rs\.?\s*([0-9,]+).*?on\s+([0-9A-Za-z-]+)',
    'flipkart': r'id\s*([A-Z0-9-]+).*?sent\s*([0-9]+).*?Date:\s*([0-9/]+)',
    'swiggy': r'order\s*([0-9]+).*?amount\s*([-]?[0-9]+).*?completed\s*([0-9A-Za-z-]+)',
    'uber': r'Trip on\s+(.*?)\s+cost\s+([0-9]+)'
}

# ---------------------------
# 3. Vendor normalization
# ---------------------------
vendor_map = {
    'Amazon.in': 'Amazon',
    'Flipkart India': 'Flipkart'
}

# ---------------------------
# 4. Parsing function
# ---------------------------
def parse_receipt(text):
    text_lower = text.lower()

    for vendor, pattern in patterns.items():
        if vendor in text_lower:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if vendor == 'uber':
                    # Uber has no order_id, date comes first
                    return (
                        'Uber',
                        None,
                        match.group(2),
                        match.group(1).strip() + ' 2025'
                    )
                else:
                    return (
                        vendor.capitalize(),
                        match.group(1),
                        match.group(2),
                        match.group(3)
                    )

    return 'Unknown', None, None, None

# ---------------------------
# 5. Apply extraction
# ---------------------------
df[['merchant', 'order_id', 'amount', 'transaction_date']] = df['raw_text'].apply(
    lambda x: pd.Series(parse_receipt(x))
)

# ---------------------------
# 6. Normalize merchant names
# ---------------------------
df['merchant'] = df['merchant'].map(vendor_map).fillna(df['merchant'])

# ---------------------------
# 7. Clean amount column
# ---------------------------
df['amount'] = (
    df['amount']
    .astype(str)
    .str.replace(',', '', regex=True)
)

df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

# ---------------------------
# 8. Clean & standardize dates
# ---------------------------
df['transaction_date'] = df['transaction_date'].str.replace('/', '-', regex=False)

df['transaction_date'] = pd.to_datetime(
    df['transaction_date'],
    errors='coerce',
    dayfirst=True
)

df['transaction_date'] = df['transaction_date'].dt.strftime('%Y-%m-%d')

# ---------------------------
# 9. QA logic + failure reason
# ---------------------------
def qa_reason(row):
    if pd.isna(row['transaction_date']):
        return 'Invalid Date'
    if pd.isna(row['amount']):
        return 'Missing Amount'
    if row['amount'] <= 0:
        return 'Negative Amount'
    if row['amount'] >= 10000:
        return 'Amount Too High'
    if row['merchant'] == 'Unknown':
        return 'Unknown Merchant'
    return 'OK'

df['qa_reason'] = df.apply(qa_reason, axis=1)

df['valid'] = df['qa_reason'].apply(lambda x: 'Yes' if x == 'OK' else 'No')

# ---------------------------
# 10. Save outputs
# ---------------------------
final_df = df.drop(columns=['raw_text'])

final_df.to_csv('cleaned_receipts.csv', index=False)
final_df[final_df['valid'] == 'No'].to_csv('qa_report.csv', index=False)

print("\n--- FINAL CLEANED DATA ---")
print(final_df.to_string())
