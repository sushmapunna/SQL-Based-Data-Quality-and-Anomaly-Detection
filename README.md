# SQL-Based-Data-Quality-and-Anomaly-Detection
Built SQL audits to detect duplicates, missing fields, and invalid transactions, ensuring data integrity. Designed automated merchant-level volume anomaly detection using Window Functions to flag sudden day-over-day spikes and drops.
**Goal:** Programmatically detect duplicate records and suspicious volume trends at the database level.

### 🔹 The Solution (`qa_queries.sql`)
I designed a suite of SQL validation scripts using **Window Functions** to audit data health.

* **Volume Anomaly Detection:** Uses `LAG()` to calculate day-over-day transaction volume per merchant. It automatically flags "Spikes" or "Drops" (e.g., a change of ≥2 orders vs. the previous day), enabling proactive trend monitoring.
* **Duplicate Detection:** Identifies distinct Order IDs that appear multiple times in the dataset.
* **Data Integrity Audit:** Summarizes dataset health by counting NULL values and logic errors (e.g., negative amounts).

---
## 🛠 Tech Stack & Skills
* **Languages:** Python 3.x, SQL.
* **Libraries:** Pandas, NumPy, Re (Regex), SQLite.
* **Concepts:** ETL Pipelines, Data Cleaning, Window Functions (LAG, OVER), Root Cause Analysis (RCA), Anomaly Detection.

---

## 🚀 How to Run This Project

### 1. Setup
Ensure you have Python and Pandas installed:
```bash
pip install pandas
python cleaner.py
python run_sql.py
