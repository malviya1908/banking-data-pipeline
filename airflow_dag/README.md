# Airflow DAG for GCS to BigQuery Pipeline
This Airflow DAG is designed to load data from Google Cloud Storage (GCS) to Google BigQuery and perform a join operation on two datasets. It transfers data related to banking, specifically accounts and customers, and stores the transformed result in a third table.

## Overview

The DAG performs the following steps:

1. **Load Accounts Data**: Loads the `accounts.csv` file from a specific folder in GCS into the `Raw_Dataset` dataset in BigQuery, specifically into the `accounts` table.
2. **Load Customer Data**: Loads the `customers.csv` file from a specific folder in GCS into the `Raw_Dataset` dataset in BigQuery, specifically into the `customer` table.
3. **Transform Data**: Joins the `accounts` and `customer` tables and inserts the result into a new table, `cust_acc`, within the `Transformed_dataset` dataset in BigQuery.

## DAG Details
- **DAG ID**: `gcs_to_bigquery_pipeline`
- **Schedule Interval**: `0 5 * * *` (Runs daily at 5:00 AM)
- **Start Date**: 1 day ago (`days_ago(1)`)
- **Owner**: Rahul
- **Tags**: `gcs`, `bigquery`, `banking`, `gcs_to_bigquery`


## Variables
- `project_id`: Google Cloud Project ID (`synthetic-nova-438808-k6`)
- `bucket_name`: GCS Bucket Name (`dataproc_bucket_prac`)
- `location`: GCP region for BigQuery operations (`asia-south1`)
- `RAW_DATASET_NAME`: BigQuery dataset for raw data (`synthetic-nova-438808-k6.Raw_Dataset`)
- `TRANSFOMED_DATASET_NAME`: BigQuery dataset for transformed data (`synthetic-nova-438808-k6.Transformed_dataset`)
- `TABLE_NAME_1`: Name of the accounts table (`accounts`)
- `TABLE_NAME_2`: Name of the customer table (`customer`)
- `TABLE_NAME_3`: Name of the transformed table (`cust_acc`)

## Steps Performed in the DAG
### 1. Load Accounts Data from GCS to BigQuery

The `GCSToBigQueryOperator` is used to load `accounts.csv` from the GCS bucket into the `accounts` table in BigQuery.

- **Source Path**: `bank_data/accounts_data/{today_date}/accounts.csv`
- **BigQuery Table**: `synthetic-nova-438808-k6.Raw_Dataset.accounts`
- **Schema Fields**:
    - Account_ID (STRING)
    - Customer_ID (STRING)
    - Account_Type (STRING)
    - Opening_Date (DATE)
    - Branch (STRING)
    - Account_Status (STRING)
 
### 2. Load Customer Data from GCS to BigQuery
The `GCSToBigQueryOperator` is used to load `customers.csv` from the GCS bucket into the `customer` table in BigQuery.

- **Source Path**: `bank_data/customer_data/{today_date}/customers.csv`
- **BigQuery Table**: `synthetic-nova-438808-k6.Raw_Dataset.customer`
- **Schema Fields**:
    - Customer_ID (STRING)
    - Name (STRING)
    - Credit_Score (INT64)
    - Loan_Status (STRING)
    - Date_of_Birth (DATE)
    - Date_Joined (DATE)
    - Address (STRING)
    - Phone_Number (STRING)
    - Email (STRING)
 

### 3. Join Accounts and Customers Data
The `BigQueryInsertJobOperator` is used to perform an SQL query to join the `accounts` and `customer` tables and store the results in the `cust_acc` table within the `Transformed_dataset` dataset.

- **SQL Query**:
    ```sql
    SELECT a.Account_ID, a.Customer_ID, a.Account_Type, a.Opening_Date, a.Branch, a.Account_Status,
           c.Name, c.Credit_Score, c.Loan_Status, c.Date_of_Birth, c.Date_Joined, c.Address, c.Phone_Number, c.Email
    FROM synthetic-nova-438808-k6.Raw_Dataset.accounts a
    JOIN synthetic-nova-438808-k6.Raw_Dataset.customer c
    ON a.Customer_ID = c.Customer_ID
    ```
- **BigQuery Table**: `synthetic-nova-438808-k6.Transformed_dataset.cust_acc`

## Task Dependencies
The DAG ensures the following sequence of task execution:

1. **Task 1**: `load_accounts_csv` (loads the accounts data)
2. **Task 2**: `load_customer_csv` (loads the customer data)
3. **Task 3**: `insert_query_job` (joins the accounts and customer data)

Tasks `load_accounts_csv` and `load_customer_csv` run concurrently, and once both are successful, the query job (`insert_query_job`) is executed to insert the transformed data.


## Notes
- The DAG is configured to append data to BigQuery tables, so repeated runs will not overwrite existing data.
- The GCS paths are dynamically constructed based on the current date (`today_date`), which is calculated each time the DAG runs.

## Conclusion
This Airflow DAG automates the process of transferring and transforming banking data from GCS to BigQuery, allowing for seamless data processing and integration within Google Cloud services.
  

