
# import all modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# variables section
project_id = "synthetic-nova-438808-k6"
bucket_name = "dataproc_bucket_prac"
location = "asia-south1"
RAW_DATASET_NAME = "synthetic-nova-438808-k6.Raw_Dataset"
TRANSFOMED_DATASET_NAME = "synthetic-nova-438808-k6.Transformed_dataset"
TABLE_NAME_1 = "accounts"
TABLE_NAME_2 = "customer"
TABLE_NAME_3 = "cust_acc"

# Get today's date in the required format (YYYY-MM-DD)
today_date = datetime.today().strftime('%Y-%m-%d')

query = f"""
    SELECT a.Account_ID, a.Customer_ID, a.Account_Type, a.Opening_Date, a.Branch, a.Account_Status,
              c.Name, c.Credit_Score, c.Loan_Status, c.Date_of_Birth, c.Date_Joined, c.Address, c.Phone_Number, c.Email
    FROM {RAW_DATASET_NAME}.{TABLE_NAME_1} a
    JOIN {RAW_DATASET_NAME}.{TABLE_NAME_2} c
    ON a.Customer_ID = c.Customer_ID  
"""

args = {
    "owner": "Rahul",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10)
}


# define the dag
with DAG (
    dag_id='gcs_to_bigquery_pipeline',
    schedule_interval='0 5 * * *',
    description='Dag to load data from gcs to bigquery',
    default_args=args,
    tags = ["gcs", "bigquery", "banking", "gcs_to_bigquery"]
) as dag:

# define the tasks
    # Task 1 : Load the accounts data from gcs to bigquery
    load_accounts_csv = GCSToBigQueryOperator(
        task_id="load_accounts_csv",
        bucket=bucket_name,
        source_objects=[f"bank_data/accounts_data/{today_date}/accounts.csv"],
        destination_project_dataset_table=f"{RAW_DATASET_NAME}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "Account_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Customer_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Account_Type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Opening_Date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "Branch", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Account_Status", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_APPEND"
    )

    # Task 2 : Load the customer data from gcs to bigquery  				
    load_customer_csv = GCSToBigQueryOperator(
        task_id="load_customer_csv",
        bucket=bucket_name,
        source_objects=[f"bank_data/customer_data/{today_date}/customers.csv"],
        destination_project_dataset_table=f"{RAW_DATASET_NAME}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "Customer_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Credit_Score", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Loan_Status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Date_of_Birth", "type": "DATE", "mode": "NULLABLE"},
            {"name": "Date_Joined", "type": "DATE", "mode": "NULLABLE"},
            {"name": "Address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Phone_Number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_APPEND"
    )
						
    # Task 3 : Insert the query job to join the accounts and customer data
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": TRANSFOMED_DATASET_NAME.split(".")[1],
                    "tableId": TABLE_NAME_3,
                },
                "writeDisposition": "WRITE_APPEND",
            }
        },
        location=location
    )

# define the dependencies
(load_accounts_csv, load_customer_csv) >> insert_query_job