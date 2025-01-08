import pandas as pd
import random
import uuid
import os
from faker import Faker
from datetime import datetime, timedelta
from google.cloud import storage
import io

# Initialize Faker
fake = Faker("en_IN")

# Helper function to generate random dates
def random_date(start, end):
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

# Helper function to generate random 10-digit phone number starting with +91
def generate_phone_number():
    return "+91 " + ''.join([random.choice('0123456789') for _ in range(10)])

# Helper function to generate email based on name and domain
def generate_email(name):
    name_parts = name.lower().split()
    email_prefix = "".join(name_parts[:2])  # First two parts of the name
    domains = ["gmail.com", "yahoo.com", "outlook.com", "rediffmail.com", "hotmail.com"]
    domain = random.choice(domains)
    return f"{email_prefix}{random.randint(1, 9999)}@{domain}"

# Generate Customers Data
def generate_customers(num_customers):
    customers = []
    for i in range(num_customers):
        customer_id = f"CUST-{i+1:04d}"
        name = fake.name()
        credit_score = random.randint(300, 900)  # Indian credit scores typically range between 300-900
        loan_status = random.choice(["Approved", "Pending", "Rejected", "Not Applied"])
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=70)
        date_joined = random_date(datetime(2015, 1, 1), datetime(2024, 12, 31))
        address = fake.address().replace("\n", ", ")
        phone_number = generate_phone_number()  # Generate phone number with +91
        email = generate_email(name)  # Generate email based on name
        
        customers.append((customer_id, name, credit_score, loan_status, date_of_birth, date_joined, address, phone_number, email))
    return pd.DataFrame(customers, columns=["Customer_ID", "Name", "Credit_Score", "Loan_Status", "Date_of_Birth", "Date_Joined", "Address", "Phone_Number", "Email"])

# Generate Accounts Data (without Balance column)
def generate_accounts(customers):
    accounts = []
    for _, customer in customers.iterrows():
        account_id = f"ACC-{uuid.uuid4().hex[:8].upper()}"  # Generate unique account ID
        account_type = random.choice(["Savings", "Current", "Fixed Deposit", "Salary Account", "Recurring deposit account"])
        opening_date = random_date(datetime(2010, 1, 1), datetime(2024, 12, 31))
        branch = random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad"])
        account_status = random.choice(["Active", "Inactive", "Closed"])
        accounts.append((account_id, customer["Customer_ID"], account_type, opening_date.strftime("%Y-%m-%d"), branch, account_status))
    return pd.DataFrame(accounts, columns=["Account_ID", "Customer_ID", "Account_Type", "Opening_Date", "Branch", "Account_Status"])

# Generate the Data
num_customers = 50  # Number of customers

# Generate relational tables
customers_df = generate_customers(num_customers)
accounts_df = generate_accounts(customers_df)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"path_to_secrets_file\secrets_key_file_5b8705f92087.json"

# Set GCS bucket and folder paths
bucket_name = 'your_bucket_name'
base_folder = 'bank_data'

# Get today's date in the required format (YYYY-MM-DD)
today_date = datetime.today().strftime('%Y-%m-%d')

# Create a storage client
storage_client = storage.Client()

# Define paths for customer and account data
customer_data_path = f"{base_folder}/customer_data/{today_date}/customers.csv"
accounts_data_path = f"{base_folder}/accounts_data/{today_date}/accounts.csv"

# Function to upload dataframe to GCS without saving it locally
def upload_df_to_gcs(df, bucket_name, destination_blob_name):
    """Upload DataFrame as CSV to Google Cloud Storage without saving locally."""
    # Convert DataFrame to CSV string in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload the CSV string to GCS
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(csv_buffer, content_type='text/csv')

    print(f"File uploaded to {destination_blob_name}.")

# Upload customer and accounts DataFrames directly to GCS
upload_df_to_gcs(customers_df, bucket_name, customer_data_path)
upload_df_to_gcs(accounts_df, bucket_name, accounts_data_path)
