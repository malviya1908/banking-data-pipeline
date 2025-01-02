{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "502b2870-cd8a-4cda-b658-eb18a4f79415",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import random\n",
    "import uuid\n",
    "import os\n",
    "from faker import Faker\n",
    "from datetime import datetime, timedelta\n",
    "from google.cloud import storage\n",
    "import io\n",
    "\n",
    "# Initialize Faker\n",
    "fake = Faker(\"en_IN\")\n",
    "\n",
    "# Helper function to generate random dates\n",
    "def random_date(start, end):\n",
    "    delta = end - start\n",
    "    random_days = random.randint(0, delta.days)\n",
    "    return start + timedelta(days=random_days)\n",
    "\n",
    "# Helper function to generate random 10-digit phone number starting with +91\n",
    "def generate_phone_number():\n",
    "    return \"+91 \" + ''.join([random.choice('0123456789') for _ in range(10)])\n",
    "\n",
    "# Helper function to generate email based on name and domain\n",
    "def generate_email(name):\n",
    "    name_parts = name.lower().split()\n",
    "    email_prefix = \"\".join(name_parts[:2])  # First two parts of the name\n",
    "    domains = [\"gmail.com\", \"yahoo.com\", \"outlook.com\", \"rediffmail.com\", \"hotmail.com\"]\n",
    "    domain = random.choice(domains)\n",
    "    return f\"{email_prefix}{random.randint(1, 9999)}@{domain}\"\n",
    "\n",
    "# Generate Customers Data\n",
    "def generate_customers(num_customers):\n",
    "    customers = []\n",
    "    for i in range(num_customers):\n",
    "        customer_id = f\"CUST-{i+1:04d}\"\n",
    "        name = fake.name()\n",
    "        credit_score = random.randint(300, 900)  # Indian credit scores typically range between 300-900\n",
    "        loan_status = random.choice([\"Approved\", \"Pending\", \"Rejected\", \"Not Applied\"])\n",
    "        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=70)\n",
    "        date_joined = random_date(datetime(2015, 1, 1), datetime(2024, 12, 31))\n",
    "        address = fake.address().replace(\"\\n\", \", \")\n",
    "        phone_number = generate_phone_number()  # Generate phone number with +91\n",
    "        email = generate_email(name)  # Generate email based on name\n",
    "        \n",
    "        customers.append((customer_id, name, credit_score, loan_status, date_of_birth, date_joined, address, phone_number, email))\n",
    "    return pd.DataFrame(customers, columns=[\"Customer_ID\", \"Name\", \"Credit_Score\", \"Loan_Status\", \"Date_of_Birth\", \"Date_Joined\", \"Address\", \"Phone_Number\", \"Email\"])\n",
    "\n",
    "# Generate Accounts Data (without Balance column)\n",
    "def generate_accounts(customers):\n",
    "    accounts = []\n",
    "    for _, customer in customers.iterrows():\n",
    "        account_id = f\"ACC-{uuid.uuid4().hex[:8].upper()}\"  # Generate unique account ID\n",
    "        account_type = random.choice([\"Savings\", \"Current\", \"Fixed Deposit\", \"Salary Account\", \"Recurring deposit account\"])\n",
    "        opening_date = random_date(datetime(2010, 1, 1), datetime(2024, 12, 31))\n",
    "        branch = random.choice([\"Mumbai\", \"Delhi\", \"Bangalore\", \"Chennai\", \"Kolkata\", \"Hyderabad\"])\n",
    "        account_status = random.choice([\"Active\", \"Inactive\", \"Closed\"])\n",
    "        accounts.append((account_id, customer[\"Customer_ID\"], account_type, opening_date.strftime(\"%Y-%m-%d\"), branch, account_status))\n",
    "    return pd.DataFrame(accounts, columns=[\"Account_ID\", \"Customer_ID\", \"Account_Type\", \"Opening_Date\", \"Branch\", \"Account_Status\"])\n",
    "\n",
    "\n",
    "# Generate the Data\n",
    "num_customers = 50  # Number of customers\n",
    "\n",
    "# Generate relational tables\n",
    "customers_df = generate_customers(num_customers)\n",
    "accounts_df = generate_accounts(customers_df)\n",
    "\n",
    "# Set the path to your service account key file\n",
    "os.environ[\"GOOGLE_APPLICATION_CREDENTIALS\"] = r\"D:\\GCP Project Udemy\\Projects Notes for Practise and GIT\\synthetic-nova-438808-k6-5b8705f92087.json\"\n",
    "\n",
    "# Set GCS bucket and folder paths\n",
    "bucket_name = 'dataproc_bucket_prac'\n",
    "base_folder = 'bank_data'\n",
    "\n",
    "# Get today's date in the required format (YYYY-MM-DD)\n",
    "today_date = datetime.today().strftime('%Y-%m-%d')\n",
    "\n",
    "\n",
    "# Create a storage client\n",
    "storage_client = storage.Client()\n",
    "\n",
    "# Define paths for customer and account data\n",
    "customer_data_path = f\"{base_folder}/customer_data/{today_date}/customers.csv\"\n",
    "accounts_data_path = f\"{base_folder}/accounts_data/{today_date}/accounts.csv\"\n",
    "\n",
    "\n",
    "# Function to upload dataframe to GCS without saving it locally\n",
    "def upload_df_to_gcs(df, bucket_name, destination_blob_name):\n",
    "    \"\"\"Upload DataFrame as CSV to Google Cloud Storage without saving locally.\"\"\"\n",
    "    # Convert DataFrame to CSV string in memory\n",
    "    csv_buffer = io.StringIO()\n",
    "    df.to_csv(csv_buffer, index=False)\n",
    "    csv_buffer.seek(0)\n",
    "\n",
    "    # Upload the CSV string to GCS\n",
    "    bucket = storage_client.get_bucket(bucket_name)\n",
    "    blob = bucket.blob(destination_blob_name)\n",
    "    blob.upload_from_file(csv_buffer, content_type='text/csv')\n",
    "\n",
    "    print(f\"File uploaded to {destination_blob_name}.\")\n",
    "\n",
    "# Upload customer and accounts DataFrames directly to GCS\n",
    "upload_df_to_gcs(customers_df, bucket_name, customer_data_path)\n",
    "upload_df_to_gcs(accounts_df, bucket_name, accounts_data_path)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}