# GCP Banking ETL Pipeline

## **Table of Contents**
1. [Project Overview](#project-overview)
2. [Project Architecture](#project-architecture)
3. [Prerequisites](#prerequisites)
4. [Technologies Used](#technologies-used)
5. [Data Flow](#data-flow)
6. [Input Files](#input-files)
7. [Conclusion](#conclusion)


## Project Overview

This project implements an ETL pipeline to automate the extraction, transformation, and loading (ETL) of banking data into Google BigQuery. The pipeline is built using Apache Airflow and Google Cloud Platform (GCP) services. It ingests synthetic customer and account data from Google Cloud Storage (GCS), performs transformations, and stores the transformed data in BigQuery for further analytics and reporting.

## **Project Architecture**
The architecture consists of three main stages:
1. **Data Ingestion**: Accounts and customer data are uploaded to Google Cloud Storage (GCS).
2. **Data Loading**: Data from GCS is loaded into BigQuery using the `GCSToBigQueryOperator`.
3. **Data Transformation**: Data is joined and transformed using SQL queries in BigQuery, resulting in a combined dataset.

Airflow manages the workflow, scheduling tasks, and monitoring the pipeline execution.

### Architecture :
![Architecture Diagram](https://github.com/malviya1908/banking-data-pipeline/blob/main/architecture/project_3_architecture.png)

## Prerequisites
Before running the project, ensure that you have the following prerequisites set up:
- **Google Cloud Platform (GCP) account**
- **Google Cloud Storage (GCS) bucket**
- **BigQuery datasets** for storing raw and transformed data
- **Airflow** (e.g., Google Cloud Composer)
- **Service account** with required permissions to access GCS, BigQuery, and other GCP resources

## **Technologies Used**
- **Google Cloud Platform (GCP)**: BigQuery, Cloud Storage, Cloud Composer (Airflow)
- **Apache Airflow**: Workflow orchestration tool
- **Python**: For data processing and automation

## **Data Flow**
1. The pipeline first generates synthetic customer and account data using Python scripts.
2. The generated data is uploaded to GCS.
3. Airflow orchestrates the ETL tasks, which involve loading the data into BigQuery and transforming it via SQL queries.
4. The transformed data is stored in a separate dataset in BigQuery.

## **Input Files**
- **Customer Data**: Contains customer information like name, credit score, loan status, etc.
- **Account Data**: Includes details about customer accounts such as account type, opening date, branch, etc.
These files are generated using Python and uploaded to Google Cloud Storage for further processing.
   
## **Conclusion**
This project demonstrates the seamless integration of multiple Google Cloud services with Apache Airflow for managing a data pipeline. The goal is to automate data ingestion, transformation, and loading for banking data, enabling better data analytics and insights.


  


