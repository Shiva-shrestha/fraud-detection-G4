from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import csv
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 20),  # Adjust start date as needed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Credit Card Transaction Fraud Detection',
    default_args=default_args,
    description='A pipepline injestion for the work flow',
    schedule_interval=timedelta(days=1),  # Run once per day
)

# Define your functions (tasks)

# 1. Create directory task
def create_directory(directory_name):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)
    else:
        print(f"Directory '{directory_name}' already exists.")

# 2. Split the source CSV into smaller files
def split_csv(input_file, output_prefix, rows_per_file):
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Read the header row
        output_file = None
        row_count = 0
        file_count = 1

        for row in reader:
            if row_count == 0:
                output_file = open(os.path.join("good_data", f"{output_prefix}_{file_count}.csv"), 'w', newline='')
                writer = csv.writer(output_file)
                writer.writerow(header)
            writer.writerow(row)
            row_count += 1
            if row_count == rows_per_file:
                output_file.close()
                row_count = 0
                file_count += 1

        if output_file:
            output_file.close()

# 3. Merge data from the good_data folder into a single file
def get_data(folder_path, output_file):
    required_columns = ['merchant', 'category', 'amt', 'gender', 'lat', 'long', 'city_pop', 'job', 'unix_time', 'merch_lat', 'merch_long']
    dfs = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path)
            dfs.append(df)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(output_file, index=False)
    else:
        print("No files to combine.")

# 4. Send the combined data for prediction
def ingestion_predict(uploaded_file):
    API_URL = "http://127.0.0.1:8000"
    with open(uploaded_file, 'rb') as f:
        response = requests.post(f"{API_URL}/predict-file/", files={"file": f})

    if response.status_code == 200:
        result = response.json()
        df = pd.DataFrame(result["predicted_data"])
        fraud_count = df[df['is_fraud'] == 1].shape[0]
        print(f"Number of fraud cases: {fraud_count}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

# Task definitions

# Task 1: Create directories
create_good_data_dir = PythonOperator(
    task_id='create_good_data_directory',
    python_callable=create_directory,
    op_kwargs={'directory_name': 'good_data'},
    dag=dag,
)

create_bad_data_dir = PythonOperator(
    task_id='create_bad_data_directory',
    python_callable=create_directory,
    op_kwargs={'directory_name': 'bad_data'},
    dag=dag,
)

# Task 2: Split the source CSV
split_csv_task = PythonOperator(
    task_id='split_csv',
    python_callable=split_csv,
    op_kwargs={
        'input_file': '../data/testData.csv',
        'output_prefix': 'split_data',
        'rows_per_file': 1000
    },
    dag=dag,
)

# Task 3: Merge data
merge_data_task = PythonOperator(
    task_id='merge_data',
    python_callable=get_data,
    op_kwargs={
        'folder_path': 'good_data',
        'output_file': 'combined_data.csv',
    },
    dag=dag,
)

# Task 4: Predict fraud from the merged file
predict_fraud_task = PythonOperator(
    task_id='predict_fraud',
    python_callable=ingestion_predict,
    op_kwargs={'uploaded_file': 'combined_data.csv'},
    dag=dag,
)

# Task dependencies: Set the execution order
create_good_data_dir >> create_bad_data_dir >> split_csv_task >> merge_data_task >> predict_fraud_task
