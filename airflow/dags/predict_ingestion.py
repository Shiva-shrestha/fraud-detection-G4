from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
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
    'predict_ingestion',
    default_args=default_args,
    description='A pipeline ingestion for the workflow',
    schedule_interval=timedelta(days=1),  # Run once per day
)

# Task 1: Merge data from the good_data folder into a single file
def get_data(folder_path, output_file):
    required_columns = [
        'merchant', 'category', 'amt', 'gender', 'lat', 'long', 
        'city_pop', 'job', 'unix_time', 'merch_lat', 'merch_long'
    ]
    dfs = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path)
            # Ensure only required columns are kept
            df = df[required_columns]
            dfs.append(df)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        print(f"Data combined and saved to {output_file}")
    else:
        print("No CSV files found to combine.")

# Task 2: Send the combined data for prediction
def ingestion_predict(uploaded_file):
    API_URL = "http://localhost:5433/"
    with open(uploaded_file, 'rb') as f:
        response = requests.post(f"{API_URL}/predict-file/", files={"file": f})

    if response.status_code == 200:
        result = response.json()
        df = pd.DataFrame(result["predicted_data"])
        fraud_count = df[df['is_fraud'] == 1].shape[0]
        print(f"Number of fraud cases: {fraud_count}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

# Define PythonOperators for each task

# Task to merge data
merge_data_task = PythonOperator(
    task_id='check_for_new_data',
    python_callable=get_data,
    op_kwargs={
        'folder_path': 'data/good_data',  # Define your folder path here
        'output_file': 'combined_data.csv'
    },
    dag=dag,
)

# Task to predict fraud from the merged file
predict_fraud_task = PythonOperator(
    task_id='make_prediction',
    python_callable=ingestion_predict,
    op_kwargs={'uploaded_file': 'combined_data.csv'},
    dag=dag,
)

# Set the task dependencies
merge_data_task >> predict_fraud_task
