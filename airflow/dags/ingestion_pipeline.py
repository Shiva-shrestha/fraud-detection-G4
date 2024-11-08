from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from datetime import datetime, timedelta
import os
import pandas as pd
import random
import json
import requests


# Go up two directories from the script's location
BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
print(BASE_PATH)
# Define paths for the raw, good, and bad data
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", os.path.join(BASE_PATH, "raw_data"))
GOOD_DATA_PATH = os.getenv("GOOD_DATA_PATH", os.path.join(BASE_PATH, "good_data"))
BAD_DATA_PATH = os.getenv("BAD_DATA_PATH", os.path.join(BASE_PATH, "bad_data"))

# Ensure directories exist
os.makedirs(GOOD_DATA_PATH, exist_ok=True)
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(BAD_DATA_PATH, exist_ok=True)


@dag(
    dag_id="data_ingestion_pipeline",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def data_ingestion():    
    @task
    def read_data() -> pd.DataFrame:
        files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]
        if not files:
            raise AirflowSkipException("No CSV files found in raw data.")
        
        # Choose a random file and load it as a DataFrame
        file = random.choice(files)
        file_path = os.path.join(RAW_DATA_PATH, file)
        
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            raise ValueError(f"Error reading file {file_path}: {str(e)}")
        
        # Log file read and remove it to prevent reprocessing
        print(f"Read and ingested data from {file_path}")
        os.remove(file_path)
        
        return df

    @task
    def validate_data(df: pd.DataFrame) -> dict:
        """
        Perform custom data validation checks and return a report of any issues found.
        """
        validation_report = {"missing_values": {}, "out_of_range": {}}
        
        # Check for missing values in each column
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                validation_report["missing_values"][col] = missing_count

        # Example check for out-of-range values in specific columns (customize as needed)
        if "age" in df.columns:
            out_of_range_age = df[~df["age"].between(0, 120)].index.tolist()
            if out_of_range_age:
                validation_report["out_of_range"]["age"] = out_of_range_age

        if "amt" in df.columns:
            out_of_range_salary = df[df["amt"] < 0].index.tolist()
            if out_of_range_salary:
                validation_report["out_of_range"]["amt"] = out_of_range_salary
        
        # Determine if the validation has any errors
        errors_found = bool(validation_report["missing_values"] or validation_report["out_of_range"])
        
        return {"report": validation_report, "errors_found": errors_found, "data_frame": df}

    @task
    def send_alert(validation_report: dict) -> None:
        """
        Send an alert if there are any validation errors.
        """
        if not validation_report["errors_found"]:
            print("No validation errors found. No alert sent.")
            return

        report = validation_report["report"]
        alert_msg = {
            "title": "Data Validation Report",
            "text": f"Data validation found issues:\n\n{report}"
        }

    @task
    def save_file(validation_report: dict) -> None:
        """
        Segregate data into 'good' and 'bad' based on the validation report and save to respective directories.
        """
        df = validation_report["data_frame"]
        report = validation_report["report"]
        
        # Identify bad rows based on missing values and out-of-range indices
        bad_rows = set()
        
        # Collect indices of rows with missing values
        for col, indices in report["missing_values"].items():
            bad_rows.update(df[df[col].isnull()].index)
        
        # Collect indices of rows with out-of-range values
        for col, indices in report["out_of_range"].items():
            bad_rows.update(indices)
        
        # Separate good and bad data
        #bad_data = df.loc[bad_rows]
        #good_data = df.drop(bad_rows)
        
        print(bad_rows)
        # Save data to respective directories
      #  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
       # good_data.to_csv(os.path.join(GOOD_DATA_PATH, f"good_data_{timestamp}.csv"), index=False)
       # bad_data.to_csv(os.path.join(BAD_DATA_PATH, f"bad_data_{timestamp}.csv"), index=False)

       # print(f"Good data saved to: {GOOD_DATA_PATH}")
       # print(f"Bad data saved to: {BAD_DATA_PATH}")

    @task
    def save_statistics(validation_report: dict) -> None:
        """
        Calculate and save summary statistics based on the validation report.
        """
        report = validation_report["report"]
        
        stats = {
            "total_rows": len(validation_report["data_frame"]),
            "missing_value_columns": len(report["missing_values"]),
            "out_of_range_columns": len(report["out_of_range"]),
            "errors_found": validation_report["errors_found"],
            "timestamp": datetime.now().isoformat(),
        }

        # Log the statistics as JSON
        stats_json = json.dumps(stats, indent=4)
        print(f"Statistics:\n{stats_json}")

        # Save the statistics to a file or a database as required
        with open("data_validation_statistics.json", "w") as f:
            f.write(stats_json)

    # DAG sequence
    data_df = read_data()
    validation_result = validate_data(data_df)
    send_alert(validation_result)
    save_file(validation_result)
    save_statistics(validation_result)

data_ingestion_dag = data_ingestion()
