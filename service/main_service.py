from fastapi import FastAPI, HTTPException, UploadFile, File, Query
import pandas as pd
import psycopg2
import joblib
from pydantic import BaseModel
import constant
import sys
from datetime import datetime
import datetime as dt
from psycopg2.sql import SQL, Literal

# Add the path to your folder
sys.path.append("../core/")
# Initialize FastAPI app
app = FastAPI()

# Load the model
model = joblib.load('../model/model.joblib')

# Database connection parameters
conn_params = {
    "host": "localhost",
    "port": "5432",
    "database": "dsp_project",
    "user": "postgres",
    "password": "root"
}

# Define the data model for user inputs
class Transaction(BaseModel):
    merchant: str
    category: str
    amt: float
    gender: str
    lat: float
    long: float
    city_pop: int
    job: str
    unix_time: int
    merch_lat: float
    merch_long: float
    data_source: str

# Define a helper function to insert data into the database
def insert_data_to_db(data: pd.DataFrame):
    try:
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        insert_query = '''
            INSERT INTO predict_table (merchant, category, amt, gender, lat, long, city_pop, job, unix_time, merch_lat, merch_long,trans_date_trans_time, is_fraud, data_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''
        for _, row in data.iterrows():
            cursor.execute(insert_query, (
                row['merchant'], row['category'], row['amt'], row['gender'],
                row['lat'], row['long'], row['city_pop'], row['job'],
                row['unix_time'], row['merch_lat'], row['merch_long'], row['trans_date_trans_time'], row['is_fraud'], row['data_source'],
            ))
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database insertion failed")

# Helper function to get data from the database based on date range (date only)
def get_predicted_data_by_date(from_date: datetime, to_date: datetime):
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # SQL query to retrieve data where only the date part of trans_date_trans_time is compared
        query = '''
            SELECT * FROM predict_table 
            WHERE trans_date_trans_time::date BETWEEN %s AND %s LIMIT 1000
        '''
        
        cursor.execute(query, (from_date, to_date))

        # Fetch all results and convert them to a pandas DataFrame
        records = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=col_names)

        return df.to_dict(orient="records")
    
    except Exception as e:
        print(f"Error retrieving data: {e}")
        return []
    
    finally:
        cursor.close()
        connection.close()

def get_source_filter(source):
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        if(source != 'Web'): source = 'Scheduler'
        # SQL query to retrieve data where only the date part of trans_date_trans_time is compared
        query = SQL("SELECT * FROM predict_table WHERE data_source ILIKE '%" + source +"%' LIMIT 1000")
        print('query: ', query)
        cursor.execute(query)
        

        # Fetch all results and convert them to a pandas DataFrame
        records = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=col_names)

        return df.to_dict(orient="records")
    
    except Exception as e:
        print(f"Error retrieving data: {e}")
        return []
    
    finally:
        cursor.close()
        connection.close()
    
@app.get("/filter-type/")
async def get_source_filter_api(source: str):
    try:
        results = get_source_filter(source)  # Assuming get_source_filter is async
        return {"predicted_data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/predicted-data/")
async def get_predictions(from_date: datetime = Query(..., description="Start date in the format 'YYYY-MM-DD'"),
                          to_date: datetime = Query(..., description="End date in the format 'YYYY-MM-DD'")):
    """
    Endpoint to get predicted data between a date range.
    from_date: Start date in format 'YYYY-MM-DD'
    to_date: End date in format 'YYYY-MM-DD'
    """
    # Retrieve the data from the database based on the date range
    results = get_predicted_data_by_date(from_date, to_date)

    # Return the retrieved data
    return {"predicted_data": results}

@app.post("/predict/")
async def predict_transaction(transaction: Transaction):
    # Convert input into a DataFrame
    input_data = pd.DataFrame([transaction.dict()])

    # Preprocess the input and predict
    feature_columns = model.named_steps['preprocessor'].transformers_[0][2] + \
                      model.named_steps['preprocessor'].transformers_[1][2]
    X_test_preprocessed = input_data[feature_columns]
    prediction = model.predict(X_test_preprocessed)

    # Add the prediction to the DataFrame
    input_data['is_fraud'] = prediction[0]
    current_datetime = dt.datetime.now()
    input_data['trans_date_trans_time'] =current_datetime

    # Insert the data into the database
    insert_data_to_db(input_data)

    # Convert DataFrame to JSON and return the full dataset
    result = input_data.to_dict(orient='records')
    
    return {"predicted_data": result}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    

