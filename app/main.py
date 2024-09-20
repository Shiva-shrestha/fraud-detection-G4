import streamlit as st
import pandas as pd
import requests
import constant
import sys
import datetime

# Add the path to your folder
sys.path.append("../core/")

# FastAPI backend URL
API_URL = "http://127.0.0.1:8000"

# Define the Streamlit app
st.title("Credit Card Fraud Detection")

# Predict using user input
if st.checkbox('Predict using user input'):
    st.subheader("Enter details:\n")
    
    # Input fields
    merchant = st.selectbox("Merchant", constant.merchant_type)
    category = st.selectbox("Category", constant.category_type)
    job = st.selectbox("Job", constant.job_type)
    gender = st.selectbox("Gender", ["M", "F"])
    amt = st.number_input("Transaction Amount", min_value=0.0, step=0.01)
    lat = st.number_input("Latitude", format="%.6f")
    long = st.number_input("Longitude", format="%.6f")
    city_pop = st.number_input("City Population", min_value=0)
    unix_time = st.number_input("Unix Time", min_value=0)
    merch_lat = st.number_input("Merchant Latitude", format="%.6f")
    merch_long = st.number_input("Merchant Longitude", format="%.6f")

    # On clicking "Predict Fraud"
    if st.button("Predict Fraud"):
        # Prepare the payload
        input_data = {
            'merchant': merchant,
            'category': category,
            'amt': amt,
            'gender': gender,
            'lat': lat,
            'long': long,
            'city_pop': city_pop,
            'job': job,
            'unix_time': unix_time,
            'merch_lat': merch_lat,
            'merch_long': merch_long
        }
        response = requests.post(f"{API_URL}/predict/", json=input_data)
        if response.status_code == 200:
            result = response.json()  # Extract JSON content
            df = pd.DataFrame(result["predicted_data"])
            #st.dataframe(df)
            if any(df['is_fraud'] == 1):
                st.error("This transaction is predicted to be FRAUDULENT!")
            else:
                st.success("This transaction is predicted to be LEGITIMATE.")
                


# Predict using a CSV file
if st.checkbox('Predict using file'):
    st.subheader("Upload CSV file")
    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

    if uploaded_file is not None:
        # Send the file to FastAPI for processing
        response = requests.post(f"{API_URL}/predict-file/", files={"file": uploaded_file})
        if response.status_code == 200:
            result = response.json()  # Extract JSON content
            df = pd.DataFrame(result["predicted_data"])  # Convert to DataFrame
            
            fraud_count = df[df['is_fraud'] == 1].shape[0]
            st.write(f"Number of fraudulent transactions detected: {fraud_count}")
            st.write("Prediction completed. Here's the data with predictions:")
            #st.dataframe(styled_df)  # Display the styled DataFrame
            st.dataframe(df)

        else:
            st.error(f"Error: {response.status_code} - {response.text}")
