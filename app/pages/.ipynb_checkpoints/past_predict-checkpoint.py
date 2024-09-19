import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# FastAPI URL
API_URL = "http://127.0.0.1:8000"

# Title
st.set_page_config(
    page_title="Fraud detected history",
    page_icon="💳",
)
st.title('Credit Card Transaction Fraud Detection History')

# Display all data when the page is loaded
def load_all_data():
    response = requests.get(f"{API_URL}/predicted-data/?from_date=1900-01-01&to_date={datetime.now().date()}")
    if response.status_code == 200:
        data = response.json().get('predicted_data', [])
        return pd.DataFrame(data)
    else:
        st.error("Failed to retrieve data.")
        return pd.DataFrame()

# Function to filter data based on the date range
def filter_data(from_date, to_date):
    response = requests.get(f"{API_URL}/predicted-data/?from_date={from_date}&to_date={to_date}")
    if response.status_code == 200:
        data = response.json().get('predicted_data', [])
        return pd.DataFrame(data)
    else:
        st.error("Failed to retrieve data.")
        return pd.DataFrame()

# Date input for filtering
st.sidebar.subheader("Filter by Date Range")

# Date inputs for the user to select
from_date = st.sidebar.date_input("From Date", datetime(2024, 1, 1))
to_date = st.sidebar.date_input("To Date", datetime.now().date())

# Button to trigger filtering
if st.sidebar.button("Search"):
    # Ensure 'from_date' is before 'to_date'
    if from_date > to_date:
        st.error("Error: 'From Date' must be earlier than 'To Date'.")
    else:
        filtered_data = filter_data(from_date, to_date)
        if not filtered_data.empty:
            st.success(f"Data retrieved for date range: {from_date} to {to_date}")
            st.dataframe(filtered_data.style.applymap(lambda x: 'background-color: red' if x == 1 else 'background-color: green', subset=['is_fraud']))
        else:
            st.warning("No data found for the selected date range.")
else:
    # Load and display all data initially
    all_data = load_all_data()
    if not all_data.empty:
        st.success("Displaying all available data:")
        st.dataframe(all_data.style.applymap(lambda x: 'background-color: red' if x == 1 else 'background-color: green', subset=['is_fraud']))
    else:
        st.warning("No data available.")
