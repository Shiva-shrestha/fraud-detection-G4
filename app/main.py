import streamlit as st
import pandas as pd
import requests
import constant
import sys
import datetime
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

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
            # st.dataframe(df)
            if any(df['is_fraud'] == 1):
                # Create a layout with centered columns
                col1, col2, col3 = st.columns([1, 2, 1])  # Create columns with different widths

                with col2:  # The middle column where we center the image and message
                    st.image("images/Fraud-Blog-image.jpg", width=300)  # Center the image

                    # Display ⚠️ on one line and the message on the second line
                    st.markdown('<p style="text-align:center; font-size:40px;">⚠️</p>', unsafe_allow_html=True)
                    #st.markdown('<p style="text-align:center; font-size:20px; color:red;">Suspicious Transaction Detected!</n>', unsafe_allow_html=True)
                    st.markdown('<p style="text-align:center; font-size:20px; color:grey;">Suspicious Transaction Detected! <br>A message has been sent to the account holder.</p>', unsafe_allow_html=True)

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
            fraud_count = result["fraud_count"]
            total_rows = result["total_rows"]
            fraud_percentage = result["fraud_percentage"]
            # Custom function to apply the highlighting
            def highlight_fraud(row):
                if row['is_fraud'] == 1:
                    return ['background-color: lightcoral'] * len(row)
                else:
                    return ['background-color: lightgreen'] * len(row)

            # Apply the highlighting
            # styled_df = df.style.apply(highlight_fraud, axis=1)
            fraud_count = df[df['is_fraud'] == 1].shape[0]            
            st.markdown(
                f"Number of fraudulent transactions detected: <span style='background-color: #CD5C5C;color: white; padding: 2px; border-radius: 3px;'>{fraud_count}</span>",
                unsafe_allow_html=True
            )
            st.markdown(
                f"Total rows processed: <span style='background-color: #CD5C5C ;color: white; padding: 2px; border-radius: 3px;'>{total_rows}</span>",
                unsafe_allow_html=True
            )
            st.markdown(
                f"Percentage of fraudulent transactions: <span style='background-color: #CD5C5C;color: white; padding: 2px; border-radius: 3px;'>{fraud_percentage:.2f}%</span>",
                unsafe_allow_html=True
            )
            st.write("Prediction completed. Here's the data with predictions:")
            # st.dataframe(styled_df)  # Display the styled DataFrame
            st.dataframe(df)

        # Create a pie chart of total transactions vs fraudulent transactions
            non_fraud_count = total_rows - fraud_count
            labels = ['Legitimate Transactions', 'Fraudulent Transactions']
            values = [non_fraud_count, fraud_count]

            # Plotly Pie chart with percentage
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.3, 
                                         marker=dict(colors=['#2E8B57', '#CD5C5C']),
                                         hoverinfo='label+percent', 
                                         textinfo='value+percent',
                                         textfont_size=15)])

            fig.update_layout(
                title_text="Transactions Analysis",
                annotations=[dict(text='Transactions', x=0.5, y=0.5, font_size=30, showarrow=False)]
            )

            # Display the pie chart in Streamlit
            st.plotly_chart(fig)

            df['dob'] = pd.to_datetime(df['dob'])
            current_year = datetime.now().year
            df['age'] = current_year - df['dob'].dt.year 

            # Group by year and fraud status, then count
            age_group = df.groupby(['age', 'is_fraud']).size().reset_index(name='count')
            

            # Separate fraudulent and legitimate transactions
            fraud_data = age_group[age_group['is_fraud'] == 1]
            legit_data = age_group[age_group['is_fraud'] == 0]

            # Create scatter plot using Plotly
            scatterfig = go.Figure()

            # Add Fraudulent transactions as red scatter points
            scatterfig.add_trace(go.Scatter(
                x=fraud_data['age'],
                y=fraud_data['count'],
                mode='markers',
                name='Fraudulent ',
                marker=dict(color='#CD5C5C', size=4),
            ))

            # Add Legitimate transactions as green scatter points
            scatterfig.add_trace(go.Scatter(
                x=legit_data['age'],
                y=legit_data['count'],
                mode='markers',
                name='Legitimate ',
                marker=dict(color='#2E8B57', size=4),
            ))

            # Update layout
            scatterfig.update_layout(
                title="Whole transaction by Age",
                xaxis_title="Year",
                yaxis_title="Number of Transactions",
                legend_title="Transaction Type",
            )

            # Display the scatter plot in Streamlit
            st.plotly_chart(scatterfig)

            # Group by 'category' and 'is_fraud', then count the occurrences
            category_group = df.groupby(['category', 'is_fraud']).size().reset_index(name='count')

            # Separate the fraud and legit data
            fraud_data = category_group[category_group['is_fraud'] == 1]
            legit_data = category_group[category_group['is_fraud'] == 0]

            # Merge to ensure both fraud and legit counts for each category
            merged_data = pd.merge(legit_data, fraud_data, on='category', how='outer', suffixes=('_legit', '_fraud')).fillna(0)

            # Create a horizontal bar chart
            barfig = go.Figure()

            # Add legitimate transactions
            barfig.add_trace(go.Bar(
                y=merged_data['category'],
                x=merged_data['count_legit'],
                name='Legitimate Transactions',
                orientation='h',
                marker=dict(color='#2E8B57')
            ))

            # Add fraudulent transactions
            barfig.add_trace(go.Bar(
                y=merged_data['category'],
                x=merged_data['count_fraud'],
                name='Fraudulent Transactions',
                orientation='h',
                marker=dict(color='#CD5C5C')
            ))

            # Update layout
            barfig.update_layout(
                title="Whole Transactions by Category",
                xaxis_title="Transaction Count",
                yaxis_title="Category",
                barmode='stack',  # Stacks the bars for comparison
                legend_title="Transaction Type",
                height=600  # Adjust height for better visibility of categories
            )

            # Display the bar chart in Streamlit
            st.plotly_chart(barfig)

        else:
            st.error(f"Error: {response.status_code} - {response.text}")

