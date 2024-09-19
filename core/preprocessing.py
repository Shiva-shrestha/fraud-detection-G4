import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler

COLUMNS_TO_DROP = [
    'Unnamed: 0', 'trans_date_trans_time', 'cc_num', 'first', 'last',
    'street', 'city', 'state', 'zip', 'dob', 'trans_num'
]

def preprocess_data(files):
    combined_data = pd.DataFrame()

    # Combine data from multiple CSV files
    for file in files:
        data = pd.read_csv(file)
        clean_data = data.drop(columns=COLUMNS_TO_DROP)
        combined_data = pd.concat([combined_data, clean_data], ignore_index=True)
    
    # Separate features and target
    X = combined_data.drop('is_fraud', axis=1)
    y = combined_data['is_fraud']
    return X, y

def create_preprocessor(X):
    # Identify categorical and numerical features
    categorical_features = X.select_dtypes(include=['object']).columns.tolist()
    numerical_features = X.select_dtypes(include=[np.number]).columns.tolist()

    # Define the column transformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', MinMaxScaler(), numerical_features),
            ('cat', OneHotEncoder(drop='first', sparse_output=True, handle_unknown='ignore'), categorical_features)
        ]
    )

    return preprocessor
