import joblib
import pandas as pd

def predict_on_new_data(model_filename, test_file_path=None, chunk_size=50000):
    # Load the saved model
    model_path = model_filename
    model = joblib.load(model_path)

    # Define the columns to drop
    COLUMNS_TO_DROP = [
        'Unnamed: 0', 'trans_date_trans_time', 'cc_num', 'first', 'last',
        'street', 'city', 'state', 'zip', 'dob', 'trans_num'
    ]
    
    # Store predictions
    y_pred_all = []
    fraudulent_predictions = []

    # Process test data in chunks
    for chunk in pd.read_csv(test_file_path, chunksize=chunk_size):
        chunk_cleaned = chunk.drop(columns=COLUMNS_TO_DROP, errors='ignore')
        
        # Ensure the same feature columns as used in training
        feature_columns = model.named_steps['preprocessor'].transformers_[0][2] + \
                          model.named_steps['preprocessor'].transformers_[1][2]
        X_test = chunk_cleaned[feature_columns]
        
        # Apply preprocessing and make predictions
        chunk_predictions = model.predict(X_test)
        y_pred_all.append(chunk_predictions)
        
        # Filter only the fraud predictions
        fraud_chunk = chunk[chunk_predictions == 1]
        fraudulent_predictions.append(fraud_chunk)

    # Combine all fraudulent predictions into a single DataFrame
    fraudulent_predictions_df = pd.concat(fraudulent_predictions, ignore_index=True)
    
    return fraudulent_predictions_df
