import os
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np

import preprocessing  # Import your preprocessing module

def train_and_save_model(X, y, model_filename):

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

    # Create preprocessor
    preprocessor = preprocessing.create_preprocessor(X_train)

    # Build the model pipeline with Logistic Regression
    model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', LogisticRegression(solver='liblinear', random_state=42, class_weight='balanced', max_iter=100))
    ])

    # Train the model
    model.fit(X_train, y_train)

    # Save the model using joblib
    model_path = os.path.join(model_filename)
    joblib.dump(model, model_path)

    # Evaluate the model
    y_pred = model.predict(X_test)
    class_report = classification_report(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    print("Classification Report:\n", class_report)
    print("Confusion Matrix:\n", conf_matrix)

    def compute_rmsle(y_test: np.ndarray, y_pred: np.ndarray, precision: int = 2) -> float:
        rmsle = np.sqrt(mean_squared_error(y_test, y_pred))
        return round(rmsle, precision)

    # Calculate the RMSLE for training and testing sets
    #rmsle_train = compute_rmsle(y_train, y_pred_train)
    rmsle_test = compute_rmsle(y_test, y_pred)
    
    #print(f'Training RMSLE: {rmsle_train}')
    print(f'Testing RMSLE: {rmsle_test}')

    return model_path
