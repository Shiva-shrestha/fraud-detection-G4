# Credit Card Fraud Detection(project)

# Description:
This project leverages machine learning to predict credit card transaction fraud. It employs a Python-based backend, a user-friendly Streamlit interface, and a FastAPI-powered RESTful API.

# Key Features:
Robust Model: Trains a sophisticated machine learning model on historical transaction data.
Intuitive Interface: Offers a Streamlit dashboard for easy data visualization and testing.
Flexible API: Exposes a RESTful API for seamless integration with other applications.

# Technologies:
Python
Pandas
NumPy
Scikit-learn
Streamlit
FastAPI

# System distribution:
1. Application is made using streamlit to make a user interface consist of main page where user predictions are made through user input and also by uploading of file. And past predictions are displayed in next page.
2. Backend service is created by fastAPI service and python programming language. It makes to predict and store the predicted data.
3. Data are stored in Postgres SQL database.
4. Airflow is used for data ingestion.


# ML Project

This repository contains a complete Machine Learning pipeline that includes data ingestion, processing, model building, and serving predictions. The project is designed with modular components to handle various stages of the ML workflow, such as data preprocessing, feature engineering, model training, and serving the model for predictions.

---

# Table of Contents
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
  - [Clone the repository](#clone-the-repository)
  - [Set up the virtual environment](#set-up-the-virtual-environment)
  - [Install Python dependencies](#install-python-dependencies)
  - [Data Setup](#data-setup)
  - [Airflow Setup](#airflow-setup)
  - [Start the Service](#start-the-service)
- [Usage](#usage)
- [Directory Structure](#directory-structure)
- [Contributing](#contributing)
- [License](#license)

---

# Features
- Data ingestion pipeline for reading and cleaning datasets.
- Supports both raw and cleaned data stored in separate folders.
- Trained models are stored for future use in predictions.
- Includes Apache Airflow for task scheduling and orchestration of the ML pipeline.
- Logs are maintained for each stage of data processing and model inference.
- Modular design for scalability and maintainability.

---

# Tech Stack
- **Python** – Programming language for implementing the ML pipeline.
- **Apache Airflow** – Task orchestration for scheduling and automating data pipeline tasks.
- **Scikit-learn** – Machine learning library for building models.
- **Pandas** – Data manipulation and analysis.
- **Streamlit (Optional)** – Frontend for serving the model predictions.
- **Docker (Optional)** – Containerization for easy deployment and management.

---

# Prerequisites
Before setting up the project, ensure you have the following software installed:

- Python 3.9+
- Apache Airflow
- Docker (Optional)
- PostgreSQL (if using a database for storing results)

---

# Installation & Setup

### Clone the repository
```bash
git clone https://github.com/your-username/ml-project.git
cd ml-project
```

### Set up the virtual environment
It is recommended to use a virtual environment to manage dependencies:

Using `venv`:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Using `conda`:
```bash
conda create --name ml-project python=3.9
conda activate ml-project
```

### Install Python dependencies
All necessary dependencies are listed in `requirements.txt`. Install them using:

```bash
pip install -r requirements.txt
```

Alternatively, manually install:
```bash
pip install apache-airflow pandas scikit-learn
```

### Data Setup
- Place your raw data files in the `data/raw_data` folder.
- Cleaned and preprocessed data should be placed in the `data/good_data` folder.

### Airflow Setup
If you plan to run Apache Airflow for scheduling tasks:

1. Install Apache Airflow:
   ```bash
   pip install apache-airflow
   ```

2. Initialize the Airflow database:
   ```bash
   airflow db init
   ```

3. Start the Airflow web server and scheduler:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

4. Create your DAGs (directed acyclic graphs) to automate the data ingestion, model training, and prediction tasks. Place them in the `dags/` folder.

### Start the Service
If your project includes a service for serving predictions (e.g., FastAPI or another backend):

1. Update configuration files to point to your trained model and data paths.
2. Run the service:
   ```bash
   uvicorn app.main:app --reload
   ```

---

# Usage
1. **Data Ingestion**: 
   - Airflow will trigger the ingestion pipeline to pull new data and preprocess it. 
   - Logs will be stored under `logs/dag_id=data_ingestion_pipeline`.

2. **Model Training**: 
   - Use the data in the `good_data` folder to train your model. Once trained, the model will be saved in the `model/` folder.

3. **Prediction**: 
   - Make predictions on new data using the trained model.
   - If using a service (e.g., FastAPI), interact with the model via API endpoints.

4. **Airflow**: 
   - Check the status of your tasks via the Airflow web interface at `http://localhost:8080`.

---

# Directory Structure
```bash
.
├── app
│   └── pages
├── config
├── core
├── dags
│   └── __pycache__
├── data
│   ├── bad_data
│   ├── good_data
│   └── raw_data
├── ingestion
├── logs
│   ├── dag_id=data_ingestion_pipeline
│   │   ├── run_id=manual__2024-11-09T01:39:58.974003+00:00
│   │   │   └── task_id=read_data
│   │   └── run_id=manual__2024-11-09T01:41:03.091450+00:00
│   │       └── task_id=read_data
│   ├── dag_processor_manager
│   └── scheduler
│       ├── 2024-11-08
│       ├── 2024-11-09
│       └── latest
├── main_data
├── model
├── notebook
├── plugins
└── service
```

---

# Contributing
Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-name`).
3. Make your changes.
4. Commit your changes (`git commit -m "Add feature"`).
5. Push to the branch (`git push origin feature-name`).
6. Submit a pull request.

---

# License
This project is licensed under the MIT License. See the LICENSE file for more details.