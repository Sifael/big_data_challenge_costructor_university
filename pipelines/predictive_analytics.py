"""

This Airflow pipeline implements the Appointment System.
The operational Flow involves the following steps

1. Get data from Doctorlib
2. Store Data on Staging Table
3. Get data from Jameda
4. Store Data on Staging Table
5. Join Doctor Information
6. Implement Intelligent Options

"""
from datetime import datetime, timedelta
import pandas as pd

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator


def preprocessImages(filepath, staging_table):
    pass 
    conn = True
    appointment_data = pd.read_json(filepath)
    appointment_data.to_sql(staging_table, conn=conn)
    
def preprocessImages(filepath, staging_table):
    pass 

def saveImageModel(filepath, staging_table):
    pass 

def saveMedicalHistoryModel(filepath, staging_table):
    pass 

def getModelPredictions():
    pass

def aggregateModelPredictions():
    pass

def preprocessMedicalHistory():
    pass

# MAIN DAG
with DAG("predictive_analytics",
         default_args={
             "depends_on_past": False,
            "email": ['sndandala@jacobs-university.de', 'skathiresa@constructor.university', 'pveerachan@constructor.university', 'nparmar@constructor.university' ],
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=5)},
            description="GP Appointments Management Pipeline",
            schedule_interval="@daily",
            start_date=datetime(2023,11,6),
            template_searchpath='~/airflow/sql',
            tags=["appointments_and_health_records_system"]) as dag:
    
    pass

    # FETCH IMAGES
    get_medical_images = BashOperator(
        task_id="get_medical_images",
        bash_command="curl -o https://aws.com/s3/medical-images/"
    )
    
    preprocess_images = PythonOperator(
        task_id="preprocess_images",
        python_callable=preprocessImages
    )
    
    initiate_model_training = BashOperator(
        task_id="initiate_images_model_training",
        bash_command="python main.py"
    )
    
    save_image_model = PythonOperator(
        task_id="save_image_model",
        python_callable=saveImageModel
    )
    
    get_images_model_predictions = PythonOperator(
        task_id="get_images_model_predictions",
        python_callable=getModelPredictions
    )
    
    ##
    get_medical_images >> preprocess_images >> initiate_model_training 
    initiate_model_training >> save_image_model >> get_images_model_predictions
    
    
    # FETCH IMAGES
    get_medical_history = SQLExecuteQueryOperator(
        task_id="get_medical_history",
        sql=f"""SELECT * FROM medical_history WHERE ds = '<DATEID>' """,
        split_statements=True,
        return_last=False,
    )
    
    preprocess_medical_history = PythonOperator(
        task_id="preprocess_medical_history",
        python_callable=preprocessMedicalHistory
    )
    
    initiate_model_training_medical_history = BashOperator(
        task_id="initiate_medical_history_model_training",
        bash_command="python main.py"
    )
    
    save_model_medical_history = PythonOperator(
        task_id="save_medical_history_model",
        python_callable=saveMedicalHistoryModel
    )
    
    get_medical_history_prediction = PythonOperator(
        task_id="get_medical_history_prediction",
        python_callable=getModelPredictions
    )
    
    aggregate_all_predictions = PythonOperator(
        task_id = "aggregate_all_predictions",
        python_callable=aggregateModelPredictions
        
    )
    
    
    # MEDICAL HISTORY PIPELINE
    get_medical_history >> preprocess_medical_history >> initiate_model_training_medical_history
    initiate_model_training_medical_history >> save_model_medical_history >> get_medical_history_prediction
    
    # COMBINED PIPELINE
    get_images_model_predictions >> aggregate_all_predictions
    get_medical_history_prediction >> aggregate_all_predictions
    
    