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


def patientIDtoMedicalImages(**kwargs):
    pass


def UpdateTablePatientIDtoMedicalImages(**kwargs):
    pass

# MAIN DAG
with DAG("medical_images",
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
    
    
    # MEDICAL IMAGES
    get_CT_SCANS_images = BashOperator(
        task_id="get_CT_SCANS_images",
        bash_command="curl -o /tmp/ct_scans.zip -L '' ",
    )
    
    # GET MRI IMAGES
    get_MRI_images = BashOperator(
        task_id="get_MRI_images",
        bash_command="curl -o /tmp/mri.zip -L '' ",
    )
    
    # GET XRAY IMAGES
    get_XRAY_images = BashOperator(
        task_id="get_XRAY_images",
        bash_command="curl -o /tmp/xray.zip -L '' ",
    )
    
    
    # SAVE IMAGES TO S3
    save_images_to_s3 = BashOperator(
        task_id="save_images_to_s3_bucket",
        bash_command=""
    )
    
    ## PATIENT ID TO IMAGE SET
    map_patient_id_to_images = PythonOperator(
        task_id="map_patient_id_to_images",
        python_callable=patientIDtoMedicalImages
    )

    update_patient_image_table = SQLExecuteQueryOperator(
        task_id="update_patient_image_table",
        sql=f"""INSERT INTO patient_id_to_images_table VALUES() """,
        split_statements=True,
        return_last=False,
    )
    
    
    ## MEDICAL IMAGES
    get_MRI_images >> map_patient_id_to_images
    get_CT_SCANS_images >> map_patient_id_to_images
    get_XRAY_images >> map_patient_id_to_images
    
    # UPDATE PATIENT ID TO IMAGES
    map_patient_id_to_images >> update_patient_image_table >> save_images_to_s3
    

    