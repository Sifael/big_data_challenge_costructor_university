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


def medicalHealthStagingTable(**kwargs):
    pass


def getMajorProcedures(**kwargs):
    pass

def getTherapySessions(**kwargs):
    pass


# MAIN DAG
with DAG("medical_records",
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
    
    
    # ====================================================
    #            HEALTH DATA
    # ====================================================
    get_insurance_id = BashOperator(
        task_id="get_insurance_id",
        bash_command="curl -o /tmp/appointments.json -L 'https://' ",
    )
    
    # Get therapy data
    get_therapy_data = PythonOperator(
        task_id="collect_therapy_data",
        python_callable=getTherapySessions,
    )
    
    # Get medication history
    get_medication_history = BashOperator(
        task_id="collect_medication_history",
        bash_command="curl -o /tmp/appointments.json -L 'https://' ",
    )

    # Get years doctor visit/diagnosis
    get_previous_diagnosis = BashOperator(
        task_id="collect_previous_diagnosis",
        bash_command="curl -o /tmp/appointments.json -L 'https://' ",
    )
    
    get_current_treatments = BashOperator(
        task_id="collect_current_treatments",
        bash_command="curl -o /tmp/appointments.json -L 'https://' ",
    )
    
    get_pregnancy_history = BashOperator(
        task_id="collect_pregnancy_history",
        bash_command="curl -o /tmp/appointments.json -L 'https://' ",
    )
    
    get_major_procedures = PythonOperator(
        task_id="collect_major_procedures",
        python_callable=getMajorProcedures,
    )

    # Medical Health Staging Table
    generate_staging_table = PythonOperator(
        task_id="medical_history_staging_tabl",
        python_callable=medicalHealthStagingTable
    )

    # Merge Medical health
    merge_medical_history = SQLExecuteQueryOperator(
            task_id="merge_full_medical_history",
            sql=f"""SELECT * FROM available_appointments WHERE ds = '<DATEID>' """,
            split_statements=True,
            return_last=False,
        
    )

    # access medical images
    get_medical_images = BashOperator(
        task_id="patient_medical_images",
        bash_command="curl -o /tmp/appointments.json -L 'https://aws.com/s3-bucket' "
        
    )

    ## combine all medical history into a database
    merge_all_medical_data = SQLExecuteQueryOperator(
        task_id="merge_all_medical_data",
        sql=f"""SELECT * FROM available_appointments WHERE ds = '<DATEID>' """,
        split_statements=True,
        return_last=False,
    )
    
    
    # MEDICAL HISTORY PIPELINE
    get_therapy_data >> generate_staging_table
    get_medication_history >> generate_staging_table
    get_previous_diagnosis >> generate_staging_table
    get_pregnancy_history >> generate_staging_table
    get_current_treatments >> generate_staging_table


    generate_staging_table >> merge_medical_history
    
    get_medical_images >> merge_all_medical_data
    merge_medical_history >> merge_all_medical_data
    get_insurance_id >> merge_all_medical_data
    