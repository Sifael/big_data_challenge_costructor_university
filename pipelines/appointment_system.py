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


def stageJsonAppointments(filepath, staging_table):
    conn = True
    appointment_data = pd.read_json(filepath)
    appointment_data.to_sql(staging_table, conn=conn)


DETAILS = []

# MAIN DAG
with DAG("general_practictioner_appointments",
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
    
    # 1. ACCESS DOCTORLIB API
    fetch_doctorlib_appointments = BashOperator(
        task_id="fetch_doctorlib_appointments",
        bash_command=" curl -o /tmp/doctorlib_appointments.json -L 'https://api.doctolib.de/' "
    )
    
    # 2 PROCESS JSON TO STAGING TABLE 
    stage_doctorlib_appointments = PythonOperator(
        task_id="stage_doctorlib_appointments",
        python_callable=stageJsonAppointments
    )
    
    # 3. ACCESS DOCTORLIB API
    fetch_jameda_appointments = BashOperator(
        task_id="fetch_jameda_appointments",
        bash_command=" curl -o /tmp/jameda_appointments.json -L 'https://api.jameda.de/' "
    )
    
    # 4 PROCESS JSON TO STAGING TABLE
    stage_jameda_appointments = PythonOperator(
        task_id="stage_jameda_appointments",
        python_callable=stageJsonAppointments,
        
    )
    
    # 5. JOIN APPOINTMENT AVAILABILITIES INTO APPOINTMENTS TABLE
    local_appointment_openings = SQLExecuteQueryOperator(
            task_id="local_appointment_openings",
            sql=f"""SELECT 
                        id,
                        doctor_name,
                        appointment,
                        rating,
                        review
                    FROM jameda_staging
                    JOIN ON doctorlib_staging
                    ON a.doctor_id = b.doctor_id
                    WHERE ds = '<DATEID>' 
            """,
            split_statements=True,
            return_last=False,
    )
    
    # 6. Rank 
    list_local_openings = SQLExecuteQueryOperator(
            task_id="list_local_openings",
            sql=f"""SELECT * FROM available_appointments WHERE ds = '<DATEID>' """,
            split_statements=True,
            return_last=False,
    )
    
    
    # . EXECUTE SQL INSERT TO TABLE
    log_created_appointments = BashOperator(
        task_id="log_created_appointments",
        bash_command="echo $date $user $doctor $time >> appointment_details.json"
    )
    
    # SEND CONFIRMATION EMAIL
    send_confirmation_email = EmailOperator(
        task_id="send_confirmation_email",
        to="example@gmail.com",
        subject="Your Appointment is Confirmed",
        html_content=True,
        files= 'confirmation.pdf'
    )
    
    # . EXECUTE SQL INSERT TO TABLE
    share_medical_history = BashOperator(
        task_id="share_medical_history",
        bash_command="echo $access_token >> appointment_details.json"
    )
    
    
    
    
    
    ## PIPELINE STRUCTURE
    fetch_doctorlib_appointments >> stage_doctorlib_appointments
    fetch_jameda_appointments >> stage_jameda_appointments
    
    stage_jameda_appointments >> local_appointment_openings
    stage_doctorlib_appointments >> local_appointment_openings
    
    local_appointment_openings >> list_local_openings >> log_created_appointments >> send_confirmation_email
    log_created_appointments >> share_medical_history
    
    
    
    
    
    