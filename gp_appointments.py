""" 
Date: November 5th, 2023
Participants: Parmar Nishant, Pakin Veerachanchai, Sripathy Kathiresan Tamilselvan, Sifael Ndandala


This Airflow pipeline is designed to implement the first steps of our project to track and report 
on an application that tracks and reports the GP Appointments. This DAG covers the first step of 
the application which involves Appointment Intake.

Scope:

1. Download a JSON File with all appointments daily
2. Clean and process the data into a text file
3. Insert the data into relational database

"""

from datetime import datetime, timedelta
import requests
import requests.exceptions as requests_exceptions

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 

# PLACE HOLDER FOR DETAILS
DETAILS = []

# MAIN DAG
with DAG( "gp_appointments",
          default_args={
            "depends_on_past": False,
            "email": ['sndandala@jacobs-university.de', 'skathiresa@constructor.university', 'pveerachan@constructor.university', 'nparmar@constructor.university'],
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=5)},
            description="GP Appointments Management Pipeline",
            schedule_interval="@daily",
            start_date=datetime(2023,11,6),
            template_searchpath='~/airflow/sql',
            tags=["gp_appointment"]
) as dag:

    # 1. Download Daily Appointments
    download_appointments = BashOperator(
        task_id="download_appointments_data",
        bash_command="curl -o /tmp/appointments.json -L 'https://raw.githubusercontent.com/Sifael/big_data_challenge_costructor_university/main/sample_appoint.json' ",
    )

    # 2. Extracting data from JSON File
    def ExtractAppointmentDetails():

        with open('/tmp/appointments.json', 'r') as file:
            entries =  json.load(file)
            for appointment in entries['result']:
                date = appointment['date']

                for details in appointment['appointments']:
                    id_ = details['id']
                    patient = details['patientName']
                    address = details['address']
                    insurance = details['insurance']
                    insurance_status = details['status']
                    visit_reason = details['visit_reason']
                    start_time = details['startTime']
                    end_time = details['endTime']
                    doctor = details['general_practitioner']
        
        details = ','.join([date, id_, patient, address, insurance, insurance_status, visit_reason, start_time, end_time, doctor])

        with open('/tmp/appoints.txt', 'w+') as file:
            file.write(details)

    appointment_details = PythonOperator(
        task_id='appointment_details',
        python_callable=ExtractAppointmentDetails
    )

    # 3. EXECUTE SQL INSERT TO TABLE
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_appointment_details",
        sql=f"""INSERT INTO 'appointments' VALUES {DETAILS}""",
        split_statements=True,
        return_last=False,
    )

    # DAG Protocol
    download_appointments >> appointment_details >> insert_data




