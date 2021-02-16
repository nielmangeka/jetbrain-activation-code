from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'owner': 'herpaniel',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['herpaniel.mangeka@pintek.id'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

bucket_name = 'pintek-data-bucket' # change to your bucket name here
applicant_filename = 'cloudsql_to_bigquery/filename/applicant/applicant.json'
applicant_schema_filename = 'cloudsql_to_bigquery/schema/applicant/applicant_schema.json'

with DAG('ExtractApplicantTable', schedule_interval = '0 2 * * *', default_args=default_args) as dag:
    extract = MySqlToGoogleCloudStorageOperator(
                task_id='extract',
                sql='SELECT updated_at,tax_id,sex,religion,registrant_id,phone_verified,nationality,marital_status,born_place,phone,longitude,phone2,is_verified,mother_name,fullname,created_at,TIME_FORMAT(time_to_contact, "%H %i %s") as time_to_contact,family_id,email,insurance,email_verified,born_date,education,id,blacklisted,dependent,latitude,applicant_num,national_id FROM pintek.applicant', # change to your mysql table
                bucket=bucket_name,
                filename=applicant_filename,
                schema_filename=applicant_schema_filename,
                mysql_conn_id='cloudsql-read-replica', # change to your mysql connection id
            )

    load = GoogleCloudStorageToBigQueryOperator(
                task_id='load',
                destination_project_dataset_table='pintek-production.ProductionLayer.applicant', #change to your bq
                bucket=bucket_name,
                source_objects=[applicant_filename],
                schema_object=applicant_schema_filename,
                write_disposition='WRITE_TRUNCATE',
                #time_partitioning= {'time_partitioning_type':'DAY','field':'created_at'},
                create_disposition='CREATE_IF_NEEDED',
                source_format='NEWLINE_DELIMITED_JSON'
            )

    # transform = BigQueryOperator(
    #             task_id='transform',
    #             sql="SELECT * REPLACE(REGEXP_REPLACE(cellphone, '[^0-9]', '') AS cellphone) FROM 'StagingLayer.loan_app",
    #             use_legacy_sql=False,
    #             destination_dataset_table='StagingLayer.loan_app_clean' #change to your bq

    #         )

    extract >> load
    #  >> transform