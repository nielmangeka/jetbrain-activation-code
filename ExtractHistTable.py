# PT. PINDUIT TEKNOLOGI INDONESIA (PINTEK)
# Created by Herpaniel Rumende mangeka
# 15 FEBRUARI 2021

from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args ={
    'owner': 'herpaniel',
    'depends_on_past': False,
    'email': ['herpaniel.mangeka@gmail.com', 'andrea.philo@pintek.id'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Create_History_Table',
    default_args=default_args,
    description='Create History Table in Production Layer',
    schedule_interval ='0 1 * * *',
    start_date=days_ago(2),
    tags=['PINTEK'],
)

tables_history = ["lender_contacts","school_parents","page_logo_route_category","master_province","master_ojk_salary","master_promo_details","lender_repayment_setting","master_digital_signature_status","master_product_loan","master_country","master_bank","loan_lender","lender_loan_type","loan_app_status","lender_user","lender_stakeholder","lender_akta_history","asliri_verify_location","asliri_verify_tax_personal","asliri_verify_profesional","applicant_guarantor","applicant_bank_acc"]
export_tables_history = tables_history
tables = []
for tablehist in export_tables_history:
    Create_History_Table = BigQueryOperator(
        task_id='{}_to_bigquery'.format(tablehist),
        sql = "SELECT timestamp_add(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', current_timestamp() , 'UTC+7')),INTERVAL -1 DAY) as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(tablehist),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_APPEND",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.ProductionLayer.hist_{}'.format(tablehist),
        dag = dag
    )
    tables.append(Create_History_Table)
Create_History_Table