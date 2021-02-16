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

# bucket_name = 'pintek-data-bucket' # change to your bucket name here
# loan_app_filename = 'loan_app.json'
# loan_app_schema_filename = 'loan_app_schema.json'

dag= DAG('ExtractProductionDB', catchup=False,
        schedule_interval = '0 2 * * *',
        default_args=default_args)

class TableConfig:
    def __init__(self,
                 cloud_sql_instance,
                 export_bucket,
                 export_database,
                 export_table,
                 export_query,
                 gcp_project,
                 stage_dataset,
                 stage_table,
                 stage_final_query,
                 bq_location
                 ):

        self.params = {
            'export_table': export_table,
            'export_bucket': export_bucket,
            'export_database': export_database,
            'export_query': export_query,
            'gcp_project': gcp_project,
            'stage_dataset': stage_dataset,
            'stage_table': stage_table or export_table,
            'stage_final_query': stage_final_query,
            'cloud_sql_instance': cloud_sql_instance,
            'bq_location': bq_location or "US",
        }


def get_tables():        
    pintek_tables = ["activations","api_developer","api_developer_log","applicant_addresses","applicant_b2c","applicant_b2c_basicinstant","applicant_b2c_basicinstant_kyc_manual","applicant_b2c_creditbureau","applicant_bank_acc","applicant_blacklist","applicant_details","applicant_docs","applicant_docs_additional","applicant_employments","applicant_guarantor","applicant_references","applicant_va","asliri_json","asliri_verify_basic_instant","asliri_verify_location","asliri_verify_profesional","asliri_verify_selfie","asliri_verify_tax_personal","automate_schedule","automate_schedule_loan_details","automate_schedule_pefindo_report_contractdata","automate_schedule_pefindo_report_individual","bank_inquiry","bank_inquiry_correction","bni_disbursement","bni_disbursement_broker","bni_disbursement_institution_instant","bni_disbursement_lender_instant","callback_disbursement_response_payment_gateway","callback_fva_response_payment_gateway","callback_inquiry_iluma","callback_repayment_response_bni","data_upload","digisign_documents","digisign_json","digisign_signature","digital_signature","digital_signature_credential","digital_signature_documents","email_out","feedback_user","institution","institution_bank_acc","institution_branches","institution_brief","institution_docs","institution_images","institution_major","institution_programs","invite_institution","lender","lender_addresses","lender_agreement","lender_akta_history","lender_cofs","lender_contacts","lender_details","lender_digital_signature","lender_digital_signature_documents","lender_docs","lender_loan_type","lender_repayment_setting","lender_stakeholder","lender_user","lender_user_secret","lender_va","loan_app","loan_app_analysis","loan_app_decision_info","loan_app_details","loan_app_disbursement","loan_app_docs","loan_app_legal","loan_app_notes","loan_app_status","loan_app_status_log","loan_collection_activity_history","loan_cra_activity_history","loan_dpd","loan_insurance","loan_lender","loan_lender_digital_signature_document","loan_repayment_borrower_details","loan_repayment_borrower_distributions","loan_repayment_borrower_schedule","loan_repayment_details_discount_log","loan_repayment_lender_details","loan_repayment_lender_schedule","loan_repayments_borrower","loan_repayments_lender","loan_repayments_lender_user_logs","log_ogp_bni","log_va_bni","logs_registrant_data_changes","logs_sms_sent","logs_temp_link","m_accial_borrower_type","m_accial_collateral_type","m_accial_concept","m_accial_country","m_accial_currency","m_accial_education_type","m_accial_facility","m_accial_frequency","m_accial_fund","m_accial_industry","m_accial_loan_document_type","m_accial_loan_structure_type","m_accial_marital_status","m_accial_product","m_accial_product_type","m_accial_settlement_type","m_accial_term_unit","m_list_feedback","m_partner","m_partner_credential","m_partner_request_log","m_partner_token_leads","m_partner_type","m_template_user_feedback","master_bank","master_blacklist","master_company_type","master_cos_of_funds","master_country","master_digital_signature_status","master_employment_type_details","master_insurance","master_insurance_broker","master_insurance_rate","master_ojk_salary","master_product_loan","master_promo","master_promo_details","master_promo_meta","master_promo_rules","master_province","notifications","page_banner","page_banner_file","page_logo","page_logo_categories","page_logo_route_category","page_logo_routes","page_meta","pages","pefindo_borrower_company","pefindo_borrower_data_historis","pefindo_borrower_fasilitas_info_terakhir","pefindo_borrower_fasilitas_list","pefindo_borrower_fasilitas_permintaan","pefindo_borrower_individual","pefindo_borrower_pengkinian_identitas","pefindo_borrower_score","pefindo_borrower_score_risk","pefindo_log_error","pefindo_segmentation","pefindo_xml","persistences","privy_logs","qontak_deal_sync","referral","registrant","registrant_jail","registrant_otp","registrant_secret","reminders","role_users","roles","school_parents","setting_admin_fee","setting_application_status","setting_b2c_employment_type","setting_b2c_loan_assumption","setting_company","setting_doc","setting_employment_type","setting_instant_log_stage","setting_instant_log_stage_status","setting_institution_category","setting_loan_type","student","student_addresses","student_riwayatpembayaran","student_school_bankaccount","student_summarypembayaran","system_settings","t_accial_logs","t_accial_logs_payment","t_instant_logs","t_sme_additional_information","t_sme_borrower","t_sme_business_analysis","t_sme_crm_analysis_comments","t_sme_crm_analysis_documents","t_sme_digital_signature","t_sme_disbursement_detail","t_sme_document","t_sme_financial_analysis","t_sme_guarantor_profile","t_sme_loan","t_sme_loan_details","t_sme_management_list","t_sme_owner_list","t_sme_pic_profile","t_sme_risk_mitigation","t_sme_user_file_upload","t_sme_user_notes","t_sme_users","throttle","user","user_division","user_logs","user_otp","user_role","user_role_module","user_secret","users","vendor_api_access"]
    export_tables = pintek_tables
    tables = []
    for dimm in export_tables:
        cfg = TableConfig(cloud_sql_instance='pintek-main-database-biznet-source-cloudsql',
                          export_table=dimm.split(".")[-1],
                          export_bucket='pintek-data-bucket',
                          export_database=dimm.split(".")[0],
                          export_query="SELECT * from {}".format(dimm),
                          gcp_project="pintek-production",
                          stage_dataset="ProductionLayer",
                          stage_table=None,
                          stage_final_query=None,
                          bq_location="US")
        tables.append(cfg)
    return tables

def gen_export_table_task(table_config):
    export_task = MySqlToGoogleCloudStorageOperator(task_id='export_{}'.format(table_config.params['export_table']),
                                                    dag=dag,
                                                    sql=table_config.params['export_query'],
                                                    bucket=table_config.params['export_bucket'],
                                                    filename="cloudsql_to_bigquery/filename/{}/{}".format(table_config.params['export_database'],
                                                                                                 table_config.params['export_table']) + "_{}",
                                                    schema_filename="cloudsql_to_bigquery/schema/{}/schema_raw".format(table_config.params['export_table']),
                                                    mysql_conn_id="cloudsql-read-replica")
    export_task.doc_md = """\
    #### Export table from cloudsql to cloud storage
    task documentation
    """
    return export_task

def gen_import_table_task(table_config):
    import_task = GoogleCloudStorageToBigQueryOperator(
        task_id='{}_to_bigquery'.format(table_config.params['export_table']),
        bucket=table_config.params['export_bucket'],
        source_objects=["cloudsql_to_bigquery/filename/{}/{}*".format(table_config.params['export_database'],
                                                             table_config.params['export_table'])],
        destination_project_dataset_table="{}.{}.{}".format(table_config.params['gcp_project'],
                                                            table_config.params['stage_dataset'],
                                                            table_config.params['stage_table']),
        schema_object="cloudsql_to_bigquery/schema/{}/schema_raw".format(table_config.params['export_table']),
        write_disposition='WRITE_TRUNCATE',
        #time_partitioning= {'time_partitioning_type':'DAY','field':'created_at'},
        source_format="NEWLINE_DELIMITED_JSON",
        dag=dag)

    import_task.doc_md = """\
        #### Import table from storage to bigquery
        task documentation    
        """
    return import_task
""" 1
The code that follows setups the dependencies between the tasks
"""

for table_config in get_tables():
    export_script = gen_export_table_task(table_config)
    import_script = gen_import_table_task(table_config)

    export_script >> import_script