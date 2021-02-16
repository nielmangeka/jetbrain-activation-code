# PT. PINDUIT TEKNOLOGI INDONESIA (PINTEK)
# Created by Herpaniel Rumende mangeka
# 10 FEBRUARI 2021

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
    'Partition_Table_to_Staging_Layer',
    default_args=default_args,
    description='Create History Table in Production Layer',
    schedule_interval ='0 4 * * *',
    start_date=days_ago(2),
    tags=['PINTEK'],
)
tables_with_created_at = ["activations","api_developer","api_developer_log","applicant","applicant_addresses","applicant_b2c","applicant_b2c_basicinstant","applicant_b2c_basicinstant_kyc_manual","applicant_b2c_creditbureau","applicant_blacklist","applicant_details","applicant_docs","applicant_docs_additional","applicant_employments","applicant_references","applicant_va","asliri_json","asliri_verify_basic_instant","asliri_verify_selfie","automate_schedule","automate_schedule_loan_details","automate_schedule_pefindo_report_contractdata",	"automate_schedule_pefindo_report_individual",	"bank_inquiry",	"bank_inquiry_correction",	"bni_disbursement_broker",	"bni_disbursement_institution_instant",	"bni_disbursement_lender_instant",	"callback_disbursement_response_payment_gateway",	"callback_fva_response_payment_gateway",	"callback_inquiry_iluma",	"callback_repayment_response_bni",	"digisign_documents",	"digisign_json",	"digisign_signature",	"digital_signature",	"digital_signature_credential",	"digital_signature_documents",	"feedback_user","institution","institution_bank_acc","institution_branches","institution_brief","institution_docs","institution_images","institution_major","institution_programs","invite_institution","lender","lender_addresses","lender_agreement","lender_cofs","lender_details","lender_digital_signature","lender_digital_signature_documents","lender_docs","lender_va","loan_app","loan_app_analysis","loan_app_decision_info","loan_app_disbursement","loan_app_docs","loan_app_notes","loan_app_status_log",	"loan_collection_activity_history",	"loan_cra_activity_history",	"loan_dpd",	"loan_insurance",	"loan_lender_digital_signature_document",	"loan_repayment_borrower_details",	"loan_repayment_details_discount_log",	"loan_repayment_lender_details",	"loan_repayments_borrower",	"loan_repayments_lender",	"loan_repayments_lender_user_logs",	"log_ogp_bni",	"log_va_bni",	"logs_registrant_data_changes",	"logs_sms_sent",	"logs_temp_link",	"m_accial_borrower_type",	"m_accial_collateral_type",	"m_accial_concept","m_accial_country","m_accial_currency","m_accial_education_type","m_accial_facility","m_accial_frequency","m_accial_fund","m_accial_industry","m_accial_loan_document_type","m_accial_loan_structure_type","m_accial_marital_status","m_accial_product","m_accial_product_type","m_accial_settlement_type","m_accial_term_unit","m_list_feedback","m_partner_request_log","m_partner_token_leads","m_partner_type","m_template_user_feedback","master_blacklist","master_company_type","master_cos_of_funds","master_employment_type_details",	"master_insurance",	"master_insurance_broker",	"master_insurance_rate",	"master_promo",	"master_promo_meta",	"master_promo_rules",	"notifications",	"page_banner",	"page_banner_file",	"page_logo",	"page_logo_categories",	"page_logo_routes",	"page_meta",	"pages",	"pefindo_borrower_company",	"pefindo_borrower_data_historis",	"pefindo_borrower_fasilitas_info_terakhir",	"pefindo_borrower_fasilitas_list",	"pefindo_borrower_fasilitas_permintaan",	"pefindo_borrower_individual",	"pefindo_borrower_pengkinian_identitas","pefindo_borrower_score","pefindo_borrower_score_risk","pefindo_log_error","pefindo_segmentation","pefindo_xml","persistences","privy_logs","qontak_deal_sync","referral","registrant","registrant_jail","registrant_otp","reminders","role_users","roles","setting_admin_fee","setting_application_status","setting_b2c_employment_type","setting_b2c_loan_assumption","setting_company","setting_doc","setting_employment_type","setting_instant_log_stage","setting_instant_log_stage_status",	"setting_institution_category",	"setting_loan_type",	"student",	"student_addresses",	"student_riwayatpembayaran",	"student_school_bankaccount",	"student_summarypembayaran",	"system_settings",	"t_accial_logs",	"t_accial_logs_payment",	"t_instant_logs",	"t_sme_additional_information",	"t_sme_borrower",	"t_sme_business_analysis",	"t_sme_crm_analysis_comments",	"t_sme_crm_analysis_documents",	"t_sme_digital_signature",	"t_sme_disbursement_detail",	"t_sme_document",	"t_sme_financial_analysis",	"t_sme_guarantor_profile","t_sme_loan","t_sme_loan_details","t_sme_management_list","t_sme_owner_list","t_sme_pic_profile","t_sme_risk_mitigation","t_sme_user_file_upload","t_sme_user_notes","t_sme_users","throttle","user","user_division","user_logs","user_otp","user_role","user_role_module","users","vendor_api_access"]
export_tables_with_created_at = tables_with_created_at
tables = []       
for dim1 in export_tables_with_created_at:
    Partition_Task1 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dim1),
        sql = "SELECT created_at as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dim1),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False, 
        destination_dataset_table='pintek-production.Staging.{}'.format(dim1),
        dag = dag
    )
    tables.append(Partition_Task1)
Partition_Task1

tables_with_current_time = ["lender_contacts","school_parents","page_logo_route_category","master_province","master_ojk_salary","master_promo_details","lender_repayment_setting","master_digital_signature_status","master_product_loan","master_country","master_bank","loan_lender","lender_loan_type","loan_app_status","lender_user","lender_stakeholder","lender_akta_history","asliri_verify_location","asliri_verify_tax_personal","asliri_verify_profesional","applicant_guarantor","applicant_bank_acc"]
export_tables_with_current_time = tables_with_current_time
tables = []       
for dimm2 in export_tables_with_current_time:
    Partition_Task2 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm2),
        sql = "SELECT TIMESTAMP(FORMAT_TIMESTAMP('%F %T', current_timestamp() , 'UTC+7')) as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm2),
        # export_query="SELECT * from {}".format(dimm2),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm2),
        dag = dag
    )
    tables.append(Partition_Task2)

tables_with_updated_at = ["user_secret","registrant_secret","lender_user_secret"]
export_tables_with_updated_at = tables_with_updated_at
tables = []       
for dimm3 in export_tables_with_updated_at:
    Partition_Task3 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm3),
        sql = "SELECT updated_at as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm3),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm3),
        dag = dag
    )
    tables.append(Partition_Task3)

tables_with_contract_creation_date = ["loan_app_legal"]
export_tables_with_contract_creation_date = tables_with_contract_creation_date
tables = []
for dimm4 in export_tables_with_contract_creation_date:
    Partition_Task4 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm4),
        sql = "SELECT contract_creation_date as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm4),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm4),
        dag = dag
    )
    tables.append(Partition_Task4)

tables_with_created_date = ["bni_disbursement"]
export_tables_with_created_date = tables_with_created_date
tables = []
for dimm5 in export_tables_with_created_date:
    Partition_Task5 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm5),
        sql = "SELECT created_date as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm5),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm5),
        dag = dag
    )
    tables.append(Partition_Task5)

tables_with_created_datetime = ["m_partner_credential","loan_repayment_borrower_schedule","loan_repayment_lender_schedule","loan_repayment_borrower_distributions"]
export_tables_with_created_datetime = tables_with_created_datetime
tables = []
for dimm6 in export_tables_with_created_datetime:
    Partition_Task6 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm6),
        sql = "SELECT created_datetime as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm6),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm6),
        dag = dag
    )
    tables.append(Partition_Task6)

tables_with_join_date = ["m_partner"]
export_tables_with_join_date = tables_with_join_date
tables = []
for dimm7 in export_tables_with_join_date:
    Partition_Task7 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm7),
        sql = "SELECT join_date as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm7),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm7),
        dag = dag
    )
    tables.append(Partition_Task7)

tables_with_LatestPaymentDate = ["data_upload"]
export_tables_with_LatestPaymentDate = tables_with_LatestPaymentDate
tables = []
for dimm8 in export_tables_with_LatestPaymentDate:
    Partition_Task8 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm8),
        sql = "SELECT LatestPaymentDate as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm8),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm8),
        dag = dag
    )
    tables.append(Partition_Task8)

tables_with_timestamp = ["email_out"]
export_tables_with_timestamp = tables_with_timestamp
tables = []
for dimm9 in export_tables_with_timestamp:
    Partition_Task9 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm9),
        sql = "SELECT timestamp as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm9),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm9),
        dag = dag
    )
    tables.append(Partition_Task9)

tables_with_approved_at = ["loan_app_details"]
export_tables_with_approved_at = tables_with_approved_at
tables = []
for dimm10 in export_tables_with_approved_at:
    Partition_Task10 = BigQueryOperator(
        task_id='{}_to_bigquery'.format(dimm10),
        sql = "SELECT approved_at as PARTITIONTIME,* FROM pintek-production.ProductionLayer.{}".format(dimm10),
        # export_query="SELECT * from {}".format(dimm),
        write_disposition = "WRITE_TRUNCATE",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False,
        destination_dataset_table='pintek-production.Staging.{}'.format(dimm10),
        dag = dag
    )
    tables.append(Partition_Task10)

Partition_Task2 >> Partition_Task3 >> Partition_Task4
Partition_Task5 >> Partition_Task6 >> Partition_Task7
Partition_Task8 >> Partition_Task9 >> Partition_Task10