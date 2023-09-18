# Importation des modules Airflow nécessaires
from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import sys

# Ajout du chemin d'accès à des fichiers personnalisés
sys.path.append('/opt/airflow/includes')
import queries  # Importation des requêtes SQL personnalisées
from emp_dim_insert_update import join_and_detect_new_or_changed_rows  # Importation d'une fonction personnalisée

# Définition de fonctions pour vérifier s'il y a des IDs à insérer ou à mettre à jour avant l'insertion et la mise à jour
def check_ids_to_update(**context):
    ids_to_update = context['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if ids_to_update == '':
        return 'check_rows_to_insert'
    else:
        return 'snowflake_update_task'

def check_rows_to_insert(**context):
    rows_to_insert = context['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert")
    if rows_to_insert is None:
        return 'skip_snowflake_insert_task'
    else:
        return 'snowflake_insert_task'

# Création du DAG Airflow
with DAG("ETL_Dag", start_date=datetime(2023, 5, 12), catchup=False, schedule='@hourly') as Dag:

    # Tâche pour extraire les données financières et les stocker dans AWS S3
    extract_finance = SqlToS3Operator(
        task_id="extract_finance",
        sql_conn_id="PostgreSQL_conn",
        aws_conn_id="AWS_S3_conn",
        query=queries.SELECT_EMP_SAL,
        s3_bucket="staging.emp.data",
        s3_key="Dina_emp_data.csv",
        replace=True
    )
    
    # Tâche pour extraire les données RH et les stocker dans AWS S3
    extract_hr = SqlToS3Operator(
        task_id="extract_hr",
        sql_conn_id="PostgreSQL_conn",
        aws_conn_id="AWS_S3_conn",
        query=queries.SELECT_EMP_DETAIL,
        s3_bucket="staging.emp.data",
        s3_key="Dina_hr_sal.csv",
        replace=True
    )
    
    # Tâche pour appliquer un script Python afin de trouver les nouveaux enregistrements et les mises à jour
    join_and_detect_task = join_and_detect_new_or_changed_rows()
    
    # Tâche pour insérer les nouveaux enregistrements
    snowflake_insert_task = SnowflakeOperator(
        task_id='snowflake_insert_task',
        sql=queries.INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id="snowflake_conn",
        trigger_rule="none_failed"
    )
    
    # Tâche pour mettre à jour les enregistrements modifiés
    snowflake_update_task = SnowflakeOperator(
        task_id='snowflake_update_task',
        sql=queries.UPDATE_DWH_EMP_DIM('{{ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")}}'),
        snowflake_conn_id="snowflake_conn",
    )
    
    # Tâche pour vérifier s'il y a des IDs à mettre à jour avant de réaliser la mise à jour
    check_ids_to_update_task = BranchPythonOperator(
        task_id='check_ids_to_update',
        python_callable=check_ids_to_update,
        provide_context=True
    )
    
    # Tâche pour vérifier s'il y a des IDs à insérer avant de réaliser la tâche d'insertion
    check_rows_to_insert_task = BranchPythonOperator(
        task_id='check_rows_to_insert',
        python_callable=check_rows_to_insert,
        provide_context=True
    )
    
    # Ordonnancement des tâches
    [extract_finance, extract_hr] >> join_and_detect_task >> check_ids_to_update_task >> [snowflake_update_task, check_rows_to_insert_task]
    check_rows_to_insert_task >> snowflake_insert_task
    snowflake_update_task >> snowflake_insert_task
