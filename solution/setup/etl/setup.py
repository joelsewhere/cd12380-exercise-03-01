from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import BranchSQLOperator, SQLExecuteQueryOperator

DATABASES = [
    'raw',
    'analytics',
]

@dag
def setup():

    for database in DATABASES:

        @task_group(group_id=database)
        def group():

            check_exists = BranchSQLOperator(
                task_id="check_exists",
                conn_id="redshift_raw",
                sql="""
                    SELECT COUNT(*) > 0 
                    FROM pg_database 
                    WHERE datname = '{{ params.database }}';
                """,
                params={'database': database},
                follow_task_ids_if_true=[f'{database}.exists'],
                follow_task_ids_if_false=[f'{database}.create']
            )

            create = SQLExecuteQueryOperator(
                task_id="create",
                conn_id="redshift_raw",
                sql="CREATE DATABASE {{ params.database }};",
                params={"database": database}
                )
            
            exists = EmptyOperator(
                task_id="exists"
                )
            
            check_exists >> [create, exists]
            
        group()

setup()


