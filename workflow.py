import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 3)
}


sql_Union= 'create table Aggriculture_workflow.Exports_Temp as SELECT * FROM Aggriculture.Exports_2000' \
'UNION ALL' \
'SELECT * FROM Aggriculture.Exports_2001' \
'UNION ALL' \
'SELECT * FROM Aggriculture.Exports_2002 '\
'UNION ALL' \
'SELECT * FROM Aggriculture.Exports_2003' \
'UNION ALL' \
'SELECT * FROM Aggriculture.Export_2004' \
'UNION ALL' \
'SELECT * FROM Aggriculture.Exports_2005'

with models.DAG(
        'workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

	union_tables = BashOperator(
		task_id='union_tables',
		bash_command='bq query --use_legacy_swl=false "'+sql_Union +'"')