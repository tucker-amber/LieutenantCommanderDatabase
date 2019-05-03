import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 3)
}


sql_union='create table Aggriculture.Exports_Temp as select * from Aggriculture.Exports_2000' \
			'union all' \
			'select * from Aggriculture.Exports_2001' \
			'union all' \
			'select * from Aggriculture.Exports_2002' \
			'union all' \
			'select * from Aggriculture.Exports_2003' \
			'union all' \
			'select * from Aggriculture.Export_2004' \
			'union all' \
			'select * from Aggriculture.Exports_2005'

with models.DAG(
        'workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

	union_tables = BashOperator(
		task_id='union_tables',
		bash_command='bq query --use_legacy_sql=false "'+sql_union+'"')

	union_tables