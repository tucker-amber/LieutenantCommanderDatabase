import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 3)
}


sql_union='create table workfllow.Exports_Temp as select * from Aggriculture.Exports_2000' \
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

sql_remove='create table workflow.Exports_2000-2005 select * except(serialid, sort_code, net_sales_for_week_next_year, outstanding_sales_next_year, region_code,country_name)' \
			'from Aggriculture.Exports_Temp'

sql_date='select * except(week_ending_date), cast(week_ending_date AS DATE) as Date' \
		'from Aggriculture.Exports_2000_to_2005_reduced'

sql_countries='create table workfllow.Countries_Temp as' \
				'select distinct country_code, country_name, region_code' \
				'from Aggriculture.Exports_2000_to_2005' \
				'where country_code is not null' \
				'order by country_code' 

with models.DAG(
        'workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

	delete_dataset = BashOperator(
        task_id='delete_dataset',
        bash_command='bq rm -r -f workflow')
                
    create_dataset = BashOperator(
        task_id='create_dataset',
        bash_command='bq mk workflow')

	union_tables = BashOperator(
		task_id='union_tables',
		bash_command='bq query --use_legacy_sql=false "'+sql_union+'"')

	remove_columns = BashOperator(
		task_id='remove_columns',
		bash_command='bq query --use_legacy_sql=false "'+sql_remove+'"')

	cast_date = BashOperator(
		task_id='cast_date',
		bash_command='bq query --use_legacy_sql=false "'+sql_date+'"')

	create_countries = BashOperator(
		task_id='create_countries',
		bash_command='bq query --use_legacy_sql=false "'+sql_countries+'"')

	exports_beam = BashOperator(
       	task_id='exports_beam',
        bash_command='python pardo_Exports_2000_to_2005.py')

	countries_beam = BashOperator(
		task_id='countries_beam',
		bash_command='python Countries_cluster.py')

	delete_dataset>> create_dataset>> union_tables >> [cast_date,create_countries] >> remove_columns >> [exports_beam,countries_beam]






