# Milestone 5: ParDo on Exports_2000_to_2005
''' Req:
1. run BigQuery query on main dataset
2. make input PCollection from query result
3. write input PCollection to local file input.txt
4. apply DoFn through ParDo
5. write output PCollection to local file output.txt
6. write output PCollection to new BigQuery table in main dataset
''' 
import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn goes here

# Initalize project id
PROJECT_ID = os.environ['alien-grove-220420']

options = {
	'project': PROJECT_ID
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

	# 1 run BigQuery query on main dataset and 2 make input PCollection from query result
	query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='QUERY GOES HERE ex: SELECT * FROM oscars.Academy_Award')) # EDIT

	# 3 write input PCollection to local file input.txt
	query_results | 'Write querried raw data to input.txt' >> WriteToText('input.txt')

	# 4 apply DoFn through ParDo
	out_pcoll = query_results | 'Description of ParDo function (ex: Extract actor' >> beam.ParDo(NAMEOFFUNC()) #EDIT add doFn name and description

	# 5 write output PCollection to local file output.txt
	out_pcoll | 'Write transformed data to output.txt' >> WriteToText('output.txt')

	qualified_table_name = PROJECT_ID + ':aggriculture.NEW_TABLE_NAME' #EDIT add new table name
	table_schema = 'NEWTABLESCHEMAGOESHERE name:STRING,count:INTEGER' #EDIT add schema for new table

	# 6 write output PCollection to new BigQuery table in main dataset
	out_pcoll | 'Write transformed data to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))