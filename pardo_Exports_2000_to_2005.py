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

# DoFn goes here.
class removeNumericCountryCode(beam.DoFn):
    def process(self, element):
        record = element
        country_code = record.get('country_code')
        # check to see if country_code is numeric
        if country_code.isdigit():
            # replace numeric value with string value EDIT
            return [element] #EDIT
        # otherwise do not alter record
        else:
            return [element]


# Initalize project id
PROJECT_ID = os.environ['PROJECT_ID']

options = {
	'project': PROJECT_ID
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

	# 1-2 make input PCollection from BigQuery query result
        # Query countries table to find inconsistent country codes (where one country has both str and numeric pk)
        # code1 = numeric, code2 = string
	query_results_countries_table = p | 'Read from Countries table in BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='select a.country_code as code1, b.country_code as code2 from Aggriculture.Countries a join Aggriculture.Countries b on a.country_name == b.country_name where a.country_code != b.country_code LIMIT 29'))

        # Query Exports_2000_to_2005 table. selects all attributes.
        query_results_Exports_2000_to_2005 = p | 'Read from Exports table BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='select * from Aggriculture.Exports_2000_to_2005 LIMIT 100')) # currently limiting to 100 for testing
	# 3 write input PCollection to local file input.txt
	query_results_countries_table | 'Write querried raw data to input.txt' >> WriteToText('input.txt')
        query_results_Exports_2000_to_2005 | 'Write querried raw data to input2.txt' >> WriteToText('input2.txt')

	# 4 apply DoFn through ParDo
        out_pcoll = query_results_Exports_2000_to_2005 | 'Description of ParDo function (ex: Extract actor' >> beam.ParDo(removeNumericCountryCode())

	# 5 write output PCollection to local file output.txt
	out_pcoll | 'Write transformed data to output.txt' >> WriteToText('output.txt')
'''
	qualified_table_name = PROJECT_ID + ':aggriculture.NEW_TABLE_NAME' #EDIT add new table name
	table_schema = 'NEWTABLESCHEMAGOESHERE name:STRING,count:INTEGER' #EDIT add schema for new table

	# 6 write output PCollection to new BigQuery table in main dataset
	out_pcoll | 'Write transformed data to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
	'''
