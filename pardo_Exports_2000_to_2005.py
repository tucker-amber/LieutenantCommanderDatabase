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

# Initalized dictionary of corresponding numerical country codes (key) and string country codes (values)
countryCodeDict = {'0000':'REGION', '1220':'CANADA', '2010':'MEXICO', '2470':'DOM REP', '2740':'TRINID', '2770':'N ANTIL', '2830': 'F W IND', '3070': 'VENEZ', '3150':'SURINAM', '3330':'PERU', '3770':
'CHILI', '3510':'BRAZIL', '3550':'URUGUAY', '3570':'3570', '5490':'THAILND','5600': 'INDNSIA', '5800': 'KOR REP', '5820':'HG KONG', '9110': 'VIRGIN I'}

# Replace all instances of numeric country codes with string country codes
class removeNumericCountryCode(beam.DoFn):
    def process(self, element):
        record = element
        country_code = record.get('country_code')
        # check to see if country_code is numeric
        if country_code.isdigit():
            # Get all other attributes of transaction
            commodity_code = record.get('commodity_code')
            current_week_export = record.get('current_week_export')
            accumulated_exports_current_year = record.get('accumulated_exports_current_year')
            outstanding_sales_current_year = record.get('outstanding_sales_current_year')
            total_outstanding_sales_exports_as_of_current_week = record.get('total_outstanding_sales_exports_as_of_current_week')
            net_sales_for_week_current_year = record.get('net_sales_for_week_current_year')
            
            # replace numeric value with string value by comparing numeric key to country code dict
            country_code = countryCodeDict.get(country_code)

            new_record = {'commodity_code': commodity_code, 'country_code': country_code, 'current_week_export': current_week_export, 'accumulated_exports_current_year': accumulated_exports_current_year, 'outstanding_sales_current_year': outstanding_sales_current_year, 'total_outstanding_sales_exports_as_of_current_week': total_outstanding_sales_exports_as_of_current_week, 'net_sales_for_week_current_year': net_sales_for_week_current_year}
            # return record with updated country_code
            return [new_record]
        # otherwise do not alter record, return as is
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
    # Query Exports_2000_to_2005 table. selects all attributes.
    query_results_Exports_2000_to_2005 = p | 'Read from Exports table BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='select * from Aggriculture.Exports_2000_to_2005 LIMIT 100')) # currently limiting to 100 for testing
	
    # 3 write input PCollection to local file input.txt
    query_results_Exports_2000_to_2005 | 'Write querried raw data to input.txt' >> WriteToText('input.txt')

	# 4 apply DoFn through ParDo
    out_pcoll = query_results_Exports_2000_to_2005 | 'Replace instances of country_code where it is numeric with string country_code' >> beam.ParDo(removeNumericCountryCode())

	# 5 write output PCollection to local file output.txt
	out_pcoll | 'Write transformed data to output.txt' >> WriteToText('output.txt')

	qualified_table_name = PROJECT_ID + ':Aggriculture.test'
	table_schema = 'commodity_code:STRING,country_code:STRING,current_week_export:INTEGER,accumulated_exports_current_year:INTEGER,outstanding_sales_current_year:INTEGER,total_outstanding_sales_exports_as_of_current_week:INTEGER,net_sales_for_week_current_year:INTEGER,date:DATE' #EDIT add schema for new table

	# 6 write output PCollection to new BigQuery table in main dataset
	out_pcoll | 'Write transformed data to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
