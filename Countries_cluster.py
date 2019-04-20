import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class removeCountryCode(beam.DoFn):
  def process(self, element):
        record = element
        country_code = record.get('country_code')
        country_name = record.get('country_name')
        region_code = record.get('region_code')

        #Create new record by removing the country code
        new_record = {'country_name': country_name, 'region_code': region_code}

        return [new_record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

    # Query Countries table on big query selecting all attributes
    query_results_Countries= p | 'Read from Countries table BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='select * from Aggriculture.Countries')) 
    
    # Write input PCollection to local file input.txt
    query_results_Countries | 'Write querried raw data to input.txt' >> WriteToText('input.txt')

    # Apply DoFn through ParDo
    out_pcoll = query_results_Countries | 'Replace instances of country_code where it is numeric with string country_code' >> beam.ParDo(removeCountryCode())

    # Write output PCollection to local file output.txt
    out_pcoll | 'Write transformed data to output.txt' >> WriteToText('output.txt')

    #Create new table
    qualified_table_name = PROJECT_ID + ':Aggriculture.CountriesMS6'
    table_schema = 'country_name:STRING,region_code:INTEGER'

    # Write output PCollection to new BigQuery table in main dataset
    out_pcoll | 'Write transformed data to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))