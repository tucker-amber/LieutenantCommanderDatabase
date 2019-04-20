import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# Replace all instances of numeric country codes with string country codes
class updateImports(beam.DoFn):
    def process(self, element):
        record = element
        Year = record.get('Year')
        Reporter = record.get('Reporter')
        Partner = record.get('Partner')
        Commodity = record.get('Commodity')
        Qty_Unit = record.get('Qty_Unit')
        Alt_Qty_Unit = record.get('Alt_Qty_Unit')
        Netweight__kg_ = record.get('Netweight__kg_')
        if Netweight__kg_.isdigit():
            pass
        else:
            Netweight__kg_ = 0

        Trade_Value__US__ = record.get('Trade_Value__US__')

        #Create new record by removing the country code
        new_record = {'Year': Year, 'Reporter': Reporter, 'Partner': Partner, 'Commodity': Commodity, 'Qty_Unit': Qty_Unit, 'Alt_Qty_Unit': Alt_Qty_Unit, 'Netweight__kg_': Netweight__kg_, 'Trade_Value__US__': Trade_Value__US__}

        return [new_record]
# Initalize project id
PROJECT_ID = os.environ['PROJECT_ID']

options = {
	'project': PROJECT_ID
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:


    # Query Countries table on big query selecting all attributes
    query_results_Countries= p | 'Read from Import table BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='select * from Aggriculture.Imports_All_Raw LIMIT 100')) # EDIT! currently limiting to 100 for testing
	
    # Write input PCollection to local file input.txt
    query_results_Countries | 'Write querried raw data to input.txt' >> WriteToText('input.txt')

	# Apply DoFn through ParDo
    out_pcoll = query_results_Countries | 'Clean imports table' >> beam.ParDo(updateImports())

	# Write output PCollection to local file output.txt
    out_pcoll | 'Write transformed data to output.txt' >> WriteToText('output.txt')

    #Create new table
    qualified_table_name = PROJECT_ID + ':Aggriculture.Imports_MS10'
    table_schema = 'Year:INTEGER,Reporter:STRING,Partner:STRING,Commodity:STRING,Qty_Unit:STRING,Alt_Qty_Unit:INTEGER,Netweight__kg_:INTEGER,Trade_Value__US__:INTEGER'

	# Write output PCollection to new BigQuery table in main dataset
    out_pcoll | 'Write transformed data to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
