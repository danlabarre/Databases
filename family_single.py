import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatNOPFn(beam.DoFn):
  def process(self, element):
    family_record = element
    hhx = family_record.get('household_id')
    fmx = family_record.get('family_id')
    year = family_record.get('year')
    num_phones = family_record.get('cell_phones')

    # change extreme values for number of cellphones in a household to -1 No Response. The max family size is 16, so assume values greater than 32 is unreasonable
    if num_phones > 32:
        num_phones = -1
    
    # create key, value pairs
    family_dict = {'year':year,'household_id':hhx,'family_id':fmx,"cell_phones":num_phones}
    return [family_dict]
           
         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is required when using the BQ source
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create beam pipeline using local runner
p = beam.Pipeline('DirectRunner', options=opts)

sql = 'SELECT year,household_id,family_id,cell_phones FROM National_Center_for_Health_Statistics_Modeled.family LIMIT 100'
bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

# write PCollection to log file
query_results | 'Write log 1' >> WriteToText('query_results.txt')

# apply ParDo to format the student's date of birth  
formatted_Family = query_results | 'Format NOP' >> beam.ParDo(FormatNOPFn())

# write PCollection to log file
formatted_Family | 'Write log 2' >> WriteToText('formatted_NOP.txt')

dataset_id = 'National_Center_for_Health_Statistics_Modeled'
table_id = 'family_Beam'
schema_id = 'year:INTEGER,household_id:INTEGER,family_id:INTEGER,cell_phones:INTEGER'

# write PCollection to new BQ table
formatted_Family | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(100)
                                                )
result = p.run()
result.wait_until_finish()
