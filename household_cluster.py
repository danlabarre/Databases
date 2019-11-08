import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
class junction(beam.DoFn):
    def process(self, element):
        record = element
        housetype = record.get('house_type')
        if housetype == "House, apartment, flat, condo":
            index = 1
        elif housetype == "Student quarters in college dormitory":
            index = 2
        elif housetype == "Mobile home/trailer w/no permanent rooms added":
            index = 3
        elif housetype == "Mobile home/trailer w/1+ permanent rooms added":
            index = 4
        elif housetype == "Group quarter unit not specified above":
            index = 5
        elif housetype == "HU not specified above":
            index = 6
        elif housetype == "HU in rooming house":
            index = 7
        elif housetype == "Quarters not HU in room or board house":
            index = 8
        elif housetype == "Not ascertained":
            index = 9
        elif housetype == "HU in nontransient hotel, motel":
            index = 10
        elif housetype == "Unit not permanent-transient hotel, motel":
            index = 11
        elif housetype == "HU-permanent in transient hotel, motel":
            index = 12
        junction_tuple = (index, housetype) 
        return [junction_tuple]

class big_q(beam.DoFn):
    def process(self, element):
        index, house_type = element
        record = {'index': index, 'house_type': house_type}
        return [record]

PROJECT_ID= os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

options = {'runner': 'DataflowRunner', 'job_name': 'junction', 'project': PROJECT_ID, 'temp_location': BUCKET + '/temp', 'staging_location': BUCKET + '/staging', 'machine_type': 'n1-standard-4', 'num_workers': 1}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

p = beam.Pipeline('DataflowRunner', options=opts)

sql = 'SELECT house_type from National_Center_for_Health_Statistics_Modeled.household group by house_type'
bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

junction_pcoll = query_results | 'Creates Junction Table' >> beam.ParDo(junction())

junction_pcoll | 'Write Files' >> WriteToText(DIR_PATH + 'output.txt')

push_to_BQ = junction_pcoll | 'Gets data ready for BQ push' >> beam.ParDo(big_q())

dataset_id = 'National_Center_for_Health_Statistics_Modeled'
table_id = 'household_house_type_junction_Beam_DF'
schema_id = 'index:INTEGER, house_type:STRING'

# write PCollection to new BQ table
push_to_BQ | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, table=table_id, schema=schema_id, project=PROJECT_ID, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, batch_size=int(100))

result = p.run()
result.wait_until_finish()
