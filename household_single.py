import os
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
                return [(index, housetype)]

class big_q(beam.DoFn):
    def process(self, element):
        index, house_type = element
        record = {'index': index, 'house_type': house_type}
        return [record]

PROJECT_ID = os.environ['PROJECT_ID']
options = {'project': PROJECT_ID}
opts = beam.pipeline.PipelineOptions(flag=[], **options)
with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT house_type from National_Center_for_Health_Statistics_Modeled.household group by house_type limit 100'))
    # write PCollection to log file
    query_results | 'Write to log' >> WriteToText('input.txt')
    # apply ParDo to the PCollection
    junction_pcoll = query_results | 'Creates Junction Table' >> beam.ParDo(junction())
    # write PCollection to log file
    junction_pcoll | 'Write Files' >> WriteToText('output.txt')
    out_pcoll = junction_pcoll | 'Make BQ Record' >> beam.ParDo(big_q())
    qualified_table_name = PROJECT_ID + ':National_Center_for_Health_Statistics_Modeled.household_house_type_junction_Beam'
    table_schema = 'index:INTEGER, house_type:STRING'
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
