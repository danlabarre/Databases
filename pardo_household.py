import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class clean_up(beam.DoFn):
    # Transforms the house_type field into an intiger to be matched with a junction table
    def process(self, element):
        record = element
        household_id = record.get('household_id')
        final_weight = record.get('final_weight')
        house_type = record.get('house_type')
        responsive_family = record.get('responsive_family')
        non_responsive_family = record.get('non_responsive_family')
        responsive_people = record.get('responsive_people')
        non_responsive_people = record.get('non_responsive_people')
        responsive_children = record.get('responsive_children')
        region = record.get('region')
        if house_type == 'House, apartment, flat, condo':
            house_type = 1
        elif house_type == 'Student quarters in college dormitory':
            house_type = 2
        elif house_type == 'Mobile home/trailer w/no permanent rooms added':
            house_type = 3
        elif house_type == 'Mobile home/trailer w/1+ permanent rooms added':
            house_type = 4
        elif house_type == 'Group quarter unit not specified above':
            house_type = 5
        elif house_type == 'HU not specified above':
            house_type = 6
        elif house_type == 'HU in rooming house':
            house_type = 7
        elif house_type == 'Quarters not HU in room or board house':
            house_type = 8
        elif house_type == 'Not ascertained':
            house_type = 9
        elif house_type == 'HU in nontransient hotel, motel':
            house_type = 10
        elif house_type == 'Unit not permanent-transient hotel, motel':
            house_type = 11
        elif house_type == 'HU-permanent in transient hotel, motel':
            house_type = 12
        return [(household_id, final_weight, house_type, responsive_family, non_responsive_family, responsive_people, non_responsive_people, responsive_children, region)]

class big_q(beam.DoFn):
    # Puts the new values into a dictionary to be uploaded into BigQuery
    def process(self, element):
        household_id, final_weight, house_type, responsive_family, non_responsive_family, responsive_people, non_responsive_people, responsive_children, region = element
        record = {'household_id': household_id, 'final_weight': final_weight, 'house_type': house_type, 'responsive_family': responsive_family, 'non_responsive_family': non_responsive_family, 'responsive_people': responsive_people, 'non_responsive_people': non_responsive_people, 'responsive_children': responsive_children, 'region': region}
        return [record]


PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
            'project': PROJECT_ID
            }
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM National_Center_for_Health_Statistics_Modeled.household'))

    # write PCollection to log file
    query_results | 'Write to log' >> WriteToText('input.txt')

    # apply ParDo to the PCollection 
    cln_pcoll = query_results | 'Change house_type' >> beam.ParDo(clean_up())

    
    # write PCollection to log file
    cln_pcoll | 'Write Files' >> WriteToText('output.txt')


    out_pcoll = cln_pcoll | 'Make BQ Record' >> beam.ParDo(big_q())
                                             
    qualified_table_name = PROJECT_ID + ':National_Center_for_Health_Statistics_Modeled.cleaned_household' 
    table_schema = 'household_id:INTEGER,final_weight:INTEGER,house_type:INTEGER,responsive_family:INTEGER,non_responsive_family:INTEGER,responsive_people:INTEGER,non_responsive_people:INTEGER,responsive_children:INTEGER,region:STRING'
                                                                              
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
