import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:/2023 Plan B Data Engineering and Data Analysis/Final Project/pipeline-1133-96e19741fd40.json'

# Set the GCP project ID and BigQuery table name
project_id = 'pipeline-1133'
table_id = 'pipeline-1133.uber_dataset.uber_table'

# Define the schema of the BigQuery table
schema = 'DateTime:STRING,Lat:STRING,Lon:STRING,Base:STRING'

# Define the pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'pipeline-1133'
google_cloud_options.region = 'us-central1'
google_cloud_options.job_name = 'storage-bigquery2'
google_cloud_options.staging_location = 'gs://databucket_uber/staging'
google_cloud_options.temp_location = 'gs://databucket_uber/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(SetupOptions).save_main_session = True

pipeline = beam.Pipeline(options=options)

csv_lines = (pipeline
             | 'Read CSV' >> ReadFromText('gs://databucket_uber/uber_dataset.csv')
             | 'Parse CSV' >> beam.Map(lambda line: line.split(','))
             | 'Format to Dict' >> beam.Map(lambda fields: {'DateTime':(fields[0]), 'Lat': (fields[1]), 'Lon': (fields[2]), 'Base': fields[3]})
            )

csv_lines | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
    table=table_id,
    schema=schema,
    project=project_id
)

result = pipeline.run()