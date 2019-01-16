from google.cloud import bigquery

client = bigquery.Client.from_service_account_json('service_account.json')
filename = str(cachePath(f"""{date}/author-subbreddit-pairs-IDs.gzip"""))
dataset_id = '2015_IDs'
table_id = '11'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
#job_config.source_format = bigquery.SourceFormat.GZIP
job_config.skip_leading_rows = 1
job_config.autodetect = True

job = client.load_table_from_dataframe(df, table_ref, num_retries=6, 
job_id=None, job_id_prefix=None, location='US', job_config=job_config)

with open(filename, 'rb') as source_file:
    
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_id, table_id))