def run_query_bigquery(project, query):
    from google.cloud import bigquery
    bigquery_client = bigquery.Client(project=project)

    job = bigquery_client.query(query)  # API request.
    return job.result()  # Waits for the query to finish.

def read_file(project, bucket, file):
    from google.cloud import storage

    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(file)
    contents = blob.download_as_text()
    contents = contents.replace("\n", "")
    contents = contents.replace("\t", "")
    return contents

def is_valid_filename(filename):
    if filename.startswith('clm') and filename.endswith('.txt'):
        return True
    return False

def start_bigquery_merge(data, context):
    project_name = 'livelo-analytics'

    print(data['name'])
    if is_valid_filename(data['name']):
        query = read_file(project_name, data['bucket'], data['name'])
        print(query)

        result = run_query_bigquery(project_name, query)
        print(result)
    else:
        print('Invalid file name')
	