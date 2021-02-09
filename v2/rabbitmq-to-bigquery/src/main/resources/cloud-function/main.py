def start_RabbitToBigQueryDataflowProcess(data, context):
	from googleapiclient.discovery import build
	from datetime import datetime
	#replace with your projectID
	project = "livelo-analytics"
	job = f'rabbitmqtobigquery{datetime.now().strftime("%Y%m%d%H%M%S%f")}'
	print(job)
	#path of the dataflow template on google storage bucket
	template = "gs://livelo-rabbitmq-data/templates/rabbitmq-to-bigquery-image-spec.json"
	print(data['name'])
	fileSplit = data['name'].split('-')
	if len(fileSplit) > 3 and fileSplit[2] == '00000':
		print(f"processing new dataflow instance: {job}")
		#user defined parameters to pass to the dataflow pipeline job
		parameters = {
			'rabbitUser' : 'T00199',
			'rabbitPassword': 'Livelo@2020',
			'rabbitHost': 'rabbitmq-transactional.k8s.dev.pontoslivelo.com.br',
			'rabbitPort': '30600',
			'rabbitQueue': 'dataliv.test',
			'rabbitMaxReadRecords': '50',
			'outputTableSpec': 'livelo-analytics:rabbitmq_stream_data.payload_history',
			'javascriptTextTransformGcsPath': 'gs://livelo-rabbitmq-data/udf-templates/transform.js',
			'javascriptTextTransformFunctionName': 'transform'
		}
		#tempLocation is the path on GCS to store temp files generated during the dataflow job
		environment = { 
			'subnetwork': 'https://www.googleapis.com/compute/v1/projects/livelo-network-prd/regions/southamerica-east1/subnetworks/vpc-prd',
			'ipConfiguration': 'WORKER_IP_PRIVATE',
			'workerRegion': 'southamerica-east1',
			'tempLocation': 'gs://livelo-rabbitmq-data/temp'
		}

		service = build('dataflow', 'v1b3', cache_discovery=False)
		#below API is used when we want to pass the location of the dataflow job
		request = service.projects().locations().flexTemplates().launch(
			projectId=project,
			location='southamerica-east1',
			body={
				'launchParameter': {
					'jobName': job,
					'containerSpecGcsPath': template,
					'parameters': parameters,
					'environment': environment,
				}
			}
		)
		response = request.execute()
		print(str(response))
	else:
		print("Not necessary to process a new dataflow")