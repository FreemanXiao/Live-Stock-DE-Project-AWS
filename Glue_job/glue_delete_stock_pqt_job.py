import sys
import json
import boto3

# Set table & bucket info 
BUCKET_TO_DEL = 'stock-information-parquet-bucket-2024-freeman'
DATABASE_TO_DEL = 'data_engineering_project_database'
TABLE_TO_DEL = 'stock_information_data_pqt_table'
QUERY_OUTPUT_BUCKET = 's3://athena-query-results-june-2024/'


# delete all objects in the bucket
s3_client = boto3.client('s3')

while True:
    objects = s3_client.list_objects(Bucket=BUCKET_TO_DEL)
    content = objects.get('Contents', [])
    if len(content) == 0:
        break
    for obj in content:
        s3_client.delete_object(Bucket=BUCKET_TO_DEL, Key=obj['Key'])


# drop the table too
client = boto3.client('athena')

queryStart = client.start_query_execution(
    QueryString = f"""
    DROP TABLE IF EXISTS {DATABASE_TO_DEL}.{TABLE_TO_DEL};
    """,
    QueryExecutionContext = {
        'Database': f'{DATABASE_TO_DEL}'
    }, 
    ResultConfiguration = { 'OutputLocation': f'{QUERY_OUTPUT_BUCKET}'}
)

# list of responses
resp = ["FAILED", "SUCCEEDED", "CANCELLED"]

# get the response
response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

# wait until query finishes
while response["QueryExecution"]["Status"]["State"] not in resp:
    response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])
    
# if it fails, exit and give the Athena error message in the logs
if response["QueryExecution"]["Status"]["State"] == 'FAILED':
    sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])
