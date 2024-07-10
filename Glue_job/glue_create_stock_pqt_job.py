import sys
import boto3

client = boto3.client('athena')

SOURCE_TABLE_NAME = 'stockstock_information_result_2024_freeman'
NEW_TABLE_NAME = 'stock_information_data_pqt_table'
NEW_TABLE_S3_BUCKET = 's3://stock-information-parquet-bucket-2024-freeman/'
MY_DATABASE = 'data_engineering_project_database'
QUERY_RESULTS_S3_BUCKET = 's3://athena-query-results-june-2024/'

# Refresh the table
queryStart = client.start_query_execution (
    QueryString = f"""
    CREATE TABLE {NEW_TABLE_NAME} WITH
    (external_location='{NEW_TABLE_S3_BUCKET}',
    format='PARQUET',
    write_compression='SNAPPY',
    partitioned_by = ARRAY['stock_etf_partition'])
    AS
    
    SELECT * FROM (
    WITH all_data AS (
        SELECT 
            stock_etf,
            date AS full_date,
            date_format(date_parse(date, '%Y-%m-%d'), '%W') AS day_of_week,
            Partition_0 AS Year,
            Partition_1 AS Month,
            Partition_2 AS Day,
            ROUND(CAST(open AS DECIMAL(10, 2)),2) AS Open,
            ROUND(CAST(high AS DECIMAL(10, 2)),2) AS High,
            ROUND(CAST(low AS DECIMAL(10, 2)), 2) AS Low,
            ROUND(CAST(close AS DECIMAL(10, 2)),2) AS Close,
            CAST(volume AS integer) AS volume,
            ROW_NUMBER() OVER (PARTITION BY stock_etf, date ORDER BY date) AS rank_number,
            stock_etf AS stock_etf_partition
        FROM stockstock_information_result_2024_freeman),
    Transfer_Nvda AS (
        SELECT 
            stock_etf,
            full_date,
            day_of_week,
            Year,
            Month,
            Day,
            ROUND(Open/10,2) AS Open,
            ROUND(High/10,2) AS High,
            ROUND(Low/10,2) AS Low,
            ROUND(Close/10,2) AS Close,
            volume,
            rank_number,
            stock_etf_partition
        FROM 
            all_data
        WHERE stock_etf = 'NVDA' AND full_date < '2024-06-08')
    
    SELECT * 
    FROM all_data
    WHERE  
        rank_number = 1 AND
        (stock_etf != 'NVDA' or
        (stock_etf = 'NVDA' AND full_date > '2024-06-08'))
        
    UNION ALL 
    
    SELECT * 
    FROM Transfer_Nvda
    WHERE rank_number = 1 )

    ;
    """,
    QueryExecutionContext = {
        'Database': f'{MY_DATABASE}'
    }, 
    ResultConfiguration = { 'OutputLocation': f'{QUERY_RESULTS_S3_BUCKET}'}
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
