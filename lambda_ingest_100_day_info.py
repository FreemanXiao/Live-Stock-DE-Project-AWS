import json
import boto3
import urllib3
from datetime import datetime

# REPLACE WITH YOUR DATA FIREHOSE NAME
FIREHOSE_NAME = 'PUT-S3-FvibE'

def lambda_handler(event, context):
    
    # Function to fetch data from an API and send to Firehose
    def fetch_data_and_send_to_firehose(api_url):
        http = urllib3.PoolManager()
        r = http.request("GET", api_url)
        r_dict = json.loads(r.data.decode(encoding='utf-8', errors='strict'))
        
        # Extract symbol
        symbol = r_dict['Meta Data']['2. Symbol']
        
        # Initialize boto3 client for Firehose
        fh = boto3.client('firehose')
        
        # Iterate over all dates in 'Time Series (Daily)' and send each record to Firehose
        for date, data in r_dict['Time Series (Daily)'].items():
            processed_dict = {
                'Stock_ETF': symbol,
                'Date': date,
                'Open': data['1. open'],
                'High': data['2. high'],
                'Low': data['3. low'],
                'Close': data['4. close'],
                'Volume': data['5. volume']
            }
            
            # Convert the dictionary to a JSON string
            msg = json.dumps(processed_dict) + '\n'
            
            # Send the record to Firehose
            reply = fh.put_record(
                DeliveryStreamName=FIREHOSE_NAME,
                Record={
                    'Data': msg
                }
            )
    
    # Define API URLs ## I replace the personal API key to xxx. 
    api_urls = [
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=VOO&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=QQQ&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SCHG&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=GOOGL&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=META&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NVDA&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSLA&outputsize=compact&apikey=xxx",
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AMZN&outputsize=compact&apikey=xxx"
    ]

    # Fetch data and send to Firehose for each API URL
    for api_url in api_urls:
        fetch_data_and_send_to_firehose(api_url)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Records sent successfully!')
    }
