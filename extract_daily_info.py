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
        
        # Get the first date from the 'Time Series (Daily)' keys
        first_date = next(iter(r_dict['Time Series (Daily)']))
        first_data = r_dict['Time Series (Daily)'][first_date]
        
        processed_dict = {
            'Stock_ETF': symbol,
            'Date': first_date,
            'Open': first_data['1. open'],
            'High': first_data['2. high'],
            'Low': first_data['3. low'],
            'Close': first_data['4. close'],
            'Volume': first_data['5. volume']
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
    
    # Define API URLs ## I hide the personal API key to XXX. 
    api_urls = [
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=VOO&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=QQQ&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SCHG&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=GOOGL&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=META&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NVDA&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSLA&outputsize=compact&apikey=xxx"
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AMZN&outputsize=compact&apikey=xxx"
    ]
    # Fetch data and send to Firehose for APIs
    for api_url in api_urls:
        fetch_data_and_send_to_firehose(api_url)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Records sent successfully!')
    }
