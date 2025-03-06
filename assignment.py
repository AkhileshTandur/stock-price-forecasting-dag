from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests

# Constants
SNOWFLAKE_CONN= "snowflake_conn"
SYMBOL = "NVDA"

# Function to get Snowflake cursor
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    return hook.get_conn().cursor()

# Task to extract data from Alpha Vantage
@task
def extract_data():
    # Get API key from Airflow Variables
    api_key = Variable.get("VANTAGE_API_KEY")
    
    # API request
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&outputsize=full&apikey={api_key}"
    response = requests.get(url)
    
    # Check if response was successful 
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    
    data = response.json()
    
    # Validate that we have the expected data structure
    if "Time Series (Daily)" not in data:
        raise Exception(f"Unexpected API response format: {data}")
    
    return data

# Task to transform data
@task
def transform_data(data):
    records = []
    time_series = data.get('Time Series (Daily)', {})
    
    for date, values in time_series.items():
        try:
            record = {
                'symbol': SYMBOL,
                'date': date,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume'])
            }
            records.append(record)
        except (KeyError, ValueError) as e:
            print(f"Error processing record for date {date}: {str(e)}")
    
    return records

#@task
@task
def load_data(records):
    cursor = get_snowflake_cursor()
    table = "DEV.RAW.STOCK_PRICE"

    try:
        if not records:
            print(" No records to insert.")
            return

        cursor.execute("BEGIN;")
        cursor.execute(f'DELETE FROM {table};')

        query = f"""
            INSERT INTO {table} 
            (SYMBOL, "DATE", "OPEN", HIGH, LOW, CLOSE, VOLUME) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        formatted_records = [
            (r["symbol"], r["date"], float(r["open"]), float(r["high"]), 
             float(r["low"]), float(r["close"]), int(r["volume"])) for r in records
        ]

        cursor.executemany(query, formatted_records)
        cursor.execute("COMMIT;")
        print(f" Inserted {len(formatted_records)} records into {table}")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(f" Error: {e}")
        raise e

    finally:
        cursor.close()


# Define the DAG
with DAG(
    dag_id="stock_price_pipeline_OGS",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # Runs daily
    catchup=False
) as dag:
    
    # Extract-Transform-Load Pipeline
    extracting_data = extract_data()
    transformed_data = transform_data(extracting_data)
    load_task = load_data(transformed_data)
    # view_task = create_market_data_view()
    
    # Set dependencies
    extracting_data >> transformed_data >>load_task #>> view_task