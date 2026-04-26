from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



#this function returns the response object of a given input endpoint
def get_api_data(base_url, params = None, page_number = None):
    
    import requests
    
    #if page_number is added to the input, then add it to the params dictionary
    if page_number is not None:
        params["page"] = page_number
                
    #requests.get sends a GET request to the specified url
    #The get() method returns a requests.Response object.
    #This response object has methods and properties
    try:
        response = requests.get(base_url, params, timeout=30)
    except Exception as e:
        raise Exception(f"Error when making request: {str(e)}")
    return response
    

#this function gets the first 3 pages (due to the API free version limitaion) of the coingecko markets endpoint
#and writes them to the destination bronze layer. The destination file path is also returned to be consumed by other functions
#context refers to a dictionary of runtime information available to a task during its execution.
#it contains metadata about the current DAG run, the specific task instance, and the Airflow environment
def ingest_coins_data_batch(**context):
    
    import json
    import boto3
    from datetime import datetime
    import time

    now = datetime.utcnow() #get the current datetime
    
    ingestion_date = now.strftime("%Y-%m-%d") #get the date as string
    ingestion_ts_nodash = now.strftime("%Y%m%dT%H%M%S") #get timestamp without separators
    
    
    
    s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",  
    aws_access_key_id="minio",
    aws_secret_access_key="minio123")
     
    payload = []
        
    #this endpoint allows you to query all the supported coins with price, market cap, volume and market related data
    base_url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {'vs_currency':'usd','per_page': 250}
       
    for page_number in range(1,4):
    
        retries = 1

        while retries <= 3:
            
            response = get_api_data(base_url, params, page_number)
            is_success = response.ok #returns True if status_code is less than 400, otherwise False

            if is_success:
                time.sleep(5)
                data = response.json()
                if data:
                    #json.dumps to store raw API payload as a string, avoiding schema enforcement and ensuring safe schema evolution in Bronze
                    #if the coin is not stored as string, the processing engine (ex: spark) will see the data type as STRUCT not string.
                    #STRUCT will auto infer the schema leading to many nested fields. If changes happen to the API output,
                    #(columns removed, added, changed data type), #the STRUCT column will break because loading the data will be dependent
                    #on the API output
                    #now.isoformat() is used to be able to convert the python datetime object to json
                    payload.append({'page_number': page_number, 'ingestion_ts': now.isoformat(), 'source': 'Gecko_coins_markets_endpoint', 'data': json.dumps(data)})  
                    break
                else:
                    #success response without data from the current page onwards means that there is no data 
                    #from the source in the time being
                   raise Exception('response successful but there is no data from the source in the time being')          

            else:
                retries = retries + 1

        if retries > 3: #after the 3 retries without success, get the response code
            try: #check if there is an exception when trying to parse the error response code
                status_code = response.status_code
                error_body = response.json()

                #The dict.get(key, default) method in Python retrieves a value for a given key from a dictionary
                #Using dict.get(key) avoids the KeyError by returning a default value (or None)
                error_message = error_body.get("status", {}).get("error_message", "Unknown API error")

            except Exception:
                raise Exception(f"Failed to parse error response")
                
            else: 
                raise Exception(f"Failed on page {page_number} after {retries - 1} retries "
                                            f"with status code: {status_code} and message: {error_message}") 
    
    prefix = f"Bronze/coins_markets/ingestion_date={ingestion_date}/" #create a dynamic path
    run_id = context["run_id"] #get the DAG run id
    
    file_path = f"{prefix}coins_markets_{ingestion_ts_nodash}_{run_id}.json"
    
    s3.put_object(
        Bucket = 'warehouse',
        Key = file_path,
        Body = json.dumps(payload)
    )

    return file_path       
    
    
#this function gets the file_path returned from ingest_coins_data_batch as its input,
#makes the payload easier for processing by making each coin in one record, then writes the data 
#to S3 pointing to a stg hive table. This stg table will be used to insert to iceberg later.
def prepare_coins_data_batch(**context):
    
    import json
    import boto3
    import pandas as pd
    from io import BytesIO

    
    #an airflow object representing the current running task instance
    ti = context["ti"]
    
    #retrieve the return value from the extract task (ingest_coins_data_batch() function)
    bronze_file_key = ti.xcom_pull(task_ids="extract")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123"
    )

    #read the json file from the bronze layer
    bucket = 'warehouse'
    obj = s3.get_object(Bucket=bucket, Key=bronze_file_key)
    bronze_python = json.loads(obj["Body"].read()) #convert the json object to a python list

    coins_list = []
    
    for page in bronze_python:
        data_list = json.loads(page['data']) #convert the nested json coins object to a python list
        
        for coin in data_list:
            coins_list.append({'coin_id': coin['id'], 'symbol': coin['symbol'], 'name': coin['name'],
                           'current_price': coin['current_price'], 'market_cap': coin['market_cap'],
                           'total_volume': coin['total_volume'], 
                           'page_number': page['page_number'], 'ingestion_ts': page['ingestion_ts'],
                           'source': page['source']})
        
    df = pd.DataFrame(coins_list)
    

    #enforce data types that come from the API to avoid data type mismatch when inserting
    df = df.astype({
        "coin_id": "string",
        "symbol": "string",
        "name": "string",
        "current_price": "float64",
        "market_cap": "float64",
        "total_volume": "float64",
        "page_number": "int32"
    })
    df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"])

    prefix = f"silver/coins_markets/stg_data/"
    #the same file name is used in each run to overwrite the data in the stg hive table (it contains only the latest snapshot)
    silver_file_key = f"{prefix}coins_markets.parquet" 

    #create an in memory file like object to place 
    #the parquet file in the upload it to S3 because pandas does not work will with minio
    buffer = BytesIO() #create the memory buffer object
    df.to_parquet(buffer, index=False) #convert the df to parquet and place it in the memory buffer

    #upload the buffer to minio
    s3.put_object(
        Bucket=bucket,
        Key=silver_file_key,
        Body=buffer.getvalue() #extracts the binary data from the buffer to be sent in the Body of the put_object call.
    )

    return silver_file_key


#this funtion inserts the transformed data to the iceberg silver table using the stg hive table.
#Inserting is done this way because iceberg does NOT automatically read files 
#just because they exist in a folder (as similar to Hive). Iceberg needs commits since each commit creates
#a new metadata file that tracks snaphots and manifest files. iceberg requires all writes to go through 
#a compute engine (like trino or spark). In real world scenarios, spark is used to write to iceberg tables since 
#the current method uses pandas, pandas does not support writing to iceberg so i'm using trino for inserting, 
#but this method is not the preferred for large scale ingestion.

def load_to_iceberg_table(**context):

    import trino.dbapi
        
    conn = trino.dbapi.connect(
        host="trino",
        port=8080,
        user="trino",
        catalog="hive",
        schema="silver"
    )

    cursor = conn.cursor()

    #refresh the staging table after overwriting the silver parquet file in prepare_coins_data_batch()
    #cursor.execute("REFRESH TABLE hive.silver.stg_coins_markets")
    
    
    #the PK of the destination table is coin_id with ingestion_ts
    #merge ensures idempotency (running the same data twice yields the same result) 
    #both join conditions in the query are the PK of the table
    
    query = f"""
    MERGE INTO iceberg.silver.coins_markets t
    USING (
        SELECT *
        FROM hive.silver.stg_coins_markets
    ) s
    ON t.coin_id = s.coin_id
    AND t.ingestion_ts = s.ingestion_ts 

    WHEN MATCHED THEN UPDATE SET
        symbol = s.symbol,
        name = s.name,
        current_price = s.current_price,
        market_cap = s.market_cap,
        total_volume = s.total_volume,
        page_number = s.page_number,
        source = s.source

    WHEN NOT MATCHED THEN INSERT (
        coin_id, symbol, name, current_price,
        market_cap, total_volume, page_number,
        ingestion_ts, source
    )
    VALUES (
        s.coin_id, s.symbol, s.name, s.current_price,
        s.market_cap, s.total_volume, s.page_number,
        s.ingestion_ts, s.source
    )
    """
    
    #switch catalog from hive to iceberg
    cursor.execute("USE iceberg.silver")
    
    #run the merge query
    cursor.execute(query)



default_args = {
    "owner": "abdelrahman",
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="coingecko_lakehouse_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=ingest_coins_data_batch
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=prepare_coins_data_batch
    )
    
    load = PythonOperator(
        task_id="load",
        python_callable=load_to_iceberg_table
    )
    
    # This task triggers dbt to compile Jinja into SQL and execute it in Trino
    dbt_gold = BashOperator(
    task_id="dbt_gold",
    bash_command="""
    /home/airflow/.local/bin/dbt run \
      --project-dir /opt/airflow/dbt \
      --profiles-dir /opt/airflow/dbt \
      --target dev
    """,
)
    
    """
    Breakdown of the command:
      /home/airflow/.local/bin/dbt run \  # Run dbt using full path to avoid PATH issues in Airflow
      --project-dir /opt/airflow/dbt \  # Points to folder containing dbt_project.yml and models/
      --profiles-dir /opt/airflow/dbt \  # Points to folder containing profiles.yml (connection config)
      --target dev                      # Uses the "dev" target defined inside profiles.yml
    
    """
    extract >> transform >> load >> dbt_gold







"""
CREATE TABLE iceberg.silver.coins_markets (
    coin_id VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    current_price DOUBLE,
    market_cap DOUBLE,
    total_volume DOUBLE,
    page_number INT,
    ingestion_ts TIMESTAMP,
    source  VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(ingestion_ts)']
); 



CREATE TABLE hive.silver.stg_coins_markets (
    coin_id VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    current_price DOUBLE,
    market_cap DOUBLE,
    total_volume DOUBLE,
    page_number INTEGER,
    ingestion_ts TIMESTAMP,
    source VARCHAR
)
WITH (
    external_location = 's3a://warehouse/silver/coins_markets/stg_data/',
    format = 'PARQUET'
);


"""
