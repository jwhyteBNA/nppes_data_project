import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
import time
import polars
import io
import psycopg2
from io import StringIO

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def get_blob_service_client():
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        raise ValueError("AzureWebJobsStorage environment variable is not set.")
    return BlobServiceClient.from_connection_string(conn_str)


def get_psycopg2_connection():
    """Get direct psycopg2 connection for fast COPY operations"""
    try:
        connection_string = (
            f"host={os.environ.get('POSTGRES_HOST')} "
            f"port={os.environ.get('POSTGRES_PORT')} "
            f"dbname={os.environ.get('POSTGRES_DB')} "
            f"user={os.environ.get('POSTGRES_USER')} "
            f"password={os.environ.get('POSTGRES_PASSWORD')}"
        )
        return psycopg2.connect(connection_string)
    except Exception as e:
        error_message = f"Failed to connect to PostgreSQL with psycopg2: {str(e)}"
        raise ValueError(error_message)


def extract_data_from_blob(filename):
    CONTAINER_NAME = "nppes"
    relevant_columns = [
        "NPI",
        "Entity Type Code",
        "Provider Organization Name (Legal Business Name)",
        "Provider Last Name (Legal Name)",
        "Provider First Name",
        "Provider Middle Name",
        "Provider Name Prefix Text",
        "Provider Name Suffix Text",
        "Provider Credential Text",
        "Provider Other Organization Name",
        "Provider First Line Business Practice Location Address",
        "Provider Second Line Business Practice Location Address",
        "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name",
        "Provider Business Practice Location Address Postal Code",
        "Healthcare Provider Taxonomy Code_1",
        "Healthcare Provider Primary Taxonomy Switch_1",
        "Healthcare Provider Taxonomy Code_2",
        "Healthcare Provider Primary Taxonomy Switch_2",
        "Healthcare Provider Taxonomy Code_3",
        "Healthcare Provider Primary Taxonomy Switch_3",
        "Healthcare Provider Taxonomy Code_4",
        "Healthcare Provider Primary Taxonomy Switch_4",
        "Healthcare Provider Taxonomy Code_5",
        "Healthcare Provider Primary Taxonomy Switch_5",
        "Healthcare Provider Taxonomy Code_6",
        "Healthcare Provider Primary Taxonomy Switch_6",
        "Healthcare Provider Taxonomy Code_7",
        "Healthcare Provider Primary Taxonomy Switch_7",
        "Healthcare Provider Taxonomy Code_8",
        "Healthcare Provider Primary Taxonomy Switch_8",
        "Healthcare Provider Taxonomy Code_9",
        "Healthcare Provider Primary Taxonomy Switch_9",
        "Healthcare Provider Taxonomy Code_10",
        "Healthcare Provider Primary Taxonomy Switch_10",
        "Healthcare Provider Taxonomy Code_11",
        "Healthcare Provider Primary Taxonomy Switch_11",
        "Healthcare Provider Taxonomy Code_12",
        "Healthcare Provider Primary Taxonomy Switch_12",
        "Healthcare Provider Taxonomy Code_13",
        "Healthcare Provider Primary Taxonomy Switch_13",
        "Healthcare Provider Taxonomy Code_14",
        "Healthcare Provider Primary Taxonomy Switch_14",
        "Healthcare Provider Taxonomy Code_15",
        "Healthcare Provider Primary Taxonomy Switch_15",
    ]

    try:
        print(f"Starting chunked blob data extraction for file: {filename}")
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, blob=filename
        )

        file_buffer = io.BytesIO()

        print("Downloading blob data...")
        blob_stream = blob_client.download_blob()
        print(f"Blob download complete. Data size: {len(blob_stream):,} bytes")

        for chunk in blob_stream.chunks():
            file_buffer.write(chunk)

        # Reset buffer position to beginning
        file_buffer.seek(0)

        print("Creating lazy CSV scan...")
        lazy_df = polars.scan_csv(file_buffer).select(relevant_columns)

        return lazy_df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def load_chunked_blob_data_to_postgres(
    lazy_df, target_table, chunk_size=100_000
):
    """Fast bulk loading using psycopg2 COPY instead of slow write_database"""
    try:
        print(f"Starting to load data to {target_table} in chunks of {chunk_size:,}")
        
        # Get direct psycopg2 connection for COPY
        pg_conn = get_psycopg2_connection()
        
        chunk_count = 0
        total_rows_processed = 0
        
        # Process in streaming chunks without counting total rows first
        while True:
            chunk_count += 1
            
            # Get chunk using streaming approach
            batch_df = lazy_df.slice(total_rows_processed, chunk_size).collect()
            
            if batch_df.is_empty():
                print("No more data to process")
                break
                
            current_chunk_size = len(batch_df)
            print(f"Processing chunk {chunk_count} ({current_chunk_size:,} rows)")
            
            # Convert to CSV in memory for COPY
            output = StringIO()
            batch_df.write_csv(output, separator='\t', include_header=False)
            output.seek(0)
            
            # Use COPY for blazing fast bulk insert
            with pg_conn.cursor() as cursor:
                try:
                    if chunk_count == 1:
                        # Truncate table on first chunk if needed
                        cursor.execute(f"TRUNCATE TABLE {target_table}")
                    
                    # Bulk copy data
                    cursor.copy_from(
                        output, 
                        target_table,
                        columns=batch_df.columns,
                        sep='\t',
                        null=''
                    )
                    pg_conn.commit()
                    
                    total_rows_processed += current_chunk_size
                    print(f"[SUCCESS] Successfully loaded chunk {chunk_count} ({current_chunk_size:,} rows)")
                    
                except Exception as e:
                    pg_conn.rollback()
                    print(f"Error loading chunk {chunk_count}: {e}")
                    raise
            
            # Break if we got less than chunk_size (last chunk)
            if current_chunk_size < chunk_size:
                break
        
        pg_conn.close()
        print(f"COMPLETE: Successfully loaded all {total_rows_processed:,} rows to {target_table}")
        
    except Exception as e:
        print(f"Failed to write DataFrame to Postgres: {e}")
        if 'pg_conn' in locals():
            pg_conn.close()
        raise


@app.route(route="NPPES_Data_Cleaning")
def NPPES_Data_Cleaning(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()  # Tick
    try:
        # Transformation Logic & Stored Procs from Main DB Table Here
        body = req.get_json()
        target_file = body.get("target_file")

        lazy_df = extract_data_from_blob(target_file)
        if lazy_df is not None:
            load_chunked_blob_data_to_postgres(
                lazy_df,
                target_table="nppes_providers",
                chunk_size=50_000,  # Now matches the function signature
            )

        # Data processing goes here
        # 1. Read data from Azure Blob Storage
        # 2. Process/clean the data
        # 3. Insert into PostgreSQL using cursor.execute()

        elapsed = time.time() - start_time  # Tock
        response = f"Elapsed time: {elapsed:.2f} seconds"
        return func.HttpResponse(response, status_code=200)
    except Exception as e:
        error_message = f"Internal server error: {str(e)}"
        return func.HttpResponse(error_message, status_code=500)
