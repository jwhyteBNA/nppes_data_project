import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
import time
import psycopg2

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def get_blob_service_client():
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        raise ValueError("AzureWebJobsStorage environment variable is not set.")
    return BlobServiceClient.from_connection_string(conn_str)


def get_postgres_connection():
    try:
        connection_string = (
            f"postgresql://{os.environ.get('POSTGRES_USER')}:"
            f"{os.environ.get('POSTGRES_PASSWORD')}@"
            f"{os.environ.get('POSTGRES_HOST')}:"
            f"{os.environ.get('POSTGRES_PORT')}/"
            f"{os.environ.get('POSTGRES_DB')}"
        )
        connection = psycopg2.connect(connection_string)
        return connection
    except Exception as e:
        error_message = f"Failed to connect to PostgreSQL: {str(e)}"
        raise ValueError(error_message)


def extract_data_from_blob(filename):
    # may require polars chunking, may not depending on ram
    pass


@app.route(route="NPPES_Data_Cleaning")
def NPPES_Data_Cleaning(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()  # Tick
    target_file = req.params.get("nppes_sample.csv")
    try:
        # Transformation Logic & Stored Procs from Main DB Table Here
        extract_data_from_blob(target_file)

        connection = get_postgres_connection()
        cursor = connection.cursor()

        cursor.execute("SELECT version()")

        # Data processing goes here
        # 1. Read data from Azure Blob Storage
        # 2. Process/clean the data
        # 3. Insert into PostgreSQL using cursor.execute()

        cursor.close()
        connection.close()

        elapsed = time.time() - start_time  # Tock
        response = f"Elapsed time: {elapsed:.2f} seconds"
        return func.HttpResponse(response, status_code=200)
    except Exception as e:
        error_message = f"Internal server error: {str(e)}"
        return func.HttpResponse(error_message, status_code=500)
