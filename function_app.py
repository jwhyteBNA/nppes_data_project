import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
import time
from sqlalchemy import create_engine
import polars
import io

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def get_blob_service_client():
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        raise ValueError("AzureWebJobsStorage environment variable is not set.")
    return BlobServiceClient.from_connection_string(conn_str)


def get_postgres_connection():
    try:
        connection_string = (
            f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER')}:"
            f"{os.environ.get('POSTGRES_PASSWORD')}@"
            f"{os.environ.get('POSTGRES_HOST')}:"
            f"{os.environ.get('POSTGRES_PORT')}/"
            f"{os.environ.get('POSTGRES_DB')}"
        )
        connection = create_engine(connection_string)
        return connection
    except Exception as e:
        error_message = f"Failed to connect to PostgreSQL: {str(e)}"
        raise ValueError(error_message)


def extract_data_from_blob(filename):
    # may require polars chunking, may not depending on ram
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
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, blob=filename
        )

        blob_data = blob_client.download_blob().readall()
        file_in_memory = io.BytesIO(blob_data)
        df = polars.read_csv(file_in_memory, columns=relevant_columns)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def load_subsetted_blob_data_to_postgres(df, target_table, connection):
    try:
        df.write_database(target_table, connection, if_table_exists="replace")
    except Exception as e:
        print(f"Failed to write DataFrame to Postgres: {e}")
        raise


@app.route(route="NPPES_Data_Cleaning")
def NPPES_Data_Cleaning(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()  # Tick
    try:
        # Transformation Logic & Stored Procs from Main DB Table Here
        body = req.get_json()
        target_file = body.get("target_file")
        connection = get_postgres_connection()
        subsetted_blob_data_dataframe = extract_data_from_blob(target_file)
        load_subsetted_blob_data_to_postgres(
            subsetted_blob_data_dataframe,
            target_table="nppes_providers",
            connection=connection,
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
