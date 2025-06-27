import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def get_blob_service_client():
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        raise ValueError("AzureWebJobsStorage environment variable is not set.")
    return BlobServiceClient.from_connection_string(conn_str)


# this fn can be deleted or moved later
def test_blob_service_client():
    try:
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client("nppes")
        # List blobs in the 'nppes' container to verify connection
        blobs = container_client.list_blobs()
        blob_names = [blob.name for blob in blobs]
        return f"Connected successfully to 'nppes' container. Blobs: {blob_names}"
    except Exception as e:
        return f"Failed to connect to 'nppes' container: {str(e)}"


# add target connection (postgres)


# download functions (azurite)
# paginating in chunks


@app.route(route="NPPES_Data_Cleaning")
def NPPES_Data_Cleaning(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()  # Tick
    try:
        # Transformation Logic & Stored Procs from Main DB Table Here

        result = test_blob_service_client()
        elapsed = time.time() - start_time  # Tock
        response = f"{result}\nElapsed time: {elapsed:.2f} seconds"
        return func.HttpResponse(response, status_code=200)
    except Exception as e:
        error_message = f"Internal server error: {str(e)}"
        return func.HttpResponse(error_message, status_code=500)
