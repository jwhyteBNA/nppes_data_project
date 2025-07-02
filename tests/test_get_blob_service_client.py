import pytest
import polars
from function_app import get_blob_service_client

def test_get_blob_service_client(mocker):
    mocker.patch.dict('os.environ', {'AzureWebJobsStorage': 'mock_connection_string'})
    mock_blob_service_client = mocker.patch('function_app.BlobServiceClient')
    mock_blob_service_client.from_connection_string.return_value = 'mock_client'

    client = get_blob_service_client()

    assert client == 'mock_client'
    mock_blob_service_client.from_connection_string.assert_called_once_with('mock_connection_string')