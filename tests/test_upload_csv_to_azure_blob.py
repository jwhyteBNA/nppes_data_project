import pytest
import polars as pl
from function_app import upload_csv_to_azure_blob

def test_upload_csv_to_azure_blob(mocker):
    csv_data = "col1,col2\nval1,val3\nval2,val4\n"
    filename = "test_blob.csv"

    mock_blob_service_client = mocker.MagicMock()
    mock_blob_client = mocker.MagicMock()
    mock_blob_service_client.get_blob_client.return_value = mock_blob_client

    mocker.patch("function_app.get_blob_service_client", return_value=mock_blob_service_client)

    upload_csv_to_azure_blob(filename, csv_data)

    mock_blob_client.upload_blob.assert_called_once()
    args, kwargs = mock_blob_client.upload_blob.call_args
    assert args[0] == csv_data.encode("utf-8")
    assert kwargs.get("overwrite") is True