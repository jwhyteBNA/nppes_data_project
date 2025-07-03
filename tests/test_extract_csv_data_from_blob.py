import pytest
import polars as pl
from function_app import extract_csv_data_from_blob, get_blob_service_client


class DummyBlobStream:
    def chunks(self):
        # Provide real CSV bytes
        yield b"col1,col2\nval1,val2\nval3,val4\n"


class DummyBlobClient:
    def download_blob(self):
        return DummyBlobStream()


class DummyBlobServiceClient:
    def get_blob_client(self, container, blob):
        return DummyBlobClient()


@pytest.fixture
def mock_blob(monkeypatch):
    monkeypatch.setattr(
        "function_app.get_blob_service_client", lambda: DummyBlobServiceClient()
    )


def test_extract_csv_data_from_blob_scan(mock_blob):
    result = extract_csv_data_from_blob(
        filename="fake.csv",
        relevant_columns=["col1", "col2"],
        column_mapping={"col1": "column1", "col2": "column2"},
        schema_overrides=None,
    )
    # Collect the lazy DataFrame to get a real DataFrame
    df = result.collect()
    expected = pl.DataFrame({"column1": ["val1", "val3"], "column2": ["val2", "val4"]})
    assert df.equals(expected)
