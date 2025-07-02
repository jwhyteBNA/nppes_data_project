import pytest
import polars as pl
from function_app import load_chunked_blob_data_to_postgres

def test_load_chunked_blob_data_to_postgres_handles_db_error(mocker):
    # Create a small DataFrame
    df = pl.DataFrame({"col1": ["a"], "col2": ["b"]})
    lazy_df = df.lazy()

    # Mock DB connection and cursor
    mock_cursor = mocker.MagicMock()
    # Simulate an error on copy_from
    mock_cursor.copy_from.side_effect = Exception("DB error")
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch("function_app.get_psycopg2_connection", return_value=mock_conn)

    # The function should raise, and connection should be closed
    with pytest.raises(Exception, match="DB error"):
        load_chunked_blob_data_to_postgres(lazy_df, target_table="test_table", chunk_size=100_000)

    assert mock_conn.close.called
    assert mock_conn.rollback.called