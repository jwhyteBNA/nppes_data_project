import pytest
import polars
from function_app import load_api_data


def test_load_api_data_missing_population(mocker):
    mock_data = [
    ["NAME", "B01001_001E", "state", "county"],
    ["Test County, USA", "12345", "47", "001"],
    ["Another County, USA", "67890", "47", "003"],
    ["Another County, USA", "", "47", "003"]
    ]

    mock_cursor = mocker.MagicMock()
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("function_app.get_psycopg2_connection", return_value=mock_conn)

    load_api_data(mock_data)

    assert mock_cursor.copy_from.called
    args, kwargs = mock_cursor.copy_from.call_args
    written_data = args[0].getvalue()  
    lines = [line for line in written_data.strip().split("\n") if line]
    assert len(lines) == 2
    for line in lines:
        assert "\t\t" not in line 
    assert "Test County, USA\t12345\t47\t001" in lines[0]
    assert "Another County, USA\t67890\t47\t003" in lines[1]