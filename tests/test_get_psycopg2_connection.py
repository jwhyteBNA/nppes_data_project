import psycopg2
from function_app import get_psycopg2_connection


class DummyConnection:
    def close(self):
        pass


def test_get_psycopg2_connection_success(monkeypatch):
    # Mock psycopg2.connect to return a dummy connection object
    monkeypatch.setattr(psycopg2, "connect", lambda *args, **kwargs: DummyConnection())

    conn = get_psycopg2_connection()
    assert isinstance(conn, DummyConnection)
    conn.close()
