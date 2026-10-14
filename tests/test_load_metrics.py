"""Exercise metric commit boundaries with explicit S3/SQL fixtures, not AWS."""
import io
import pytest
import csv
from olist_cdc.cloud_load import load_file
from test_events import csv_text, customer_row


@pytest.mark.parametrize('fail',[False,True])
def test_file_metrics_only_count_committed_files(monkeypatch,fail):
    monkeypatch.setenv('CAPTURE_PREFIX','raw')
    class Connection:
        loaded=False
        def cursor(self):return self
        def execute(self,query,*args):
            assert not query.startswith(('CREATE TEMP TABLE', 'DROP TABLE', 'INSERT INTO "raw".customers'))
            if query.startswith("COPY "):
                assert query.startswith('COPY "raw"."customers" FROM ')
                assert "FORMAT AS CSV" in query and "NULL AS '__OLIST_NULL__'" in query
                assert "TIMEFORMAT 'auto'" in query
                assert "EMPTYASNULL" not in query and "BLANKSASNULL" not in query
                assert "PARQUET" not in query
        def fetchone(self):return None
        def commit(self):
            if fail:raise RuntimeError('injected commit failure')
        def rollback(self):pass
        def close(self):pass
    class S3:
        def get_object(self,**kwargs):
            return {'Body':io.BytesIO(csv_text([customer_row(),customer_row(sequence='2',operation='U'),customer_row(sequence='3',operation='D')]).encode())}
        def put_object(self,**kwargs):
            assert kwargs["Key"] == "copy-ready/cdc/test/customers.csv"
            from olist_cdc.schema import RAW_METADATA, source_columns
            columns = source_columns("customers") + list(RAW_METADATA)
            rows = [dict(zip(columns, row)) for row in csv.reader(io.StringIO(kwargs["Body"].decode()))]
            assert [r["_op"] for r in rows] == ["I", "U", "D"]
            assert all(r["_source_file"] == "s3://bucket/raw/cdc/test.csv" for r in rows)
    metrics={}
    if fail:
        with pytest.raises(RuntimeError):
            load_file(Connection(),S3(),'bucket','raw/cdc/test.csv','role',metrics=metrics)
        assert metrics=={}
    else:
        assert load_file(Connection(),S3(),'bucket','raw/cdc/test.csv','role',metrics=metrics)==3
        assert metrics==dict(files_committed=1,input_I=1,input_U=1,input_D=1)


def test_previously_loaded_file_is_skipped_without_downloading():
    class Connection:
        def cursor(self): return self
        def execute(self, *args): pass
        def fetchone(self): return (1,)
        def commit(self): pass
        def rollback(self): pass
        def close(self): pass
    class S3:
        def get_object(self, **kwargs):
            raise AssertionError("Previously loaded files must not be downloaded")
    metrics = {}
    assert load_file(Connection(), S3(), 'bucket', 'raw/cdc/test.csv', 'role', metrics=metrics) == 0
    assert metrics == {'files_already_loaded': 1}


def test_failure_probe_tracks_direct_copies_before_ledger_failure():
    from scripts.verify_load_failure import FailBeforeLedger, InjectedFailure
    class Connection:
        def cursor(self): return self
        def execute(self, *args): pass
    probe = FailBeforeLedger(Connection())
    cursor = probe.cursor()
    cursor.execute('COPY "raw"."customers" FROM \'s3://bucket/customers.csv\'')
    cursor.execute('COPY "raw"."orders" FROM \'s3://bucket/orders.csv\'')
    assert probe.copied_tables == ['"raw"."customers"', '"raw"."orders"']
    with pytest.raises(InjectedFailure):
        cursor.execute('INSERT INTO "raw".loaded_files (source_file,row_count) VALUES (%s,%s)', ('file.csv', 2))
    assert probe.triggered
