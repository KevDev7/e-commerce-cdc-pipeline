"""Exercise metric commit boundaries with explicit S3/SQL fixtures, not AWS."""
import io
import pytest
from olist_cdc.cloud_load import load_file
from test_events import csv_text, customer_row


@pytest.mark.parametrize('fail',[False,True])
def test_file_metrics_only_count_committed_files(monkeypatch,fail):
    monkeypatch.setenv('CAPTURE_PREFIX','olist-v1')
    class Connection:
        loaded=False
        def cursor(self):return self
        def execute(self,query,*args):pass
        def fetchone(self):return None
        def commit(self):
            if fail:raise RuntimeError('injected commit failure')
        def rollback(self):pass
        def close(self):pass
    class S3:
        def get_object(self,**kwargs):
            return {'Body':io.BytesIO(csv_text([customer_row(),customer_row(sequence='2',operation='U'),customer_row(sequence='3',operation='D')]).encode())}
        def put_object(self,**kwargs):pass
    metrics={}
    if fail:
        with pytest.raises(RuntimeError):
            load_file(Connection(),S3(),'bucket','olist-v1/cdc/test.csv','role',metrics=metrics)
        assert metrics=={}
    else:
        assert load_file(Connection(),S3(),'bucket','olist-v1/cdc/test.csv','role',metrics=metrics)==3
        assert metrics==dict(files_committed=1,input_I=1,input_U=1,input_D=1)
