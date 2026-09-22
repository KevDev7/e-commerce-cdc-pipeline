import pytest
from olist_cdc.db import connect
from scripts import benchmark_cloud


@pytest.mark.integration
def test_measured_workload_writes_expected_rows_and_rejects_repeat(database, monkeypatch):
    name = database.info.dbname
    database.commit()
    monkeypatch.setattr(benchmark_cloud, 'connect', lambda: connect(name))
    result = benchmark_cloud.write('local-measured-test', 10)
    assert result['expected_operations'] == {'I':40,'U':20,'D':4}
    keys = benchmark_cloud.identities('local-measured-test',10)
    for table in keys:
        assert database.execute(f'SELECT count(*) FROM ecommerce.{table}').fetchone() == (9,)
    assert database.execute('SELECT DISTINCT city FROM ecommerce.customers').fetchall() == [('campinas',)]
    assert database.execute('SELECT DISTINCT status FROM ecommerce.orders').fetchall() == [('delivered',)]
    assert database.execute('SELECT order_id FROM ecommerce.orders ORDER BY order_id').fetchall() == sorted((k,) for k in keys['orders'][1:])
    database.commit()
    with pytest.raises(ValueError,match='already exists'):
        benchmark_cloud.write('local-measured-test',10)
    assert database.execute('SELECT count(*) FROM ecommerce.orders').fetchone() == (9,)
