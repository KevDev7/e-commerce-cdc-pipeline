import csv
import hashlib
import io
import zipfile

import psycopg
import pytest

from olist_cdc import seed

CUSTOMER = '0' * 31 + '1'
ORDER = '0' * 31 + '2'


@pytest.fixture
def archive(tmp_path, monkeypatch):
    def make(*, orphan=False):
        rows = {
            'customers': [CUSTOMER, CUSTOMER, '00123', 'sao paulo', 'SP'],
            'orders': [ORDER, CUSTOMER, 'created', '2017-01-01 12:00:00', '', '', '', '2017-01-08 00:00:00'],
            'order_items': [ORDER, '1', 'a'*32, 'b'*32, '2017-01-02 12:00:00', '100.25', '5.00'],
            'order_payments': [CUSTOMER if orphan else ORDER, '1', 'credit_card', '1', '105.25'],
        }
        path = tmp_path / 'olist.zip'
        with zipfile.ZipFile(path, 'w') as z:
            for table, mapping in seed.TABLES.items():
                stream = io.StringIO(newline=''); writer = csv.writer(stream)
                writer.writerow([v for v in mapping.values() if v]); writer.writerow(rows[table])
                z.writestr('olist_' + table + '_dataset.csv', stream.getvalue())
        monkeypatch.setattr(seed, 'SHA256', hashlib.sha256(path.read_bytes()).hexdigest())
        return path
    return make


@pytest.mark.integration
def test_seed_preserves_nulls_keys_and_retries_without_resetting_source(database, archive):
    path = archive()
    assert seed.load(database,path)['row_counts'] == dict.fromkeys(seed.TABLES,1)
    database.execute("UPDATE ecommerce.customers SET city='campinas'"); database.commit()
    assert seed.load(database,path)['status'] == 'already_loaded'
    assert database.execute('SELECT city,postal_code FROM ecommerce.customers').fetchone() == ('campinas','00123')
    assert database.execute('SELECT approved_at FROM ecommerce.orders').fetchone()[0] is None
    assert database.execute('SELECT order_item_key FROM ecommerce.order_items').fetchone()[0] == ORDER+':1'
    assert database.execute('SELECT payment_key FROM ecommerce.order_payments').fetchone()[0] == ORDER+':1'


@pytest.mark.integration
def test_invalid_relationship_rolls_back_entire_seed(database, archive):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        seed.load(database,archive(orphan=True))
    assert database.execute('SELECT count(*) FROM ecommerce.customers').fetchone()[0] == 0
    assert database.execute('SELECT count(*) FROM project_meta.seed_runs').fetchone()[0] == 0


def test_changed_upstream_archive_is_rejected(tmp_path):
    path=tmp_path/'unexpected.zip';path.write_bytes(b'unexpected archive')
    with pytest.raises(ValueError,match='checksum changed'):
        seed.download(path)
