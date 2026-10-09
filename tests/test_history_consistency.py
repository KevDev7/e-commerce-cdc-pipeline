"""Exercise the actual dbt consistency query against deliberately mismatched rows."""
from pathlib import Path

from jinja2 import Environment, StrictUndefined
import pytest


CURRENT = ('c1', 'person1', 'sao paulo', 'SP', '00123')
ATTRIBUTES = ['customer_unique_id', 'city', 'state', 'postal_code']


@pytest.fixture
def compare_history(database):
    database.execute('CREATE SCHEMA marts')
    database.execute('''CREATE TABLE marts.dim_customers (
        customer_id text, customer_unique_id text, city text, state text, postal_code text)''')
    database.execute('CREATE TABLE marts.dim_customer_history (LIKE marts.dim_customers)')
    database.execute('ALTER TABLE marts.dim_customer_history ADD COLUMN is_current boolean')
    query = Environment(undefined=StrictUndefined).from_string(
        Path('dbt/tests/customer_history_current_matches_dimension.sql').read_text()
    ).render(ref=lambda name: 'marts.' + name)

    def compare(current, history):
        database.execute('INSERT INTO marts.dim_customers VALUES (%s,%s,%s,%s,%s)', current)
        database.execute('INSERT INTO marts.dim_customer_history VALUES (%s,%s,%s,%s,%s,true)', history)
        return database.execute(query).fetchall()

    return compare


@pytest.mark.integration
@pytest.mark.parametrize('index', range(1, 5), ids=ATTRIBUTES)
def test_history_consistency_detects_attribute_mismatch(compare_history, index):
    history = list(CURRENT)
    history[index] = 'different'
    assert compare_history(CURRENT, history) == [('c1',)]


@pytest.mark.integration
@pytest.mark.parametrize('index', range(1, 5), ids=ATTRIBUTES)
@pytest.mark.parametrize('values', [(None, ''), ('', None)], ids=['null-empty', 'empty-null'])
def test_history_consistency_distinguishes_null_from_empty(compare_history, index, values):
    current, history = list(CURRENT), list(CURRENT)
    current[index], history[index] = values
    assert compare_history(current, history) == [('c1',)]


@pytest.mark.integration
@pytest.mark.parametrize('current', [CURRENT, ('c1', None, None, None, None), ('c1', '', '', '', '')])
def test_history_consistency_accepts_matching_attributes(compare_history, current):
    assert compare_history(current, current) == []
