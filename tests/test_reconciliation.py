from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from scripts.reconcile_cloud import compare_rows


def test_ordered_iterators_compare_all_fields_without_hashes():
    source = iter([('c1', None, Decimal('12.30'), datetime(2026, 1, 1, tzinfo=timezone.utc))])
    target = iter([['c1', None, Decimal('12.30'), datetime(2026, 1, 1, 3, tzinfo=timezone(timedelta(hours=3))) ]])
    assert compare_rows('customers', source, target) == {'count': 1, 'all_fields_match': True}
    assert compare_rows('customers', iter([]), iter([])) == {'count': 0, 'all_fields_match': True}


@pytest.mark.parametrize('source,target,message', [
    ([('c1', 'old')], [('c1', 'new')], 'first mismatch'),
    ([('c1', None)], [('c1', '')], 'first mismatch'),
    ([('c1',)], [], 'warehouse ended'),
    ([], [('c1',)], 'source ended'),
])
def test_reconciliation_rejects_changed_fields_and_missing_rows(source, target, message):
    with pytest.raises(AssertionError, match=message):
        compare_rows('customers', iter(source), iter(target))
