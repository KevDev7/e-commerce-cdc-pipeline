"""Exercise completion semantics using S3 metadata fixtures; no AWS calls."""
import pytest

from olist_cdc import microbatch


@pytest.fixture
def capture(monkeypatch):
    objects = [{'Key': 'raw/cdc/001.csv', 'ETag': 'first', 'Size': 100}]

    class S3:
        def client(self, name):
            assert name == 's3'
            return self

        def get_paginator(self, name):
            assert name == 'list_objects_v2'
            return self

        def paginate(self, **kwargs):
            assert kwargs == dict(Bucket='demo', Prefix='raw/')
            return [{'Contents': objects[:1]}, {'Contents': objects[1:]}]

    monkeypatch.setattr(microbatch, 'aws_session', S3)
    monkeypatch.setenv('S3_BUCKET', 'demo')
    monkeypatch.setenv('REDSHIFT_HOST', 'warehouse')
    monkeypatch.setenv('CAPTURE_PREFIX', 'raw')
    return objects


def test_completed_batch_skips_until_capture_changes(tmp_path, capture):
    assert microbatch.prepare_batch(tmp_path, 'first')
    microbatch.complete_batch(tmp_path, 'first')
    microbatch.complete_batch(tmp_path, 'first')  # Airflow may retry after acknowledgement.
    assert not microbatch.prepare_batch(tmp_path, 'quiet')
    capture.append({'Key': 'raw/cdc/002.csv', 'ETag': 'second', 'Size': 80})
    assert microbatch.prepare_batch(tmp_path, 'next')


def test_failed_build_is_retried_even_when_raw_was_already_loaded(tmp_path, capture):
    assert microbatch.prepare_batch(tmp_path, 'first')
    microbatch.complete_batch(tmp_path, 'first')
    capture[0]['ETag'] = 'changed'
    assert microbatch.prepare_batch(tmp_path, 'failed-build')
    # Loading raw does not acknowledge a batch. Only complete after successful dbt build/tests.
    assert microbatch.prepare_batch(tmp_path, 'retry')
    microbatch.complete_batch(tmp_path, 'retry')
    assert not microbatch.prepare_batch(tmp_path, 'quiet')


def test_files_arriving_during_build_are_not_acknowledged_early(tmp_path, capture):
    assert microbatch.prepare_batch(tmp_path, 'first')
    capture.append({'Key': 'raw/cdc/002.csv', 'ETag': 'second', 'Size': 80})
    microbatch.complete_batch(tmp_path, 'first')
    assert microbatch.prepare_batch(tmp_path, 'next')


def test_missing_checkpoint_rebuilds_and_different_warehouse_rebuilds(tmp_path, capture, monkeypatch):
    assert microbatch.prepare_batch(tmp_path, 'first')
    microbatch.complete_batch(tmp_path, 'first')
    monkeypatch.setenv('REDSHIFT_HOST', 'replacement-warehouse')
    assert microbatch.prepare_batch(tmp_path, 'replacement')
    microbatch.complete_batch(tmp_path, 'replacement')
    (tmp_path / 'completed.json').unlink()
    assert microbatch.prepare_batch(tmp_path, 'lost-checkpoint')


def test_empty_capture_fails_instead_of_looking_healthy(tmp_path, capture):
    capture.clear()
    with pytest.raises(RuntimeError, match='No DMS files'):
        microbatch.prepare_batch(tmp_path, 'empty')
    assert not (tmp_path / 'completed.json').exists()


def test_acknowledgement_requires_its_own_prepared_run(tmp_path, capture):
    assert microbatch.prepare_batch(tmp_path, 'first')
    with pytest.raises(FileNotFoundError):
        microbatch.complete_batch(tmp_path, 'different')
    assert not (tmp_path / 'completed.json').exists()
