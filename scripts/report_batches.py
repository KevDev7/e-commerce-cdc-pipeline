"""Inspect local batch audit records without contacting AWS."""
import argparse
import json
from olist_cdc.audit import connect_audit

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', help='Show every task attempt for this run')
    args = parser.parse_args()
    connection = connect_audit()
    try:
        if args.run_id:
            rows = connection.execute('SELECT * FROM task_attempts WHERE run_id=? ORDER BY started_at,step,attempt',(args.run_id,))
        else:
            rows = connection.execute('SELECT * FROM batch_runs ORDER BY started_at DESC LIMIT 20')
        print(json.dumps([dict(row) for row in rows],indent=2))
        last = connection.execute("SELECT max(last_finished_at) FROM batch_runs WHERE status='success'").fetchone()[0]
        print('Last successful completed batch:',last or 'none recorded')
    finally:
        connection.close()
