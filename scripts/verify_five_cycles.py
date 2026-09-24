"""Measure five real scheduler cycles; only generate source writes and read results."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from olist_cdc.cloud_load import aws_session, warehouse_connection
from olist_cdc.db import connect
from olist_cdc.events import parse_csv
from olist_cdc.simulate import PHASES, ids, run_phase
from scripts.reconcile_cloud import main as reconcile
from scripts.verify_cloud_scenario import verify

ROOT=Path(__file__).resolve().parents[1]
COMPOSE=['docker','compose','-f','compose.airflow.yaml','exec','-T','-e','PYTHONWARNINGS=ignore','airflow']
PHASE_GROUPS=[('open',),('approve',),('ship',),('deliver','correct'),
              ('repeat-order','create-delete-test','delete-test','rollback-test')]
EXPECTED_EVENTS=[19,18,16,17,20]
STATUSES=['created','approved','shipped','delivered','delivered']


def now(): return datetime.now(timezone.utc).isoformat()


def runs():
    code="""import json
from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as s:
 rs=s.query(DagRun).filter(DagRun.dag_id=='olist_cdc').order_by(DagRun.execution_date).all()
 print('RESULT='+json.dumps([dict(run_id=r.run_id,state=r.state,logical_date=str(r.execution_date),start=str(r.start_date),end=str(r.end_date),tasks=[dict(task=t.task_id,state=t.state,tries=t.try_number,duration=t.duration) for t in r.get_task_instances(session=s)]) for r in rs]))
"""
    out=subprocess.check_output(COMPOSE+['python','-c',code],cwd=ROOT,text=True,stderr=subprocess.DEVNULL)
    return json.loads(next(x[7:] for x in out.splitlines() if x.startswith('RESULT=')))


def pause(value):
    subprocess.run(COMPOSE+['airflow','dags','pause' if value else 'unpause','olist_cdc'],
                   cwd=ROOT,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)


def raw_state():
    with warehouse_connection() as c:
        q=c.cursor();result={}
        for t in ('customers','orders','order_items','order_payments'):
            q.execute(f'SELECT count(*),count(DISTINCT _event_id),coalesce(max(_source_order),0) FROM "raw".{t}')
            n,unique,seq=q.fetchone();assert n==unique
            result[t]={'rows':n,'max_order':str(seq)}
        c.commit();return result


def main(prefix):
    load_dotenv(ROOT/'.env.cloud',override=True)
    path=ROOT/'data/five-cycles.json'
    if path.exists():raise RuntimeError('Existing measurement; inspect before another run')
    previous_runs=runs()
    if any(r['state'] in ('running','queued') for r in previous_runs):
        raise RuntimeError('Finish active Airflow work and pause before measurement')
    reconcile()
    baseline=raw_state();baseline_count=sum(x['rows'] for x in baseline.values())
    baseline_max=max(int(x['max_order']) for x in baseline.values())
    session=aws_session();s3=session.client('s3');bucket=os.environ['S3_BUCKET']
    def inventory():
        return {x['Key']:x['Size'] for p in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix='raw/') for x in p.get('Contents',[])}
    original=inventory();evidence={'started_at':now(),'prefix':prefix,'baseline':baseline,
        'baseline_reconciliation':json.loads((ROOT/'data/cloud-reconciliation.json').read_text()),
        'original_raw_files':original,'previous_run_ids':[r['run_id'] for r in previous_runs],
        'cycles':[],'manual_warehouse_loads':0,'full_refreshes':0}
    def save():path.write_text(json.dumps(evidence,indent=2)+'\n')
    save();progress=prefix+'-progress';observed={r['run_id'] for r in previous_runs};cumulative=0;deadline=time.monotonic()+2400
    try:
        for index,phases in enumerate(PHASE_GROUPS,1):
            scenario=f'{prefix}-c{index}';started=now()
            with connect() as source:
                for phase in PHASES:run_phase(source,scenario,phase)
                for phase in phases:run_phase(source,progress,phase)
            cumulative+=EXPECTED_EVENTS[index-1]
            print(f'Cycle {index}: committed {EXPECTED_EVENTS[index-1]} changes at {started}',flush=True)
            # The first cycle is enabled only after the restored task has emitted
            # correctly ordered CDC records. Subsequent cycles stay on the schedule.
            if index==1:
                while True:
                    current=inventory();new=[k for k in current if k not in original]
                    events=[e for k in new for e in parse_csv(s3.get_object(Bucket=bucket,Key=k)['Body'].read().decode(),f's3://{bucket}/{k}')]
                    assert all(int(e.values[-4])>baseline_max for e in events),'New capture sequence overlaps the retained baseline'
                    if len(events)==EXPECTED_EVENTS[0]:
                        evidence['cutover_new_min_order']=str(min(e.values[-4] for e in events));save();break
                    assert len(events)<EXPECTED_EVENTS[0]
                    if time.monotonic()>deadline:raise TimeoutError('CDC delivery deadline')
                    time.sleep(15)
                # Start near a boundary, leaving a full interval for verification,
                # the next source workload, and DMS file delivery. A mid-interval
                # start can legitimately produce an idle scheduled cycle.
                boundary=(int(time.time())//300+1)*300+2
                print(f'First batch captured; waiting until {datetime.fromtimestamp(boundary,timezone.utc).isoformat()} to enable scheduling',flush=True)
                while time.time()<boundary:time.sleep(min(15,boundary-time.time()))
                pause(False)
            while True:
                current_runs=runs();new_runs=[r for r in current_runs if r['run_id'] not in observed]
                if new_runs:
                    run=new_runs[0]
                    if run['state']=='failed':raise AssertionError(f'Scheduled run failed: {run}')
                    if run['state']=='success':
                        assert all(t['state']=='success' for t in run['tasks']),run
                        assert run['run_id'].startswith('scheduled__'),run
                        observed.add(run['run_id']);break
                if time.monotonic()>deadline:raise TimeoutError('Five-cycle deadline')
                print(f'Cycle {index}: waiting for scheduled completion',flush=True);time.sleep(30)
            if index==5:pause(True)
            state=raw_state();assert sum(x['rows'] for x in state.values())==baseline_count+cumulative
            with warehouse_connection() as c:
                q=c.cursor();scenario_result=verify(q,scenario)
                q.execute('SELECT status FROM marts.fct_orders WHERE order_id=%s',(ids(progress)['order'],))
                assert q.fetchone()[0]==STATUSES[index-1]
                if index==5:verify(q,progress)
                c.commit()
            reconcile()
            results=json.loads((ROOT/'dbt/target/run_results.json').read_text())
            assert all(r['status'] in ('success','pass') for r in results['results'])
            evidence['cycles'].append({'cycle':index,'generated_at':started,'events':EXPECTED_EVENTS[index-1],
                'airflow':run,'raw':state,'progress_order_status':STATUSES[index-1],
                'scenario':scenario_result,'reconciliation':json.loads((ROOT/'data/cloud-reconciliation.json').read_text()),
                'dbt':dict(Counter(r['status'] for r in results['results']))})
            save();print(f'Cycle {index}: VERIFIED, all source fields match',flush=True)
        dates=[datetime.fromisoformat(x['airflow']['logical_date']) for x in evidence['cycles']]
        assert all((b-a).total_seconds()==300 for a,b in zip(dates,dates[1:])),dates
        final=inventory();assert all(final[k]==size for k,size in original.items())
        assert all(k.startswith('raw/cdc/') for k in final if k not in original)
        evidence.update(completed_at=now(),five_consecutive_intervals=True,new_snapshot_files=0,total_new_events=cumulative)
        save();print('SUCCESS: five consecutive scheduled cycles verified',flush=True)
    except Exception as e:
        evidence.update(failed_at=now(),error=f'{type(e).__name__}: {e}');save();raise
    finally:
        pause(True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--prefix',required=True);a=p.parse_args();main(a.prefix)
