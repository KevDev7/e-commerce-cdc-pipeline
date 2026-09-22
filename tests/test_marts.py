"""Warehouse behavior uses explicit DMS-format fixtures, not fabricated WAL."""
import hashlib
import os
import shutil
import subprocess

import pytest

from olist_cdc.db import ROOT
from olist_cdc.events import NULL, parse_csv, source_columns
from olist_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text


@pytest.mark.integration
@pytest.mark.parametrize('overlap_snapshot',[False,True])
def test_marts_handle_late_files_deletes_history_and_multiple_payments(database,tmp_path,overlap_snapshot):
    initialize_raw(database)
    time0='2026-01-01T00:00:00Z';time1='2026-01-02T00:00:00Z'
    def row(table,data,sequence,op='I',timestamp=time0):
        values=[str(data.get(c,time0 if c=='updated_at' else NULL)) for c in source_columns(table)]
        return [op,table,'ecommerce',*values,'0/ABC',str(sequence),timestamp]
    customer=dict(customer_id='c1',customer_unique_id='person1',city='sao paulo',state='SP',postal_code='00123')
    order=dict(order_id='o1',customer_id='c1',status='created',purchased_at='2017-01-01 12:00:00')
    item=dict(order_item_key='o1:1',order_id='o1',order_item_id=1,product_id='prod',seller_id='seller',shipping_limit_at='2017-01-02',price=50,freight_value=5)
    payment=dict(payment_key='o1:1',order_id='o1',payment_sequential=1,payment_type='credit_card',payment_installments=1,payment_value=60)
    earlier=[row('customers',customer,1),row('customers',{**customer,'customer_id':'c2'},2),
             row('orders',order,3),row('order_items',item,4),row('order_items',{**item,'order_item_key':'o1:2','order_item_id':2},5),
             row('order_payments',payment,6),row('order_payments',{**payment,'payment_key':'o1:2','payment_sequential':2,'payment_value':50},7),
             row('orders',{**order,'status':'approved'},8,'U'),
             row('orders',{**order,'order_id':'o2','status':'canceled'},12)]
    later=[row('orders',{**order,'status':'delivered'},9,'U',time1),
           row('customers',{**customer,'city':'campinas'},10,'U',time1),
           row('customers',{**customer,'customer_id':'c2'},11,'D',time1)]
    later += [row('orders',{**order,'status':'delivered','approved_at':'2017-01-01'},13,'U',time1),
              row('orders',{**order,'order_id':'o3'},14,'I',time0),
              row('orders',{**order,'order_id':'o3'},15,'D',time1),
              row('orders',{**order,'order_id':'o3'},16,'I',time1),
              row('orders',{**order,'order_id':'o5'},17,'I','2026-03-08T01:30:00-05:00'),
              row('orders',{**order,'order_id':'o5','status':'delivered'},18,'U','2026-03-08T03:30:00-04:00')]
    for name,rows in [('later.csv',later),('earlier.csv',earlier),('replayed.csv',earlier)]:
        text=csv_text(rows);load_local(database,parse_csv(text,name),name,hashlib.sha256(text.encode()).hexdigest())
    profiles=tmp_path/'profiles';profiles.mkdir();shutil.copyfile(ROOT/'dbt/profiles.yml.example',profiles/'profiles.yml')
    def build():
        database.commit()
        result=subprocess.run([str(ROOT/'.venv/bin/dbt'),'build','--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(profiles),'--target','local'],capture_output=True,text=True,
            env={**os.environ,'WAREHOUSE_DATABASE':database.info.dbname,'DBT_SEND_ANONYMOUS_USAGE_STATS':'false',
                 'DBT_TARGET_PATH':str(tmp_path/'target'),'DBT_LOG_PATH':str(tmp_path/'logs')},timeout=120)
        assert result.returncode==0,result.stdout[-8000:]+result.stderr[-2000:]
    build()
    snapshots=[[r[0],*r[3:]] for r in earlier[:2]]
    if overlap_snapshot:snapshots[0][4]='campinas'
    for snapshot in snapshots:snapshot[-1]='2026-01-03T00:00:00Z' if overlap_snapshot else '2025-12-31T00:00:00Z'
    text=csv_text(snapshots)
    load_local(database,parse_csv(text,'customers/LOAD.csv',snapshot_table='customers'),'customers/LOAD.csv',hashlib.sha256(text.encode()).hexdigest())
    # Older stable or later overlapping order snapshots must not invent transitions.
    order_snapshot = row('orders',{**order,'status':'delivered' if overlap_snapshot else 'created'},0)
    order_snapshot = [order_snapshot[0],*order_snapshot[3:]]
    order_snapshot[-1]='2026-01-03T00:00:00Z' if overlap_snapshot else '2025-12-31T00:00:00Z'
    baseline = row('orders',{**order,'order_id':'o4','status':'delivered'},0)
    baseline = [baseline[0],*baseline[3:]]
    text=csv_text([order_snapshot,baseline])
    load_local(database,parse_csv(text,'orders/LOAD.csv',snapshot_table='orders'),'orders/LOAD.csv',hashlib.sha256(text.encode()).hexdigest())
    build()
    assert database.execute("SELECT status,item_total,freight_total,order_total,payment_total,item_count,payment_count FROM analytics_marts.fct_orders WHERE order_id='o1'").fetchone()==('delivered',100,10,110,110,2,2)
    assert database.execute("SELECT item_count,payment_count,has_items,has_payments FROM analytics_marts.fct_orders WHERE order_id='o2'").fetchone()==(0,0,False,False)
    assert database.execute('SELECT customer_id,city FROM analytics_marts.dim_customers').fetchall()==[('c1','campinas')]
    assert database.execute("SELECT city,is_current FROM analytics_marts.dim_customer_history WHERE customer_id='c1' ORDER BY source_order_from").fetchall()==[('sao paulo',False),('campinas',True)]
    assert database.execute("SELECT count(*) FROM analytics_marts.dim_customer_history WHERE customer_id='c2' AND is_current").fetchone()[0]==0
    assert database.execute('SELECT count(*) FROM raw.order_payments').fetchone()[0]==2
    assert database.execute('SELECT count(*) FROM raw.customers WHERE _is_snapshot').fetchone()[0]==2
    history=database.execute("SELECT status,is_current,observed_duration_seconds FROM analytics_marts.fct_order_status_history WHERE order_id='o1' ORDER BY source_order_from").fetchall()
    assert [r[:2] for r in history]==[('created',False),('approved',False),('delivered',True)]
    assert history[1][2]==86400 and history[2][2] is None
    assert database.execute("SELECT is_deleted,is_current FROM analytics_marts.fct_order_status_history WHERE order_id='o3' ORDER BY source_order_from").fetchall()==[(False,False),(True,False),(False,True)]
    assert database.execute("SELECT status,is_initial_snapshot,observed_duration_seconds FROM analytics_marts.fct_order_status_history WHERE order_id='o4'").fetchall()==[('delivered',True,None)]

    assert database.execute("SELECT observed_duration_seconds FROM analytics_marts.fct_order_status_history WHERE order_id='o5' ORDER BY source_order_from").fetchall()==[(3600,),(None,)]
