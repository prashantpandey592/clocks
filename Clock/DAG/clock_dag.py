# -*- coding: utf-8 -*-
"""
Created on Mon May 18 21:14:48 2020

@author: rugved
"""


import datetime as dt
from airflow import DAG, models
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataProcPySparkOperator, DataprocClusterDeleteOperator
import datetime
import os
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
from airflow.utils import trigger_rule


CLUSTER_NAME = 'dataprocpp1805'
PYSPARK_JOB = "gs://prashant592/Clock_Exe/Code/Clock_Exercise.py"


yesterday = dt.datetime.combine(
    dt.datetime.today() - dt.timedelta(1),
    dt.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
#    'retry_delay': dt.timedelta(seconds=30),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'dataproc_spark_submit',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    
     create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name=CLUSTER_NAME,
        num_workers=0,
        zone=models.Variable.get('gce_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')
    
     run_spark_job = DataProcPySparkOperator(
        task_id = 'run_spark_job',
        main = PYSPARK_JOB,
        cluster_name = CLUSTER_NAME,
     )

     delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        project_id = default_dag_args['project_id'],
        task_id = 'delete_dataproc_cluster',
        cluster_name = CLUSTER_NAME,
        trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    )

create_dataproc_cluster >> run_spark_job >> delete_dataproc_cluster