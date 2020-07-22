# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import os

from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import mssql_to_gcs
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import email_operator

# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# We define some variables that we will use in the DAG tasks.
SUCCESS_TAG = 'success'
FAILURE_TAG = 'failure'

DATE = '{{ ds }}'

DEFAULT_DAG_ARGS = {
    'start_date': YESTERDAY,
    'retries': 0,
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(dag_id='mssql_gcs_dataflow_bigquery_dag_1',
                description='A DAG triggered by an external Cloud Function',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:
    # Export task that will process SQL statement and save files to Cloud Storage.
    export_sales_orders = mssql_to_gcs.MsSqlToGoogleCloudStorageOperator(
        task_id='export_sales_orders',
        sql='SELECT * FROM WideWorldImporters.Sales.Orders;',
        bucket=models.Variable.get('mssql_export_bucket'),
        filename=DATE + '-export.json',
        mssql_conn_id='mssql_default',
        dag=dag
    )

    # Here we create two conditional tasks, one of which will be executed
    # based on whether the export_sales_orders was a success or a failure.
    success_move_task = email_operator.EmailOperator(task_id='success',
                                                    	trigger_rule=TriggerRule.ALL_SUCCESS,
														to=models.Variable.get('email'),
														subject='mssql_gcs_dataflow_bigquery_dag_1 Job Succeeded: start_date {{ ds }}',
														html_content="HTML CONTENT"
														)

    failure_move_task = email_operator.EmailOperator(task_id='failure',
                                                    	trigger_rule=TriggerRule.ALL_FAILED,
														to=models.Variable.get('email'),
														subject='mssql_gcs_dataflow_bigquery_dag_1 Job Failed: start_date {{ ds }}',
														html_content="HTML CONTENT"
														)

    # The success_move_task and failure_move_task are both downstream from the
    # dataflow_task.
    export_sales_orders >> success_move_task
    export_sales_orders >> failure_move_task