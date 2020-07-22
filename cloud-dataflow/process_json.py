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

import argparse
import logging
import ntpath
import re
import json

import apache_beam as beam
from apache_beam.options import pipeline_options

class RowTransformer(object):

    def __init__(self, filename, load_dt):
        self.filename = filename
        self.load_dt = load_dt

    def parse(self, row):
        data = json.loads(row) 
        data['filename'] = self.filename
        data['load_dt'] = self.load_dt
        return data

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', dest='input', required=True,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output BQ table to write results to.')
    parser.add_argument('--load_dt', dest='load_dt', required=True,
                        help='Load date in YYYY-MM-DD format.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    row_transformer = RowTransformer(filename=ntpath.basename(known_args.input),
                                     load_dt=known_args.load_dt)
    p_opts = pipeline_options.PipelineOptions(pipeline_args)

    with beam.Pipeline(options=p_opts) as pipeline:
        
        rows = pipeline | "Read from text file" >> beam.io.ReadFromText(known_args.input)
        
        dict_records = rows | "Convert to BigQuery row" >> beam.Map(
            lambda r: row_transformer.parse(r))
        
        bigquery_table_schema = {
            "fields": [
                {
                    "mode": "NULLABLE", 
                    "name": "BackorderOrderID", 
                    "type": "INTEGER"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "Comments", 
                    "type": "STRING"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "ContactPersonID", 
                    "type": "INTEGER"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "CustomerID", 
                    "type": "INTEGER"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "CustomerPurchaseOrderNumber", 
                    "type": "INTEGER"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "DeliveryInstructions", 
                    "type": "STRING"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "ExpectedDeliveryDate", 
                    "type": "DATE"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "InternalComments", 
                    "type": "STRING"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "IsUndersupplyBackordered", 
                    "type": "BOOLEAN"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "LastEditedBy", 
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE", 
                    "name": "LastEditedWhen", 
                    "type": "TIMESTAMP"
                },
                {
                    "mode": "NULLABLE", 
                    "name": "OrderDate", 
                    "type": "DATE"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "OrderID", 
                    "type": "INTEGER"
                }, 
                {
                    "mode": "NULLABLE", 
                    "name": "PickedByPersonID", 
                    "type": "INTEGER"
                },
                            {
                    "mode": "NULLABLE", 
                    "name": "PickingCompletedWhen", 
                    "type": "TIMESTAMP"
                },
                {
                    "mode": "NULLABLE", 
                    "name": "SalespersonPersonID", 
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE", 
                    "name": "filename", 
                    "type": "STRING"
                },
                {
                    "mode": "NULLABLE", 
                    "name": "load_dt", 
                    "type": "DATE"
                }
            ]
        }

        dict_records | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                                known_args.output,
                                schema=bigquery_table_schema,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()