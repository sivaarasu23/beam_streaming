from __future__ import absolute_import

import argparse
import logging

import six

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.internal.clients.bigquery.bigquery_v2_messages import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery.bigquery_v2_messages import TableFieldSchema
import json
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/sivaa/beam/subtle-seer-113110-183172fcd165.json'

def dump(line):
  logging.info(line)
  return line

class CollectOrders(beam.DoFn):
  def process(self, element):  
    result = [
        ((element['service_area_name'], element['payment_type'],
         element['status']), element['order_number'])
    ]

    return result

class Split(beam.DoFn):
    def process(self, element):
      return [json.loads(element)]

def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic', required=True,
      help=('Output PubSub topic of the form '
            '"projects/<PROJECT>/topic/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))

  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  if known_args.input_subscription:
    lines = p | beam.io.ReadStringsFromPubSub(
        subscription=known_args.input_subscription)
  else:
    lines = p | beam.io.ReadStringsFromPubSub(topic=known_args.input_topic)

  # Couting number of orders received 
  counts = (lines
            | 'dict_t' >> (beam.ParDo(Split()))
            | 'split' >> (beam.ParDo(CollectOrders()))
            | beam.WindowInto(window.FixedWindows(15, 0))
            | 'group' >> beam.GroupByKey()
            | 'Counting orders' >> beam.CombineValues(beam.combiners.CountCombineFn())
            )
  counts | 'Printcounts' >> beam.Map(lambda x: dump(x))

  SCHEMA = {
    'status': 'STRING',
    'payment_type': 'STRING',
    'order_number':'INTEGER',
    'service_area_name':'STRING'
    }

  # Format the counts into a PCollection of strings.
  def format_result(order_count):
    grouping, count = order_count

    result = {'service_area_name':grouping[0],'payment_type':grouping[1],'status':grouping[2],'order_number':count}

    return result

  output = counts | 'format' >> beam.Map(format_result)

  output | 'Printoutput' >> beam.Map(lambda x: dump(x))


  table_schema = TableSchema()
  for k,v in SCHEMA.iteritems():
          field_schema = TableFieldSchema()
          field_schema.name=k
          field_schema.type=v
          field_schema.mode='nullable'
          table_schema.fields.append(field_schema)

  output | 'writetobq' >> beam.io.WriteToBigQuery(
    project='subtle-seer-113110',
    dataset='test_dataset',
    table='gojek_stream_table',
    schema=table_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  run()