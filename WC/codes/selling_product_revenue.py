from __future__ import absolute_import
import argparse
import logging
import sys
import unicodedata

import apache_beam as  beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CalculateRevenueFn(beam.DoFn):
    def process(self, element):
        timestamp, product, quantity, rate = element.split(",")
        revenue = float(rate) * float(quantity)
        logging.info(product + 'Revenue : {}'.format(revenue))
        yield (product, revenue)


class RevenueSum ( beam.DoFn):
    def process(self, element):
        product, revenue = element
        return [(product, sum(revenue))]


class FormatJsonFn(beam.DoFn):
    def process(self, element):
        product, revenue = element
        logging.info('FormatJsonFn :' + product + ' => ' +  str(revenue))
        product_name = unicodedata.normalize('NFKD', product).encode('ascii','ignore')

        revenue = float(revenue)
        return [
            {
                'product':product_name,
                'revenue':revenue
            }
        ]


def run(argv=sys.argv):
    # setup the command Line argument related to data processing
    parser = argparse.ArgumentParser()

    # Input file that contains sales data
    parser.add_argument('--input',
                        dest='input',
                        default='../data/spikey_sales_weekly.csv',
                        # required=True,
                        help='Input sales data source')

    # This is output file to that will contains discounted top selling products
    parser.add_argument('--output',
                        dest='output',
                        default='../output/output_product_revenue',
                        # required=True,
                        help='Provide output file detail')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_option = PipelineOptions(pipeline_args)
    pipeline_option.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_option) as p:
        product_revenue = (
            p
            | 'Read' >> ReadFromText(known_args.input)
            | ' Calculate Revenue' >> beam.ParDo(CalculateRevenueFn())
            | 'Group by Product' >> beam.GroupByKey()
            | 'Sum by Product' >> beam.ParDo(RevenueSum())
            | 'format to json' >> beam.ParDo(FormatJsonFn())
            | 'Write To File' >> beam.io.WriteToText(known_args.output + '_product_revenue.json')
            #| 'Write To File' >> beam.io.WriteToBigQuery(known_args.output, schema='Product_Revenue')
        )
        # result = p.run()
        # result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.info("======= Logging starts here =====")
    run()