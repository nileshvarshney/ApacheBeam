"""
This program will read sales data and offer data. From sales data
it will identify the top selling product ( sold quantity >= 100) and match with the
offered item's product id and return a list of product ID and Product name in output
file
"""

from __future__ import absolute_import

import sys
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pvalue import AsList
import logging



class TopSellingProducts(beam.DoFn):
    THRESHOLD = 90

    def process(self, element):
        tokens = element.split("\t")
        product_id = tokens[0]
        product_name = tokens[2]
        quantity = int(tokens[len(tokens) - 1])
        if quantity >= self.THRESHOLD:
            # logging.info("TopSellingProducts.process {} <=> {}".format( product_id, product_name))
            yield (product_id, product_name)


class OfferedItem(beam.DoFn):
    def process(self, element):
        tokens = element.split("\t")
        # logging.info("OfferedItem.process {} ".format(tokens[0]))
        yield tokens[0]


def match_id_fn(top_seller, discounted_items):
    if top_seller[0] in discounted_items:
        # logging.info(top_seller[1])
        #yield  top_seller
        yield  top_seller[0].encode('utf-8'), top_seller[1].encode('utf-8')


def run(argv=sys.argv):

    # setup the command Line argument related to data processing
    parser = argparse.ArgumentParser()

    # Input file that contains sales data
    parser.add_argument('--input1',
                        dest='input1',
                        default='../data/spikey_sales_weekly.txt',
                        # required=True,
                        help='Input Sales Data file')

    # Input file that contains all the offers currently running
    parser.add_argument('--input2',
                        dest='input2',
                        default='../data/spikey_offers.txt',
                        # required=True,
                        help='Input Offers Data file')

    # This is output file to that will contains discounted top selling products
    parser.add_argument('--output',
                        dest='output',
                        default='../output/output_top_seller_offer',
                        # required=True,
                        help='Input Offers Data file')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Apache beam pipeline option
    pipeline_option = PipelineOptions(pipeline_args)
    pipeline_option.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_option) as p:
        # Read sales data
        logging.info("Input sales data file %s", known_args.input1)
        sold_items = p | 'Read Sales Data' >> ReadFromText(known_args.input1)

        logging.info("Input offer data file %s", known_args.input2)
        discounted_items = p | 'Read offers' >> ReadFromText(known_args.input2)

        # Get Top Selling Items
        top_selling_items = (
            sold_items
            | 'Top Selling Products' >> beam.ParDo(TopSellingProducts())
        )
        logging.info("top_selling_items calculation completed")

        discounted_item_ids = (
            discounted_items
            | 'Offer Items' >> beam.ParDo(OfferedItem())
        )
        logging.info("discounted_item_ids calculation completed")

        top_discounted_items = (
            top_selling_items
            | 'Discounted Item Match' >> beam.FlatMap(match_id_fn, AsList(discounted_item_ids))
        )

        (top_discounted_items | 'Write output File' >> WriteToText(known_args.output,
                                                                   file_name_suffix='.csv',
                                                                   header='Product_ID, Product_Name'
                                                                   ))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.info("** ===== Logging Starts here =======**")
    run()
