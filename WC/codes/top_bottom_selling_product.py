"""
This program will find the top and bottom 20 selling products
"""
from __future__ import  absolute_import
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import  WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import pvalue
import logging

class SplitItemBasedOnSalesFn(beam.DoFn):
    # constant tags
    OUTPUT_TAG_TOP_SELLER = 'tag_top_seller'
    OUTPUT_TAG_POOR_SELLER = 'tag_top_seller'

    def process(self, element):
        tokens = element.split("\t")

        product = tokens[2]
        quantity = int(tokens[len(tokens) -1])

        if quantity >= 1000:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_TOP_SELLER, product)

        if quantity <= 5:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_POOR_SELLER, product)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default = '../data/spikey_sales_weekly.txt',
                        # required=True,
                        help='Input File to Process')

    parser.add_argument('--output',
                        dest='output',
                        default='../output/products',
                        # required=True,
                        help='output processed file')

    known_args, pipeline_Args = parser.parse_known_args(argv)

    pipeline_option = PipelineOptions(argv)
    pipeline_option.view_as(SetupOptions).save_main_session = True
    logging.getLogger().setLevel(logging.INFO)
    with beam.Pipeline(options=pipeline_option) as p:
        # read input file
        items = p | ReadFromText(known_args.input)
        logging.info("Print the items %s", items)

        split_item_result = (
            items
            | 'Split Result' >> beam.ParDo(SplitItemBasedOnSalesFn()).with_outputs(
            SplitItemBasedOnSalesFn.OUTPUT_TAG_TOP_SELLER, SplitItemBasedOnSalesFn.OUTPUT_TAG_POOR_SELLER))

        top_seller = split_item_result[SplitItemBasedOnSalesFn.OUTPUT_TAG_TOP_SELLER]
        poor_seller = split_item_result[SplitItemBasedOnSalesFn.OUTPUT_TAG_POOR_SELLER]

        # (top_seller | WriteToText(known_args.output + '_top_seller'))
        (poor_seller | WriteToText(known_args.output + '_poor_seller'))


if __name__ == "__main__":
    run()