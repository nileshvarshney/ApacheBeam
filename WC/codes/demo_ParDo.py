from __future__ import absolute_import

import argparse
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText, WriteToText


class RemoveTimeDoFn(beam.DoFn):
    def process(self, element):
        id, timestamp, product, units, price = element.split("\t")
        yield (
            str(timestamp.split(" ")[0]),
            str(product),
            int(units),
            float(price),
            int(units) * float(price))


def format_text(element):
    return '{},{},{},{},{}'.format(element[0],element[1],element[2],element[3], element[4])


def run(argv = sys.argv):
    parse = argparse.ArgumentParser()
    parse.add_argument(
        '--input',
        dest='input',
        default='../data/spikey_sales_weekly.txt',
        help='Location of Input file'
    )
    parse.add_argument(
        '--output',
        dest='output',
        default='../output/output_demoParDo',
        help='Output File Location'
    )
    known_args, pipeline_args = parse.parse_known_args(argv)

    # Apache PipeLine
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        sales = p | 'Read Weekly Sale Data' >> beam.io.ReadFromText(known_args.input)

        clean = sales | 'Remove Time' >> beam.ParDo(RemoveTimeDoFn())

        clean | 'format' >> beam.Map(format_text) | 'write' >> WriteToText(known_args.output)


if __name__ == "__main__":
    run()
