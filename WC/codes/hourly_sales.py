"""
Sliding window example to print hourly sale counts
"""

from __future__ import absolute_import

import sys
import argparse
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import logging
import re
import datetime
import time
from dateutil.parser import parse


def extract_timestamp(record):
    """
    This function finds search the date pattern and convert the identified date
    pattern into timestamp since the Epoch before retruning to calling program

    :param record: accept a elements of the p-collection
    :return: timestamp since the Epoch
    """
    mo = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6})', record)
    # logging.info('extract_timestamp: {}'.format(mo.group(0)))
    try:
        dt = parse(mo.group(0))

        # Convert a time tuple in local time to seconds since the Epoch
        logging.info('extract_timestamp :  local time to seconds since the Epoch {}'.format(
            int(time.mktime(dt.timetuple()))))
        return int(time.mktime(dt.timetuple()))
    except Exception as e:
        logging.error('extract_timestamp : Error while Processing date {}'.format(e))
        pass

    # In case of error / missing data return current time
    return time.time()


class AddTimestampDoFn(beam.DoFn):
    """
    The purpose of the this class to add the timestamp for each element of p-collection
    by implementing the DoFn ebstract method
    """
    def process(self, element):
        ts = extract_timestamp(element)
        # logging.info(beam.transforms.window.TimestampedValue(element, ts))
        yield beam.transforms.window.TimestampedValue(element, ts)


class PrintWindowFn(beam.DoFn):
    """
       The purpose of the this class to format the p-collection in desired format
       by implementing the DoFn ebstract method
       """
    def process(self, element, window = beam.DoFn.WindowParam):
        start_time = datetime.datetime.fromtimestamp(window.start)
        end_time = datetime.datetime.fromtimestamp(window.end)
        logging.info('{} <==> {} =====> {}'.format(start_time.isoformat(), end_time.isoformat(), element))
        # return ('{},{},{}'.format(start_time.isoformat(), end_time.isoformat(), element))
        return start_time.isoformat()  + '  ' +  end_time.isoformat(),str(element)


def run(argv=sys.argv):
    # setup the command Line argument related to data processing
    parser = argparse.ArgumentParser()

    # Input file that contains sales data
    parser.add_argument('--input',
                        dest='input',
                        default='../data/spikey_sales_weekly.txt',
                        # required=True,
                        help='Input sales data source')

    # This is output file to that will contains discounted top selling products
    parser.add_argument('--output',
                        dest='output',
                        default='../output/output_hourly_sales',
                        # required=True,
                        help='Provide output file detail')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Apache beam pipeline option
    pipeline_option = PipelineOptions(pipeline_args)
    pipeline_option.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_option) as p:
        # Read sales data
        logging.info("Input sales data file %s", known_args.input)
        sales = p | 'Read Sales Data' >> ReadFromText(known_args.input)

        windowed_counts = (
            sales
            # Add timestamp to each element of p-collection
            | 'Timestamp' >> beam.ParDo(AddTimestampDoFn())
            # A window transform assigning windows to each element of a PCollection.
            # sliding window of 1 hours ( 3600 seconds with sliding period 30 minutes( 1800 seconds)
            | 'Window' >> beam.WindowInto(beam.transforms.window.SlidingWindows(3600,1800))
            # Count no of records per window
            | 'WindowCount' >> beam.transforms.CombineGlobally(beam.transforms.combiners.CountCombineFn()).without_defaults()
        )
        windowed_counts = (
            windowed_counts
            # Apply ParDo transformation to print the formatted result
            | beam.transforms.ParDo(PrintWindowFn())
            # output result to a file.
            | WriteToText(known_args.output)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    logging.info("** ===== Logging Starts here =======**")
    run()