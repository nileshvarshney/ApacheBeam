""" An example that verifies the counts and includes best practices."""
from __future__ import  absolute_import

import argparse
import logging
from past.builtins import unicode
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.metrics import Metrics
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class WordCounts(beam.PTransform):
    """A transform to count the occurrences of each word.
    This will convert p-collection word line into (word, count) tupple"""
    def expand(self, p_coll):
        def count_ones(word_one):
            (word, ones) = word_one
            return (word,sum(ones))
        return (p_coll
                | 'split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                                .with_output_types(unicode)
                |'pair_with_one' >> beam.Map(lambda x: (x, 1))
                |'groups' >> beam.GroupByKey()
                |'count' >> beam.Map(count_ones)
                )


class FilterTextFn(beam.DoFn):
    """A DoFn function that filter specific key based on regular expression"""
    def __init__(self, pattern):
        super(FilterTextFn,self).__init__()
        self.pattern = pattern
        self.matched_counter = Metrics.counter(self.__class__, 'matched_counter')
        self.unmatched_counter = Metrics.counter(self.__class__,'unmatched_counter')

    def process(self, element):
        word,_ = element
        if re.match(self.pattern, word):
            logging.info('Matched %s',word)
            self.matched_counter.inc()
            yield element
        else:
            logging.debug('Unmatched %s', word)
            self.unmatched_counter.inc()

def run(argv=None):
    # Input argument processing
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='data.txt',
                        help='Input file to process')
    parser.add_argument('--output',
                        dest='output',
                        default='output_debug',
                        help='output file')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    pipeline_option = PipelineOptions(pipeline_args)
    pipeline_option.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_option) as p:
        # Read the input text file
        filtered_words = (
                p | ReadFromText(known_args.input)
                | WordCounts()
                | 'filterText' >> beam.ParDo(FilterTextFn('Company|Delhi')))

        assert_that(filtered_words, equal_to([('Company', 1), ('Delhi', 1)]))

    def format_result(word_count):
        (word, count) = word_count
        return '%s: %d' %(word, count)

    output = (filtered_words
              | 'format' >> beam.Map(format_result)
              | 'write' >> WriteToText(known_args.output)
              )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()