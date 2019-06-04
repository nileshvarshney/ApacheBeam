from __future__ import absolute_import
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
# For Google Cloud
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.metrics import Metrics
from apache_beam.metrics import MetricsFilter
from apache_beam.io import WriteToText
import argparse
import logging
import  re
from past.builtins import unicode



class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input test into word"""
    def __init__(self):
        self.word_counter = Metrics.counter(self.__class__, 'words')
        self.word_length_counter =  Metrics.counter(self.__class__,'word_lengths')
        self.word_length_dist = Metrics.distribution(self.__class__,'word_len_dist')
        self.empty_length_counter = Metrics.counter(self.__class__, 'empty_lines')

    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.

        Args:
            element : the element being processed
        """
        text_line = element.strip()
        if not text_line:
            self.empty_length_counter.inc(1)

        words = re.findall(r'[\w\']+', text_line, re.UNICODE)
        for w in words:
            self.word_counter.inc()
            self.word_length_counter.inc(len(w))
            self.word_length_dist.update(len(w))
        return words


def run(argv=None):
    """Main entry point for word count"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='../data/data.txt',
                        help='Input file to process')
    parser.add_argument('--output',
                        dest='output',
                        default='../output/output.txt',
                        help='output file')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_option = PipelineOptions(pipeline_args)

    # DirectRunner : Default Runner to run the program locally
    # To Run Locally
    pipeline_option.view_as(StandardOptions).runner = 'DirectRunner'

    # To Run on Google Cloud
    # pipeline_option = pipeline_option.view_as(GoogleCloudOptions)
    # pipeline_option.project = 'my-project-id'
    # pipeline_option.job_name = 'myjob'
    # pipeline_option.staging_location = 'gs://your-bucket-name-here/staging'
    # pipeline_option.temp_location = 'gs://your-bucket-name-here/temp'
    # pipeline_option.view_as(StandardOptions).runner = 'DataflowRunner'

    # pipeline_option.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_option)

    # read text file into p-collection
    lines = p | 'read' >> beam.io.ReadFromText(known_args.input)

    # Count the occurrences of each word.
    def count_one(word_ones):
        (word, ones) = word_ones
        return (word,sum(ones))

    def format_result(word_count):
        (word, count) = word_count
        return '%s: %d' % (word, count)

    counts = (lines |
              'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(unicode)) |
              'pair_with_one' >> beam.Map(lambda x: (x, 1)) |
              'group' >>  beam.GroupByKey() |
              'count' >> beam.Map(count_one))

    output = counts | 'format' >> beam.Map(format_result)
    output | 'write' >> WriteToText(known_args.output)

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
    # print('hello')
