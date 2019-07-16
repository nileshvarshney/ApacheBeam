import sys
import os
import argparse

import  apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def map_key_values(element):
    # print(element)
    tokens = element.split(",")
    yield (str(tokens[4]), [float(tokens[0]), float(tokens[1]),float(tokens[2]),float(tokens[3])])


def counts_values(key, values):
    sepal_length = sepal_width = petal_length  = petal_width = 0
    for l in range(len(values)):
        sepal_length += values[l][0]
        sepal_width += values[l][1]
        petal_length += values[l][2]
        petal_width += values[l][3]
    return [key, round(sepal_length/len(values), 2),
            round(sepal_width / len(values), 2),
            round(petal_length / len(values), 2),
            round(petal_width / len(values), 2),
            len(values)]


class PrintFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        tokens = element
        return ['{},{},{},{},{},{}'.format(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5])]


def run(argv = sys.argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        help='Provide Input File',
                        default='./iris.csv'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        iris = p | 'Read File ' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)

        mapped_key_values = iris | 'Key Value Map' >> beam.FlatMap(lambda element: map_key_values(element))
        grouped = mapped_key_values \
                  | 'Groupping' >> beam.GroupByKey() \
                  | 'Counted' >> beam.Map(lambda (key, values): counts_values(key, values))

        output = grouped | 'Formatted Output' >> beam.ParDo(PrintFn())
        output | beam.io.WriteToText('./iris_mean_size',
                                     append_trailing_newlines = True,
                                     file_name_suffix='.csv',
                                     shard_name_template='',
                                     header='class,sepal_length,sepal_width,petal_length,petal_width,counts')



if __name__ == "__main__":
    run()