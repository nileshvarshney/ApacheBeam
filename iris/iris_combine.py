import sys
import argparse

import  apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def map_key_values(element):
    # print(element)
    tokens = element.split(",")
    yield (str(tokens[4]), float(tokens[1]))

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
        mapped_key_values = (
                iris
                | 'Key Value Map' >> beam.FlatMap(lambda element: map_key_values(element))
        )

        # multi columns combine per key does not work as it require input int, float.
        # in multi columns make input is list which is unexpected  input to combine function
        mean_size = (
                mapped_key_values
                | 'Combine' >> beam.CombinePerKey(
            beam.combiners.MeanCombineFn())
        )

        mean_size | beam.io.WriteToText('./iris_mean_size_combine',
                                     append_trailing_newlines = True,
                                     file_name_suffix='.csv',
                                     shard_name_template='',
                                     )



if __name__ == "__main__":
    run()