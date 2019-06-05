"""
This dataflow program will find all the winary exists in california on the basis of provided data
"""
import apache_beam as beam
import sys


def find_wineries(line, text):
    if text in line:
        yield line


PROJECT='<PROJECT_ID>'
BUCKET ='<BUCKET>'


def run():
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=search_cal_wineries-job',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        'runner =DataflowRunner'
    ]

    p = beam.Pipeline(argv=sys.argv)
    input = 'gs://{0}/data/spikey_winery_list.csv'.format(BUCKET)
    output = 'gs://{0}/output/calWineries'.format(BUCKET)

    search_text = 'California'

    (p
     | 'ReadData' >> beam.io.ReadFromText(input)
     | 'GrepSearchText' >> beam.FlatMap(lambda line : find_wineries(line, search_text))
     | 'WriteOutput' >> beam.io.WriteToText(output)
     )

    p.run()


if __name__ == "__main__":
    run()