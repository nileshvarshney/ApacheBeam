"""
This dataflow program will find all the winary exists in california on the basis of provided data
"""
import apache_beam as beam
import sys


def find_wineries(line, searchText):
    if searchText in line:
        yield line

if __name__ == "__main__":
    p = beam.Pipeline(argv=sys.argv)
    input = '../data/spikey_winery_list.csv'
    output = '../output/calWineries'

    searchText = 'California'

    (p
     | 'ReadData' >> beam.io.ReadFromText(input)
     | 'GrepSearchText' >> beam.FlatMap(lambda line : find_wineries(line, searchText))
     | 'WriteOutput' >> beam.io.WriteToText(output)
     )

    p.run().wait_until_finish()
