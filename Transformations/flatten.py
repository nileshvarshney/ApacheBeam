import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

header = 'Emp ID,Name Prefix,First Name,Middle Initial,Last Name,Gender,E Mail,Father Name,Mother Name,' \
         'Mother Maiden Name,Date of Birth,Time of Birth,Age in Yrs.,Weight in Kgs.,Date of Joining,' \
         'Quarter of Joining,Half of Joining,Year of Joining,Month of Joining,Month Name of Joining,' \
         'Short Month,Day of Joining,DOW of Joining,Short DOW,Age in Company (Years),Salary,Last % Hike,' \
         'SSN,Phone No., Place Name,County,City,State,Zip,Region,User Name,Password'


class FilterGenderShortMonth(beam.DoFn):
    def process(self, element, *args, **kwargs):
        short_month = kwargs['short_month']
        gender = kwargs['gender']
        tokens = element.split(",")
        if (tokens[5] == gender) & (tokens[20] == short_month):
            return [element]

class ShortInfo(beam.DoFn):
    """
    Thia ParDo class is created to select the  employee name,
    employee id and email ids from merged dataset
    """
    def process(self, element, *args, **kwargs):
        tokens = element.split(",")
        return ['{},{},{},{},{},{}'.format(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4],tokens[6])]


def run(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input1', default='./records_50.csv', help='Provide first dataset location')
    parser.add_argument('--input2', default='./records_50_2.csv', help='Provide first dataset location')
    parser.add_argument('--output', default='./flattened_records', help='Provide first dataset location')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        dataset_01 = p | 'Read First Dataset' >> beam.io.ReadFromText(known_args.input1, skip_header_lines=1)
        dataset_02 = p | 'Read Second Dataset' >> beam.io.ReadFromText(known_args.input2, skip_header_lines=1)

        # flatten both the data sets
        merged = (dataset_01, dataset_02) \
                 | 'Flatten data sets' >> beam.Flatten()

        merged | beam.io.WriteToText(known_args.output,
                                     file_name_suffix='.csv',
                                     append_trailing_newlines=True,
                                     num_shards=0,
                                     shard_name_template='',
                                     header=header)

        # Find Male Joined in Jan

        male_month_join = merged | 'Filter Jan Join Male' >> beam.ParDo(FilterGenderShortMonth(),
                                                                        short_month='Jan', gender="M")
        male_month_join | 'Male Joined in Jan' >> beam.io.WriteToText(known_args.output,
                                                                      file_name_suffix='Jan_M.csv',
                                                                      append_trailing_newlines=True,
                                                                      num_shards=0,
                                                                      shard_name_template='',
                                                                      header=header)

        short_info = merged | 'Short Info' >> beam.ParDo(ShortInfo())

        short_info | 'Employee Short Info' >> beam.io.WriteToText(known_args.output,
                                                                  file_name_suffix='_emp_short_info.csv',
                                                                  append_trailing_newlines=True,
                                                                  num_shards=0,
                                                                  shard_name_template='',
                                                                  header='Emp ID,Name Prefix,First Name,Middle Initial,'
                                                                         'Last Name,E Mail')

        male_employee = merged | 'Male Employee' >> beam.Filter(lambda x: x.split(',')[5] == "M")

        male_employee | 'Male Employee Write' >> beam.io.WriteToText(known_args.output,
                                                                     file_name_suffix='_Male.csv',
                                                                     append_trailing_newlines=True,
                                                                     num_shards=0,
                                                                     shard_name_template='',
                                                                     header=header)


if __name__ == "__main__":
    run()