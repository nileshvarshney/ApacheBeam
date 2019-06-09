from __future__ import absolute_import
import logging

import  apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


def JoinInfo(name_info):
    logging.info(name_info)
    name, info = name_info
    logging.info('{} {} {}'.format(name,  sorted(info['phones']), sorted(info['emails'])))
    return '%s; %s; %s' %\
      (name, sorted(info['emails']), sorted(info['phones']))


def run(argv=None):
    emails_list = [
        ('amy', 'amy@example.com'),
        ('carl', 'carl@example.com'),
        ('julia', 'julia@example.com'),
        ('carl', 'carl@email.com'),
    ]
    phones_list = [
        ('amy', '111-222-3333'),
        ('james', '222-333-4444'),
        ('amy', '333-444-5555'),
        ('carl', '444-555-6666'),
    ]

    pipeline_option = PipelineOptions()
    pipeline_option.view_as(StandardOptions).runner = 'DirectRunner'

    with beam.Pipeline(options=pipeline_option) as p:
        email_data = p | 'Read Email' >> beam.Create(emails_list)
        phone_data = p | 'Read Phone' >> beam.Create(phones_list)

        results = ({'emails' : email_data, 'phones': phone_data}) | beam.CoGroupByKey() | beam.Map(JoinInfo)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    run()
