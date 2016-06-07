# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import io
import json
import click
from goodtables import pipeline
from data_quality import tasks, utilities

@click.group()
def cli():
    """The entry point into the CLI."""


@cli.command()
@click.argument('config_filepath')
@click.option('--encoding', default=None)
@click.option('--deploy', is_flag=True)
def run(config_filepath, deploy, encoding):

    """Process data sources for a Spend Publishing Dashboard instance."""

    with io.open(config_filepath, mode='rt', encoding='utf-8') as file:
        config = json.loads(file.read())

    if not os.path.isabs(config['data_dir']):
        config['data_dir'] = os.path.join(os.path.dirname(config_filepath),
                                          config['data_dir'])

    if not os.path.isabs(config['cache_dir']):
        config['cache_dir'] = os.path.join(os.path.dirname(config_filepath),
                                           config['cache_dir'])

    utilities.set_up_cache_dir(config['cache_dir'])
    source_filepath = os.path.join(config['data_dir'], config['source_file'])
    default_batch_options = {
        'goodtables': {
            'arguments': {
                'pipeline': {
                    'processors': ['structure', 'schema'],
                    'options': {
                        'schema': {'case_insensitive_headers': True}
                    }
                },
                'batch': {
                    'data_key': 'data'
                }
            }
        }
    }
    options = utilities.deep_update_dict(default_batch_options, config)
    aggregator = tasks.Aggregator(options)

    if deploy:

        def batch_handler(instance):
            aggregator.write_run()
            assesser = tasks.AssessPerformance(config)
            assesser.run()
            deployer = tasks.Deploy(config)
            deployer.run()

    else:

        def batch_handler(instance):
            aggregator.write_run()
            assesser = tasks.AssessPerformance(config)
            assesser.run()

    post_tasks = {'post_task': batch_handler, 'pipeline_post_task': aggregator.run}
    options['goodtables']['arguments']['batch'].update(post_tasks)
    batch_options = options['goodtables']['arguments']['batch']
    batch_options['pipeline_options'] = options['goodtables']['arguments']['pipeline']
    batch = pipeline.Batch(source_filepath, **batch_options)
    batch.run()


@cli.command()
@click.argument('config_filepath')
def deploy(config_filepath):

    """Deploy data sources for a Spend Publishing Dashboard instance."""

    with io.open(config_filepath, mode='rt', encoding='utf-8') as f:
        config = json.loads(f.read())

    deployer = tasks.Deploy(config)
    deployer.run()

if __name__ == '__main__':
    cli()
