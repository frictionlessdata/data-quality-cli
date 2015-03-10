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
from spd_admin import tasks


@click.group()
def cli():
    """The entry point into the CLI."""


@cli.command()
@click.argument('config_filepath')
@click.option('--deploy', is_flag=True)
def run(config_filepath, deploy):

    """Process data sources for a Spend Publishing Dashboard instance."""

    with io.open(config_filepath, mode='rt', encoding='utf-8') as f:
        config = json.loads(f.read())

    source_filepath = os.path.join(config['data_dir'], config['source_file'])
    aggregator = tasks.Aggregator(config)
    batch_options = {'pipeline_post_task': aggregator.run,
                     'data_key': 'url'}

    if deploy:

        def batch_handler(*args):
            aggregator.write_run()
            deployer = tasks.Deploy(config)
            deployer.run()

    else:

        def batch_handler(*args):
            aggregator.write_run()

    batch_options['post_task'] = batch_handler
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
