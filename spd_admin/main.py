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
from . import tasks


@click.group()
def cli():
    """The entry point into the CLI."""


@cli.command()
@click.argument('config_filepath')
@click.option('--deploy', is_flag=True)
def run(config_filepath, deploy):

    """Process the data sources for a Spend Publishing Dashboard instance."""

    with io.open(config_filepath, mode='rt', encoding='utf-8') as f:
        config = json.loads(f.read())

    source_filepath = os.path.join(config['data_dir'], config['source_file'])
    aggregator = tasks.Aggregator(config)
    batch_options = {'pipeline_post_process_handler': aggregator.run}

    if deploy:
        deployer = tasks.Deploy(config)
        batch_options['batch_process_handler'] = deployer.run

    batch = pipeline.Batch(source_filepath, **batch_options)
    batch.run()


@click.argument('config_filepath')
def deploy(config_filepath):

    with io.open(config_filepath, mode='rt', encoding='utf-8') as f:
        config = json.loads(f.read())

    deployer = tasks.Deploy(config)
    deployer.run()


if __name__ == '__main__':
    cli()
