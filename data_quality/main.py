# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import click
from goodtables import pipeline
from . import tasks, utilities, generators

@click.group()
def cli():
    """The entry point into the CLI."""

@cli.command()
@click.argument('config_filepath')
@click.option('--encoding', default=None)
@click.option('--deploy', is_flag=True)
def run(config_filepath, deploy, encoding):
    """Process data sources for a Spend Publishing Dashboard instance."""

    config = utilities.load_json_config(config_filepath)
    utilities.resolve_dir(config['cache_dir'])
    utilities.set_up_cache_dir(config['cache_dir'])
    source_filepath = os.path.join(config['data_dir'], config['source_file'])

    aggregator = tasks.Aggregator(config)

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
    config['goodtables']['arguments']['batch'].update(post_tasks)
    batch_options = config['goodtables']['arguments']['batch']
    batch_options['pipeline_options'] = config['goodtables']['arguments']['pipeline']
    batch = pipeline.Batch(source_filepath, **batch_options)
    batch.run()


@cli.command()
@click.argument('config_filepath')
def deploy(config_filepath):
    """Deploy data sources for a Spend Publishing Dashboard instance."""

    config = utilities.load_json_config(config_filepath)
    deployer = tasks.Deploy(config)
    deployer.run()


@cli.command()
@click.argument('generator_name')
@click.argument('endpoint')
@click.option('-cf','--config_filepath', type=click.Path(exists=True), default=None,
              help='Full path to the json config for data-quality-cli')
@click.option('-gp','--generator_class_path', default=None,
              help='Path to your custom generator (Ex: mymodule.CustomGenerator)')
@click.option('-ft', '--file_type', multiple=True, default=['csv','excel'],
              help='File types that should be included in sources (default: csv and excel)')
def generate(generator_name, endpoint, config_filepath, generator_class_path, file_type):
    """Generate a database from the given endpoint

    Args:

        generator_name: Name of the generator (ex: ckan)
        endpoint: Url where the generator should get the data from
    """

    file_types = list(file_type)
    config = utilities.load_json_config(config_filepath)
    if not config_filepath:
        config['data_dir'] = utilities.resolve_dir_name(os.getcwd(), config['data_dir'])
    utilities.resolve_dir(config['data_dir'])

    if generator_name not in generators._built_in_generators.keys():
        generator_class_path = (generator_class_path or
                                config.get('generator', {}).get(generator_name,None))
        if not generator_class_path:
            msg = ('You need to provide the path for your custom generator using the'
                   '`--generator_class_path` option or by providing it in the config:'
                   'Ex: {"generator":{"generator_name": "mymodule.CustomGenerator"}}')
            raise ValueError(msg)

    generator = tasks.Generate(config)
    generator.run(generator_name, endpoint, generator_class_path, file_types)


if __name__ == '__main__':
    cli()
