[![Build Status](https://travis-ci.org/frictionlessdata/data-quality-cli.svg)](https://travis-ci.org/frictionlessdata/data-quality-cli)
[![Coverage Status](https://coveralls.io/repos/frictionlessdata/data-quality-cli/badge.svg)](https://coveralls.io/r/frictionlessdata/data-quality-cli)


# Data Quality CLI

A command line tool that assesses the data quality of a set of data sources (e.g.: CSV files of open data published by a government).

## What's it about?

The `dq` (alias: `dataquality`) CLI is a batch processor

is for administering data that belongs to a Spend Publishing

Dashboard (dashboard data is in itself a github repository:
<a href="https://github.com/okfn/spd-data-example">see the example repository here</a>).

The proposed workflow is this:

* A deployer/administrator runs the validation over a set of data sources at regular intervals
* The data is managed in a git repository (eg), which the developer has locally
* The deployer/administrator should have a config file for each Spend Publishing
Dashboard she is administering
* The deployer/administrator, or possibly content editor, occasionally updates
the `sources.csv` file in the data directory with new data sources
* Periodically (once a month, once a quarter), the deployer/administrator does
`dq run /path/to-config.json --deploy`. This builds a new set of results for the data,
and deploys the updated data back to the central data repository (i.e: GitHub)
* As the Spend Publishing Dashboard is a pure client-side application, as soon as updated
data is deployed, the app will start working with the updated data.

Note that  deployer/administrator does not need a new `dq` environment per Spend Publishing
Dashboard that she administers. Rather, there must be a config file per dashboard,
based on `dq-config.example.json`. For more info about the structure of the config file
see [this section](###structure-of-jsonconfig).

`dq` currently provides two commands: `run` and `deploy`. Read more about these below.

## Install

```
pip install git+https://github.com/okfn/dataquality-cli.git#egg=dataquality
```

## Use

```
dq --help
```

### Run

```
dq run /path/to/config.json --deploy
```

Runs a *data quality assessment* on all data sources in a data repository.

* Writes aggregated results to the results.csv.
* Writes run meta data to the run.csv.
* If `--deploy` is passed, then also commits, tags and pushes the new changes back to the data repositories central repository.

### Deploy

```
dq deploy /path/to/config.json
```

Deploys this Data Quality repository to a remote.

### Generate

Generic command:

```
dq generate generator_name http://endpoint_to_data_sources
```

There is currently one built-in generator for [CKAN](http://ckan.org/) instances.
Ex: In the example below, we generate a database from `data.qld.gov.au`:

```
dq generate ckan https://data.qld.gov.au/
```

By default, it will include only `CSV` and `excel`(`XLS`, `XLSX`) files. If you want to change that use
the `--file_type` option. In the example below, we ask for `CSV` and `TXT`:

```
dq generate ckan https://data.qld.gov.au/  --file_type csv --file_type txt
```

If you want to built a custom Generator, just inherit and overwrite the methods of `data_quality.generators.BaseGenerator` class.
To load your custom generator class you need to provide the path to it so that it can be imported via
[importlib.import_module](https://docs.python.org/3/library/importlib.html#importlib.import_module).
You can either provide it in the config, or by using the `--generator_class_path` option:

```
dq generate custom_generator_name endpoint --generator_class_path mymodule.MyGenerator

```

If no config file is provided, the generator will use the [default configuration](###default-configuration)
creating the files in the folder where the command is executed. If you want to change that, use the `--config_filepath` option:

```
dq generate generator_name endpoint --config_filepath path/to/config
```

### Structure of json config

```json
{
  # folder that contains the source_file and publisher_file
  "data_dir": "data",

  # folder that will store each source as local cache
  "cache_dir": "fetched",

  # file that will contain the result for each source
  "result_file": "results.csv",

  # file  that will contain the report for each collection of sources
  "run_file": "runs.csv",

  # file containing the collection of sources that will be analyzed
  "source_file": "sources.csv",

  # file containing the publishers of the above mentioned sources
  "publisher_file": "publishers.csv",

  # will contain the results for each publisher
  "performance_file": "performance.csv",

  "remotes": ["origin"],
  "branch": "master",
  
  #name and path to custom generator (this name should be used when executing the generate command)
  "generator": {"my_generator_name": "my_module.MyGenerator" },

  # options for GoodTables ("http://goodtables.readthedocs.org/en/latest/")
  "goodtables": {

    # set base url for the report links
    "goodtables_web": "http://goodtables.okfnlabs.org",

    "arguments": {

      # options for pipeline ("http://goodtables.readthedocs.org/en/latest/pipeline.html")
      "pipeline": {

        # what processors will analyze every pipeline
        "processors": ["structure", "schema"],

        # specify encoding for every pipeline 
          (use this if all the files have the same encoding)
        "encoding": "ISO-8859-2",

         # pass options to procesors ("http://goodtables.readthedocs.org/en/latest/pipeline.html#validator-options")
        "options": {
          "schema": {"case_insensitive_headers": true}
        }
      },

      # options for batch ("http://goodtables.readthedocs.org/en/latest/batch.html")
      "batch": {

        # column from source_file containing path/url to data source
        "data_key": "data",

        # column from source_file containing path/url to schema
        "schema_key": "schema",

        # column from source_file containing file format (csv, xls)
        "format_key": "format",

        # column from source_file containings file encoding
          (use this if you want to specify encoding for each source separately)
        "encoding_key": "encoding",

        # time in seconds to wait between pipelines
        "sleep": 2,

        # execute something after the analysis of a batch is finished
        "post_task": "",

        # execute something after the analysis  of a pipeline is finished
        "pipeline_post_task": "",
      }
    }
  }
}
```

### Default config 

````json
{
    "data_dir": "current_working_directory/data",
    "cache_dir": "current_working_directory/fetched",
    "result_file": "results.csv",
    "run_file": "runs.csv",
    "source_file": "sources.csv",
    "publisher_file": "publishers.csv",
    "performance_file": "performance.csv",
    "remotes": ["origin"],
    "branch": "master",
    "goodtables": {
        "goodtables_web": "http://goodtables.okfnlabs.org",
        "arguments": {
            "pipeline": {},
            "batch": {
                "data_key": "data"
            }
        }
    }
}
````

### Schema

`Data Quality CLI` expects the following structure of the project folder, where 
the names of files and folders are the ones defined in the json config given to  `dq run`:

```
project
│
└──────data_dir
    │   source_file
    │   publisher_file
    │   run_file
    │   result_file
    │   performance_file
    │
    └───cache_dir
    │
    └───datapackage.json
```

The `datapackage.json` file is required in order to make the project
a valid [Data Package](http://specs.frictionlessdata.io/data-packages/). If you use
the `dq generate` command, it will be automatically generated for you.

An important note is that Data Quality CLI has it's own default datapackage file
which is used to define the schema for all the files that it creates: `run_file`,
`result_file`, `performance_file` and, if you use `dq generate`, for `sources_file` and 
`publisher_file`. Please take a look over [the default datapackage](data_quality/datapackage.json)
to get a better understanding of what these files contain. Also take a look 
over the [Data Package](http://specs.frictionlessdata.io/data-packages/)
specification if you'd like to customize the one in your project.

*Warning:* Changing the `schema` section for the files described in the generated `datapackage` 
can lead to inconsistencies between the expected schema and the actual contents 
of the files. If you'd like to include different fields in the `source_file` or
`publisher_file`, it's best to create your own generator that can work with the custom
schema. 
