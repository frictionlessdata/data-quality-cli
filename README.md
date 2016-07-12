[![Build Status](https://travis-ci.org/frictionlessdata/data-quality-cli.svg)](https://travis-ci.org/frictionlessdata/data-quality-cli)
[![Coverage Status](https://coveralls.io/repos/frictionlessdata/data-quality-cli/badge.svg)](https://coveralls.io/r/frictionlessdata/data-quality-cli)


# Data Quality CLI

A command line tool that assesses the quality of a set of data sources (e.g.: CSV files of open data published by a government).

## What's it about?

The `dq` (alias: `dataquality`) CLI is a tool to create and manage a [Data Package](http://specs.frictionlessdata.io/data-packages/) 
from a given source of data that can be used by [Data Quality Dashboard](https://github.com/frictionlessdata/data-quality-dashboard).
The quality assessment is done using [GoodTables](http://goodtables.readthedocs.io/en/latest/index.html) and [can be configured](#quality-config).

The proposed workflow is this:

* An administrator creates a folder for a given project which will be equivalent to a data package.
* The administrator runs the [`dq init`](#init) command to create templates for the configuration file  
and the `datapackage.json` file along with the folder structure.
* The administrator updates the [configuration file](#config) to reflect the structure of the data package 
and optionally to [configure the quality assessment](#quality-config).
* The administrator updates the `datapackage.json` file with information specific to the project
and other customizations. 
* The administrator creates a `source_file` and a `publisher_file`:
    * By using the [generate command](#generate).
    * By using custom scripts ([see this example](https://github.com/okfn/data-quality-uk-25k-spend)).
    * In any other way that is in sync with the [schema](#schema).
* The administrator [runs the validation](#run) over the set of sources.
* The data is managed in a git repository (or other version control system), which the administrator has locally
* The administrator [deploys](#deploy) the data package to a central data repository (ex: GitHub)
* The administrator [updates the configuration](https://github.com/frictionlessdata/data-quality-dashboard#configure-database)
of the corresponding Data Quality Dashboard instance
* The administrator, or possibly content editor, occasionally updates
the `source_file` file in the data directory with new data
* Periodically (once a month, once a quarter), the administrator runs
`dq run /path/to-config.json --deploy`. This builds a new set of results for the data,
and deploys the updated data back to the central data repository
* Since Data Quality Dashboard is a pure client-side application, as soon as updated
data is deployed, the app will start working with the updated data.

## Install

```
pip install git+https://github.com/okfn/dataquality-cli.git#egg=dataquality
```

## Use

```
dq --help
```

### Init

Before starting building the database, it is recommended that you run:


```
dq init --folder_path /path/to/future/datapackage
```

This command will potentially spare you some effort and create a `dq_config.json` file
with the default configuration for Data Quality CLI, a `datapackage.json` with the default
info about the data package and schemas for all the required resources, a `data` folder
that will be used to store the database and a `fetched` folder that will store the 
fetched sources. If you'd like to change the names of these folder or other configuration
options, you can make a `dq_config.json` file before running the command. The command will
leave your config file as it is and create the others according to your configuration.

After running it, you should review and update your `dq_config` and `datapackage.json`
with values specific to your project. 

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

If you want to built a custom Generator, just inherit and overwrite the methods of [`data_quality.generators.BaseGenerator`](data_quality/generators/base.py) class.
To load your custom generator class you need to provide the path to it so that it can be imported via
[importlib.import_module](https://docs.python.org/3/library/importlib.html#importlib.import_module).
You can either provide it in the config, or by using the `--generator_class_path` option:

```
dq generate custom_generator_name endpoint --generator_class_path mymodule.MyGenerator
```

If no config file is provided, the generator will use the [default configuration](###default-configuration)
creating the files in the folder where the command is executed. If you want to change that, use the `--config_file_path` option:

```
dq generate generator_name endpoint --config_file_path path/to/config
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

<a name="config"/>
### Configuration
</a>

#### Structure of json config

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

         # pass options to procesors
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

<a name="default-config"/>
#### Default config
</a>

```json
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
```

<a name="quality-config"/>
#### Quality assessment configuration
</a>

Currently, Data Quality CLI assesses the quality of a file based on its structure and
by comparing its contents against a schema. This is done using the
[built-in processors](http://goodtables.readthedocs.io/en/latest/cli.html) (a.k.a. validators) 
in [GoodTables](http://goodtables.readthedocs.io/en/latest/).

*Note:*  If the files are compressed, they cannot be found at the specified path or the path returns
an HTML page, they will be scored 0.

If you want to add other criteria for quality assessment, you can
[create a custom processor for GoodTables](http://goodtables.readthedocs.io/en/latest/tutorial.html#implementing-a-custom-processor).
Then include the name of your custom processor in the list passed to the `processors` parameter from [data quality config](###structure-of-json-config):
`"processors": ["structure", "schema", "custom_processor"]`.
You can also exclude processors that you don't want by removing them from the list.

##### Structure Processor:

  Checks the structure of a tabular file.

  Ex: blank or duplicate rows, rows that have more/less columns than the header, bad formatting etc.

  Options and their defaults:

  * `ignore_empty_rows: false` - Should empty rows be considered errors or just ignored?
  * `ignore_duplicate_rows: false` - Should duplicate rows be considered errors or just ignored?
  * `ignore_empty_columns: false`
  * `ignore_duplicate_columns: false`
  * `ignore_headerless_columns: false` - Should values in a row that don't correspond to a column be ignored?
  * `empty_strings: None` - A list/set of what should be considered empty string, otherwise only `''` will


##### Schema Processor:

  Compares the content of a tabular file against a [Json Table Schema](http://specs.frictionlessdata.io/json-table-schema/).
  You have the following options for the schema:

  1. Provide a path to the schema for each source in `source_file` and [set the "schema_key"](#config) to the name 
of the column that contains it
  2. Let GoodTables infer the schema for each file from its first few lines (less transparent).

  Options and defaults: 

  * `ignore_field_order: true` - Should columns have the same order as in the schema?
  * `infer_schema: false` - Should the schema be infered? (see above)
  * `process_extra_fields: false` - Should fields that are not present in the schema be infered and checked?
  * `case_insensitive_headers: false` - Should headers be matched with the equivalent field names from schema regardless of case?

  *Note:* If you use the schema processor but you don't provide a schema to compare against, the files will be evaluated as having no errors.

##### Examples:
  To exemplify how using different processors influences the quality assessment, we set up several versions
  of the same dataset: UK public spend over £25000.

  [Here](https://uk-25k-structure-only.herokuapp.com/) is a dashboard whose
  data quality database is assessed only on `structure`. You can find the database and configuration 
  [in this repository](https://github.com/georgiana-b/data-quality-uk-25k-spend/tree/uk-25k-spend-structure-only).

  [This alternative version](https://uk-25k-given-schema.herokuapp.com/)
  uses both `structure` and `schema` processors, comparing each file agaist the
  [spend publishing schema](https://raw.githubusercontent.com/okfn/goodtables/master/examples/hmt/spend-publishing-schema.json).
  It is the official configuration, with its corresponding repository [here](https://github.com/georgiana-b/data-quality-uk-25k-spend/tree/uk-25k-given-schema).

  Lastly, [here is the less predictible version](https://uk-25k-inferred-schema.herokuapp.com/)
  that uses both `structure` and `schema`, but it compares files agaist inferred schemas (i.e. using `infer_schema: true`).Corresponding
  database repostory [here](https://github.com/georgiana-b/data-quality-uk-25k-spend/tree/uk-25k-spend-inferred-schema).

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
the `dq init` command, it will be automatically generated for you from the 
[the default datapackage](data_quality/datapackage.json).
This file will be needed thoughout the app so you'll need to have it. 
Take a look over the [Data Package](http://specs.frictionlessdata.io/data-packages/)
specification if you'd like to customize the it for your project.

*Warning:* The `datapackage.json` file is extensively used thoughtout Data Quality CLI and 
the Data Quality Dashboard. To make sure it is kept in sync with the database that it 
describes, several checks are performed at different steps. While you are free to customize
your database by using custom generators and extra fields,
you have to make sure that the fields required by Data Quality CLI to perform it's tasks are present.

