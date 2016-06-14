[![Build Status](https://travis-ci.org/okfn/data-quality-cli.svg)](https://travis-ci.org/okfn/data-quality-cli)
[![Coverage Status](https://coveralls.io/repos/okfn/data-quality-cli/badge.svg)](https://coveralls.io/r/okfn/data-quality-cli)


USE TABULATOR IN TASKS


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

```

#### source_file schema

`source_file` contains information about sources. It is used by `GoodTables` 
to analyze each source and by `Data Quality CLI` to cache sources and assert the 
performance of each publisher. 

It should contain all the columns specified as keys in the json config 
(egg: `format_key`). If you don't have an `encoding` column, make sure you pass the 
`encoding` argument to the pipeline as shown in the [config structure](###structure-of-jsonconfig).
Besides the columns in the schema below, you can add any other columns you want.

| column_name | optional |  what_is_it |
|---|---|---|
| id | false  | id of the source |
| publisher_id | false | id of the sources's publisher (= `id` column in `publisher_file`)|
| data_key  |  false | the path/url to the source file |
| format_key  |  true | the file format of the source file(eg: csv, xls) |
| encoding_key  | true  | the encoding of the source file (eg: utf-8)|
| schema_key  | true  | the path/url to the source file's schema|
| period_id  | false  | the period this source corresponds to (date or interval of dates) | 


#### publisher_file schema

`publisher_file` contains information about publishers, and it's used by `Data Quality CLI`
to correlate a `publisher_id` with it's sources and additional information. You can 
include any additional columns.

| column_name | optional | default_value| what_is_it|
|---|---|---|---|
| id | false  | - | the id of the publisher |

#### run_file schema

`run_file` keeps track of runs. It is updated after each run. 

| column_name | what_is_it|
|---|---|
| id | id of the run  |
| timestamp | when the run finished |
| total_score |  the total score of all sources analyzed in the run |

#### result_file schema

`result_file` contains the results for each source. It is updated during each run.

| column_name | what_is_it|
|---|---|
| id | id of the result |
| source_id   | id of the source (= `id` column in `source_file`) |
| publisher_id  |  id of the source's publisher (= `publisher_id` column in `source_file`) |
| period_id | id of the source's period (= `period_id` column in `source_file`) |
| score | validity score of the source obtained from `GoodTables` |
| data | the path/url to the source file |
| schema | the path/url to the source's schema |
| summary | |
| run_id | id of the run the result is part of (= `id` column in `run_file`) |
| timestamp | timestamp of the run the result is part of  (= `timestamp` column in `run_file`) |
| report | url to the `GoodTables` report for this result |

#### performance_file schema

`performance_file` contains statistics about publishers performance both individually and
overall . The performance is calculated per period in order to track their progress.

| column_name | what_is_it|
|---|---|
| publisher_id | id of the publisher (= `publisher_id` column in `source_file`) |
| period_id | id of the analyzed period (= `period_id` column in `source_file`) |
| files_count | number of files published during the analyzed period |
| score | average score of the files published in the analyzed period |
| valid | percent of valid sources from the analyzed period |
| files_count_to_date | total number of files published up to the analyzed period |
| score_to_date | average score of all files published up to the analyzed period |
| valid_to_date | number of all valid files published up to the analyzed period |




