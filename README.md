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
`spd-admin run /path/to-config.json --deploy`. This builds a new set of results for the data,
and deploys the updated data back to the central data repository (i.e: GitHub)
* As the Spend Publishing Dashboard is a pure client-side application, as soon as updated
data is deployed, the app will start working with the updated data.

Note that  deployer/administrator does not need a new `dq` environment per Spend Publishing
Dashboard that she administers. Rather, there must be a config file per dashboard,
based on `dq-config.example.json`.

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
