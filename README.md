# spd-admin

Command line administration for Spend Publishing Dashboards.

## What's it about?

The `spd-admin` CLI is for administering data that belongs to a Spend Publishing Dashboard (dashboard data is in itself a github repository: <a href="https://github.com/pwalsh/spd-example">see the example repository here</a>).

The proposed workflow is this:

* A deployer/administrator runs the validation over a set of data sources at regular intervals
* The data is managed in a git repository (eg), which the developer has locally
* The deployer/administrator should have a config file for each Spend Publishing Dashboard she is administering
* The deployer/administrator, or possibly content editor, occasionally updates the `sources.csv` file in the data directory with new data sources
* Periodically (once a month, once a quarter), the deployer/administrator does `spd-admin run /path/to-config.json --deploy`. This builds a new set of results for the data, and deploys the updated data back to the central data repository (i.e: GitHub)
* As the Spend Publishing Dashboard is a pure client-side application, as soon as updated data is deployed, the app will start working with the updated data.

Note that  deployer/administrator does not need a new `spd-admin` environment per Spend Publishing Dashboard that she administers. Rather, there must be a config file per dashboard, based on `example-config.json`.

`spd-admin` currently provides two commands: `run` and `deploy`. Read more about these below.

## Install

```
pip install git+https://github.com/okfn/spd-admin.git#egg=spd_admin
```

## Use

```
spd-admin --help
```

### Run

```
spd-admin run /path/to/config.json --deploy
```

Runs a batch processor on all data sources in a data repository of a Spend Publishing Dashboard instance.

Writes aggregated results to the results.csv.

Writes run meta data to the run.csv.

If `--deploy` is passed, then also commits, tags and pushes the new changes back to the data repositories central repository.

### Deploy

```
spd-admin deploy /path/to/config.json
```

Deploys a data repository for a Spend Publishing Dashboard instance.
