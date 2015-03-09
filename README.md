# spd-admin

Command line administration for Spend Publishing Dashboards.

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
