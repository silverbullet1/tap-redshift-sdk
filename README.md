# tap-redshift

`tap-redshift` is a Singer tap for Redshift.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Capabilities

* `about`
* `state`
* `catalog`
* `discover`
* `stream-maps`

## Settings
| Setting                      | Required | Default | Description |
|:-----------------------------|:--------:|:-------:|:------------|
| host                         | False    | None    | Hostname for postgres instance. Note if sqlalchemy_url is set this will be ignored. |
| port                         | False    |    5439 | The port on which postgres is awaiting connection. Note if sqlalchemy_url is set this will be ignored. |
| user                         | False    | None    | User name used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| password                     | False    | None    | Password used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| database                     | False    | None    | Database name. Note if sqlalchemy_url is set this will be ignored. |
| tables                       | False    | None    | List of tables to replicate in `schema.table` format. If not specified, everything is included by default.
| sqlalchemy_url               | False    | None    | SQLAlchemy connection string. This will override using host, user, password, port, dialect, and all ssl settings. Note that you must escape password special characters properly. See https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords. **IMPORTANT**:  AWS recommends using redshift connector instead of the standard psycopg2 one. See https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/ So the dialect+driver should be set to `redshift+redshift_connector` if setting this `sqlalchemy_url` property. |



Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

```bash
pipx install -e .
```

## Configuration

### Accepted Config Options


Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-redshift --about --format=markdown
```


A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-redshift --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-redshift` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-redshift --version
tap-redshift --help
tap-redshift --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-redshift` CLI interface directly using `poetry run`:

```bash
poetry run tap-redshift --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-redshift
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-redshift --version
# OR run a test `elt` pipeline:
meltano elt tap-redshift target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
