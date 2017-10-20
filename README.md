# tableschema-ckan-datastore-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-ckan-datastore-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-ckan-datastore-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-ckan-datastore-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-ckan-datastore-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-ckan-datastore-py.svg)](https://pypi.python.org/pypi/tableschema-ckan-datastore-py)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load CKAN DataStore tables based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-ckan-datastore
```

### Examples

Code examples in this readme requires Python 3.3+ interpreter. You could see even more example in [examples](https://github.com/frictionlessdata/tableschema-ckan-datastore-py/tree/master/examples) directory.

```python
from tableschema import Table
# from sqlalchemy import create_engine

# ::TODO:: appropriate example for datastore

# Load and save table to SQL
# engine = create_engine('sqlite://')
# table = Table('data.csv', schema='schema.json')
# table.save('data', storage='sql', engine=engine)
```

## Documentation

The whole public API of this package is described here and follows semantic versioning rules. Everything outside of this readme are private API and could be changed without any notification on any new version.

### Storage

Package implements [Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage) interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

This driver provides an additional API:

#### `Storage(base_url, dataset_id=None, api_key=None)`
- `base_url (str)` - the base url (and scheme) for the CKAN instance (e.g. http://demo.ckan.org).
- `dataset_id (str)` - id or name of the CKAN dataset we wish to use as the bucket source. If missing, all tables in the DataStore are used.
- `api_key (str)` - either a CKAN user api key or, if in the format `env:CKAN_API_KEY_NAME`, an env var that defines an api key.

## Contributing

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.

To install package and development dependencies into active environment:

```
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

For linting `pylama` configured in `pylama.ini` is used. On this stage it's already
installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://pylama.readthedocs.io/en/latest/.

For example to sort results by error type:

```bash
$ pylama --sort <path>
```

For testing `tox` configured in `tox.ini` is used.
It's already installed into your environment and could be used separately with more fine-grained control as described in documentation - https://testrun.org/tox/latest/.

For example to check subset of tests against Python 2 environment with increased verbosity.
All positional arguments and options after `--` will be passed to `py.test`:

```bash
tox -e py27 -- -v tests/<path>
```

Under the hood `tox` uses `pytest` configured in `pytest.ini`, `coverage`
and `mock` packages. This packages are available only in tox envionments.

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-sql-py/commits/master).

### v0.x

Initial driver implementation.
