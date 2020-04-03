# tableschema-ckan-datastore-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-ckan-datastore-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-ckan-datastore-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-ckan-datastore-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-ckan-datastore-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-ckan-datastore.svg)](https://pypi.python.org/pypi/tableschema-ckan-datastore)
[![Github](https://img.shields.io/badge/github-master-brightgreen)](https://github.com/frictionlessdata/tableschema-ckan-datastore-py)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load CKAN DataStore tables based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
  - [Documentation](#documentation)
  - [API Reference](#api-reference)
    - [`Storage`](#storage)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-ckan-datastore
```

## Documentation

When writing data, tableschema-ckan-datastore uses the [CKAN API `datastore_upsert` endpoint](https://ckan.readthedocs.io/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_upsert) with the `upsert` method. This requires a unique key in the data defined by a [Table Schema primary key property](https://specs.frictionlessdata.io/table-schema/#primary-key). If your data has a primary key, you can use the `table.save` method:

```python
from tableschema import Table

# Load and save CKAN DataStore record
resource_id = 'bd79c992-40f0-454a-a0ff-887f84a792fb'
base_url = 'https://demo.ckan.org'
dataset_id = 'test-dataset-010203'
api_key = 'my-ckan-user-api-key'

table = Table('data.csv', schema='schema.json')  # data.csv has primary keys
table.save(resource_id,
           storage='ckan_datastore',
           base_url=base_url,
           dataset_id=dataset_id,
           api_key=api_key)
```

If you need to define the method used to save data to the DataStore, you can create the `tableschema.Storage` object directly and specify which method parameter to use:

```python
import io
import json
from tabulator import Stream
from tableschema import Storage

# Load and save CKAN DataStore record
resource_id = 'bd79c992-40f0-454a-a0ff-887f84a792fb'
base_url = 'https://demo.ckan.org'
dataset_id = 'test-dataset-010203'
api_key = 'my-ckan-user-api-key'

schema = json.load(io.open('schema.json', encoding='utf-8'))
data = Stream('data.csv', headers=1).open()

storage = Storage.connect('ckan_datastore',
                          base_url=base_url,
                          dataset_id=dataset_id,
                          api_key=api_key)
storage.create(resource_id, schema, force=True))
storage.write(resource_id, data, method='insert')  # specify the datastore_upsert method
```

## API Reference

### `Storage`
```python
Storage(self, base_url, dataset_id=None, api_key=None)
```
Ckan Datastore storage

Package implements
[Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage)
interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

> Only additional API is documented

__Arguments__
- __base_url (str)__:
- __the base url (and scheme) for the CKAN instance (e.g. http__://demo.ckan.org).
- __dataset_id (str)__:
        id or name of the CKAN dataset we wish to use as the bucket source.
        If missing, all tables in the DataStore are used.
- __api_key (str)__:
        either a CKAN user api key or, if in the format `env:CKAN_API_KEY_NAME`,
        an env var that defines an api key.


## Contributing

> The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```bash
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-ckan-datastore-py/commits/master).

#### v1.1

- Increase max size of a bucket to 100 resources

#### v1.0

- Initial driver implementation
