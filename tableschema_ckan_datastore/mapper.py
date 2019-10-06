# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import json
import dateutil
import tableschema

import logging
log = logging.getLogger(__name__)


# Module API

class Mapper(object):

    # Public

    def descriptor_to_datastore_dict(self, descriptor, bucket):
        '''
        Return a datastore dict from a table schema descriptor.
        '''
        schema = tableschema.Schema(descriptor)
        datastore_dict = {
            'fields': [],
            'resource_id': bucket,
            'force': True
        }
        for field in schema.fields:
            datastore_field = {
                'id': field.name
            }
            datastore_type = self.descritor_type_to_datastore_type(field.type)
            if datastore_type:
                datastore_field['type'] = datastore_type
            datastore_dict['fields'].append(datastore_field)

        pk = descriptor.get('primaryKey', None)
        if pk is not None:
            datastore_dict['primary_key'] = pk
        return datastore_dict

    def descritor_type_to_datastore_type(self, type):
        '''
        Return a DataStore field type from a table schema descriptor type.
        '''
        DESCRIPTOR_TYPE_MAPPING = {
            'number': 'float',
            'string': 'text',
            'integer': 'int',
            'boolean': 'bool',
            'object': 'json',
            'array': 'text[]',
            'geojson': 'json',
            'date': 'date',
            'time': 'time',
            'year': 'int',
            'datetime': 'timestamp'
        }
        try:
            return DESCRIPTOR_TYPE_MAPPING[type]
        except KeyError:
            log.warn(
                'Unsupported descriptor type \'{}\'.'.format(type))
            return None

    def datastore_fields_to_descriptor(self, fields):
        '''
        Return a table schema descriptor from a DataStore fields object.
        '''
        ts_fields = []
        for f in fields:
            # Don't include datastore internal field '_id'.
            if f['id'] == '_id':
                continue
            datastore_type = f['type']
            datastore_id = f['id']
            ts_type, ts_format = \
                self.datastore_field_type_to_schema_type(datastore_type)
            ts_field = {
                'name': datastore_id,
                'type': ts_type
            }
            if ts_format is not None:
                ts_field['format'] = ts_format
            ts_fields.append(ts_field)

        return {'fields': ts_fields}

    def datastore_field_type_to_schema_type(self, dstore_type):
        '''
        For a given datastore type, return the corresponding schema type.

        datastore int and float may have a trailing digit, which is stripped.

        datastore arrays begin with an '_'.
        '''
        dstore_type = dstore_type.rstrip('0123456789')
        if dstore_type.startswith('_'):
            dstore_type = 'array'
        DATASTORE_TYPE_MAPPING = {
            'int': ('integer', None),
            'float': ('number', None),
            'smallint': ('integer', None),
            'bigint': ('integer', None),
            'integer': ('integer', None),
            'numeric': ('number', None),
            'money': ('number', None),
            'timestamp': ('datetime', 'any'),
            'date': ('date', 'any'),
            'time': ('time', 'any'),
            'interval': ('duration', None),
            'text': ('string', None),
            'varchar': ('string', None),
            'char': ('string', None),
            'uuid': ('string', 'uuid'),
            'boolean': ('boolean', None),
            'bool': ('boolean', None),
            'json': ('object', None),
            'jsonb': ('object', None),
            'array': ('array', None)
        }
        try:
            return DATASTORE_TYPE_MAPPING[dstore_type]
        except KeyError:
            log.warn('Unsupported DataStore type \'{}\'. Using \'string\'.'
                     .format(dstore_type))
            return ('string', None)

    def restore_row(self, record, schema):
        """Restore row from DataStore record
        """
        row = {}
        for field in schema.fields:
            value = record[field.name]
            if field.type == 'datetime' and value is not None:
                value = dateutil.parser.parse(value)
            row[field.name] = field.cast_value(value)
        return row

    def convert_row(self, row, schema):
        """Convert row to DataStore record
        """
        record = {}
        for i, f in enumerate(schema.fields):
            value = self._uncast_value(row[i], f)
            record[f.name] = value
        return record

    def _uncast_value(self, value, field):
        if field.type in ['integer',
                          'number',
                          'year',
                          'date',
                          'datetime',
                          'time'] and value == '':
            return None
        if field.type in ['array', 'object', 'geojson']:
            if isinstance(value, six.string_types) and value != '':
                return json.loads(value)
            else:
                return None
        return value
