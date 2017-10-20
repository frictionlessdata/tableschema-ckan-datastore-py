# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
log = logging.getLogger(__name__)


# Module API

class Mapper(object):

    # Public

    def datastore_fields_to_descriptor(self, fields):
        '''
        Return a table schema descriptor from a DataStore fields object.
        '''
        ts_fields = []
        for f in fields:
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

    def datastore_field_type_to_schema_type(self, type):
        DATASTORE_TYPE_MAPPING = {
            'int2': ('integer', None),
            'int4': ('integer', None),
            'int8': ('integer', None),
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
            'json': ('object', None),
            'jsonb': ('object', None),
            'array': ('array', None)
        }
        try:
            return DATASTORE_TYPE_MAPPING[type]
        except KeyError:
            log.warn('Unsupported DataStore type \'{}\'. Using \'string\'.'
                     .format(type))
            return ('string', None)

    def restore_row(self, record, schema):
        """Restore row from DataStore record
        """
        row = {}
        for field in schema.fields:
            row[field.name] = field.cast_value(record[field.name])
        return row
