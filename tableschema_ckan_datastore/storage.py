# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import json
import collections
import tableschema

from . import utils
from .mapper import Mapper

import logging
log = logging.getLogger(__name__)


# Module API

class Storage(tableschema.Storage):
    """Ckan Datastore storage

    Package implements
    [Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage)
    interface (see full documentation on the link):

    ![Storage](https://i.imgur.com/RQgrxqp.png)

    > Only additional API is documented

    # Arguments
        base_url (str):
            the base url (and scheme) for the CKAN instance (e.g. http://demo.ckan.org).
        dataset_id (str):
            id or name of the CKAN dataset we wish to use as the bucket source.
            If missing, all tables in the DataStore are used.
        api_key (str):
            either a CKAN user api key or, if in the format `env\\:CKAN_API_KEY_NAME`,
            an env var that defines an api key.

    """

    # Public

    def __init__(self, base_url, dataset_id=None, api_key=None):

        # Set attributes
        base_path = "/api/3/action"
        self.__base_url = base_url.rstrip('/')
        self.__base_endpoint = self.__base_url + base_path
        self.__dataset_id = dataset_id
        self.__api_key = api_key
        self.__descriptors = {}
        self.__max_pages = 100
        self.__bucket_cache = None

        # Create mapper
        self.__mapper = Mapper()

    def __repr__(self):

        # Template and format
        template = 'Storage <{base_url}>'
        text = template.format(base_url=self.__base_url)

        return text

    @property
    def buckets(self):
        if self.__bucket_cache:
            return self.__bucket_cache

        params = {
            'resource_id': '_table_metadata'
        }
        if self.__dataset_id is not None:
            filter_ids = self.__get_resource_ids_for_dataset(self.__dataset_id)
            params.update({'filters': json.dumps({'name': filter_ids})})

        datastore_search_url = \
            "{}/datastore_search".format(self.__base_endpoint)

        response = self._make_ckan_request(datastore_search_url, params=params)

        buckets = [r['name'] for r in response['result']['records']]

        count = 1
        while response['result']['records']:
            count += 1
            next_url = self.__base_url + response['result']['_links']['next']
            response = self._make_ckan_request(next_url)
            records = response['result']['records']
            if records:
                buckets = buckets + [r['name']
                                     for r in response['result']['records']]
            if count == self.__max_pages:
                log.warn("Max bucket count exceeded. {} buckets returned."
                         .format(len(buckets)))
                break
        self.__bucket_cache = buckets
        return buckets

    def create(self, bucket, descriptor, force=False):

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]

        # Check buckets for existence
        for bucket in reversed(self.buckets):
            if bucket in buckets:
                if not force:
                    message = 'Bucket "%s" already exists.' % bucket
                    raise tableschema.exceptions.StorageError(message)
                self.delete(bucket)

        # Iterate over buckets/descriptors
        for bucket, descriptor in zip(buckets, descriptors):
            # Define resources
            tableschema.validate(descriptor)
            self.__descriptors[bucket] = descriptor
            datastore_dict = \
                self.__mapper.descriptor_to_datastore_dict(descriptor, bucket)
            datastore_create_url = \
                "{}/datastore_create".format(self.__base_endpoint)
            self._make_ckan_request(datastore_create_url, method='POST',
                                    json=datastore_dict)

        # Invalidate cache
        self.__bucket_cache = None

    def delete(self, bucket=None, ignore=False):
        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        elif bucket is None:
            buckets = reversed(self.buckets)

        for bucket in buckets:
            # Check existent
            if bucket not in self.buckets:
                if not ignore:
                    message = 'Bucket "%s" doesn\'t exist.' % bucket
                    raise tableschema.exceptions.StorageError(message)
                return

            # Remove from buckets
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            datastore_delete_url = \
                "{}/datastore_delete".format(self.__base_endpoint)
            params = {
                'resource_id': bucket,
                'force': True
            }
            self._make_ckan_request(datastore_delete_url, method='POST',
                                    json=params)

        # Invalidate cache
        self.__bucket_cache = None

    def describe(self, bucket, descriptor=None):

        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                datastore_search_url = \
                    "{}/datastore_search".format(self.__base_endpoint)
                params = {
                    'limit': 0,
                    'resource_id': bucket
                }
                response = self._make_ckan_request(datastore_search_url,
                                                   params=params)

                fields = response['result']['fields']
                descriptor = \
                    self.__mapper.datastore_fields_to_descriptor(fields)

        return descriptor

    def iter(self, bucket):
        schema = tableschema.Schema(self.describe(bucket))

        datastore_search_url = \
            "{}/datastore_search".format(self.__base_endpoint)
        params = {
            'resource_id': bucket
        }
        response = self._make_ckan_request(datastore_search_url,
                                           params=params)
        while response['result']['records']:
            for row in response['result']['records']:
                row = self.__mapper.restore_row(row, schema=schema)
                yield row
            next_url = self.__base_url + response['result']['_links']['next']
            response = self._make_ckan_request(next_url)

    def read(self, bucket):
        rows = list(self.iter(bucket))
        return rows

    def write(self, bucket, rows, method="upsert", as_generator=False):
        if as_generator:
            return self.write_aux(bucket, rows, method=method)
        else:
            collections.deque(self.write_aux(bucket, rows, method=method), maxlen=0)

    def write_aux(self, bucket, rows, method="upsert"):
        schema = tableschema.Schema(self.describe(bucket))
        datastore_upsert_url = \
            "{}/datastore_upsert".format(self.__base_endpoint)
        records = []
        for r in rows:
            records.append(self.__mapper.convert_row(r, schema))
            yield r
        params = {
            'resource_id': bucket,
            'method': method,
            'force': True,
            'records': records
        }
        self._make_ckan_request(datastore_upsert_url, method='POST',
                                json=params)

    # Private

    def __get_resource_ids_for_dataset(self, dataset_id):
        '''Get a list of resource ids for the passed dataset id.
        '''
        package_show_url = "{}/package_show".format(self.__base_endpoint)
        response = self._make_ckan_request(package_show_url,
                                           params=dict(id=dataset_id))

        dataset = response['result']
        resources = dataset['resources']
        resource_ids = [r['id'] for r in resources]
        return resource_ids

    def _make_ckan_request(self, datastore_url, **kwargs):
        response = utils.make_ckan_request(datastore_url,
                                           api_key=self.__api_key,
                                           **kwargs)

        ckan_error = utils.get_ckan_error(response)
        if ckan_error:
            msg = 'CKAN returned an error: ' + json.dumps(ckan_error)
            raise tableschema.exceptions.StorageError(msg)

        return response
