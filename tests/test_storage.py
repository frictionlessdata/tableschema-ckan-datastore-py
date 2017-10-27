# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import json
import unittest
import tableschema
from decimal import Decimal
import datetime
import requests_mock
from tabulator import Stream
from tableschema_ckan_datastore import Storage


# Tests

class TestStorage(unittest.TestCase):

    def setUp(self):
        # Create storage
        base_url = 'https://demo.ckan.org'
        self.storage = Storage(base_url=base_url,
                               dataset_id='my-dataset-id',
                               api_key='env:CKAN_API_KEY')

    def test_storage_repr(self):
        assert str(self.storage) == 'Storage <https://demo.ckan.org>'

    @requests_mock.mock()
    def test_storage_buckets(self, mock_request):

        mock_package_show_fp = "tests/mock_responses/package_show.json"
        mock_package_show = \
            json.load(io.open(mock_package_show_fp, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/package_show',
                         json=mock_package_show)

        # First response gets the _table_metadata results
        mock_datastore_search_fp_01 = \
            "tests/mock_responses/datastore_search_table_metadata_01.json"
        mock_datastore_search_01 = \
            json.load(io.open(mock_datastore_search_fp_01, encoding='utf-8'))
        # Second response ensures the _table_metadata results are exhausted
        mock_datastore_search_fp_02 = \
            "tests/mock_responses/datastore_search_table_metadata_02.json"
        mock_datastore_search_02 = \
            json.load(io.open(mock_datastore_search_fp_02, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search',
                         [{'json': mock_datastore_search_01},
                          {'json': mock_datastore_search_02}])

        bucket_list = self.storage.buckets
        assert bucket_list == ['bd79c992-40f0-454a-a0ff-887f84a792fb',
                               '79843e49-7974-411c-8eb5-fb2d1111d707']

        history = mock_request.request_history
        assert len(history) == 3

        # Calling again shouldn't make new requests
        bucket_list = self.storage.buckets
        assert bucket_list == ['bd79c992-40f0-454a-a0ff-887f84a792fb',
                               '79843e49-7974-411c-8eb5-fb2d1111d707']

        history = mock_request.request_history
        assert len(history) == 3

    @requests_mock.mock()
    def test_storage_describe(self, mock_request):

        # Response gets the resource results
        mock_datastore_search_fp_03 = \
            "tests/mock_responses/datastore_search_describe.json"
        mock_datastore_search_03 = \
            json.load(io.open(mock_datastore_search_fp_03, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search?resource_id=79843e49-7974-411c-8eb5-fb2d1111d707',  # noqa
                         json=mock_datastore_search_03)

        expected_descriptor = {
            'fields': [
                {'name': 'id', 'type': 'integer'},
                {'name': 'parent', 'type': 'integer'},
                {'name': 'name', 'type': 'string'},
                {'name': 'current', 'type': 'boolean'},
                {'name': 'rating', 'type': 'number'},
                {'name': 'created_year', 'type': 'integer'},
                {'name': 'created_date', 'type': 'date', 'format': 'any'},
                {'name': 'created_time', 'type': 'time', 'format': 'any'},
                {'name': 'created_datetime',
                    'type': 'datetime', 'format': 'any'},
                {'name': 'stats', 'type': 'object'},
                {'name': 'persons', 'type': 'array'},
                {'name': 'location', 'type': 'object'}
            ]
        }
        assert self.storage.describe('79843e49-7974-411c-8eb5-fb2d1111d707') \
            == expected_descriptor

    @requests_mock.mock()
    def test_storage_delete(self, mock_request):

        mock_package_show_fp = "tests/mock_responses/package_show.json"
        mock_package_show = \
            json.load(io.open(mock_package_show_fp, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/package_show',
                         json=mock_package_show)

        # First response gets the _table_metadata results
        mock_datastore_search_fp_01 = \
            "tests/mock_responses/datastore_search_table_metadata_01.json"
        mock_datastore_search_01 = \
            json.load(io.open(mock_datastore_search_fp_01, encoding='utf-8'))
        # Second response ensures the _table_metadata results are exhausted
        mock_datastore_search_fp_02 = \
            "tests/mock_responses/datastore_search_table_metadata_02.json"
        mock_datastore_search_02 = \
            json.load(io.open(mock_datastore_search_fp_02, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search',
                         [{'json': mock_datastore_search_01},
                          {'json': mock_datastore_search_02}])

        mock_datastore_delete = "tests/mock_responses/datastore_delete.json"
        mock_datastore_delete = \
            json.load(io.open(mock_datastore_delete, encoding='utf-8'))
        mock_request.post('https://demo.ckan.org/api/3/action/datastore_delete',  # noqa
                         json=mock_datastore_delete)

        # Delete buckets
        self.storage.delete('79843e49-7974-411c-8eb5-fb2d1111d707')

        # delete endpoint was called
        history = mock_request.request_history
        assert len(history) == 4
        delete_request = history[3]
        assert delete_request.url == \
            'https://demo.ckan.org/api/3/action/datastore_delete'
        assert delete_request.json()['resource_id'] == \
            '79843e49-7974-411c-8eb5-fb2d1111d707'

    @requests_mock.mock()
    def test_storage_delete_non_exists(self, mock_request):
        '''Attempt delete of non-existing record'''
        mock_package_show_fp = "tests/mock_responses/package_show.json"
        mock_package_show = \
            json.load(io.open(mock_package_show_fp, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/package_show',
                         json=mock_package_show)

        # First response gets the _table_metadata results
        mock_datastore_search_fp_01 = \
            "tests/mock_responses/datastore_search_table_metadata_01.json"
        mock_datastore_search_01 = \
            json.load(io.open(mock_datastore_search_fp_01, encoding='utf-8'))
        # Second response ensures the _table_metadata results are exhausted
        mock_datastore_search_fp_02 = \
            "tests/mock_responses/datastore_search_table_metadata_02.json"
        mock_datastore_search_02 = \
            json.load(io.open(mock_datastore_search_fp_02, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search',
                         [{'json': mock_datastore_search_01},
                          {'json': mock_datastore_search_02}])

        # Delete buckets

        # Delete non existent bucket
        with self.assertRaises(tableschema.exceptions.StorageError):
            self.storage.delete('non_existent')

        # delete endpoint was called
        history = mock_request.request_history
        assert len(history) == 3

    @requests_mock.mock()
    def test_storage_delete_all(self, mock_request):

        mock_package_show_fp = "tests/mock_responses/package_show.json"
        mock_package_show = \
            json.load(io.open(mock_package_show_fp, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/package_show',
                         json=mock_package_show)

        # First response gets the _table_metadata results
        mock_datastore_search_fp_01 = \
            "tests/mock_responses/datastore_search_table_metadata_01.json"
        mock_datastore_search_01 = \
            json.load(io.open(mock_datastore_search_fp_01, encoding='utf-8'))
        # Second response ensures the _table_metadata results are exhausted
        mock_datastore_search_fp_02 = \
            "tests/mock_responses/datastore_search_table_metadata_02.json"
        mock_datastore_search_02 = \
            json.load(io.open(mock_datastore_search_fp_02, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search',
                         [{'json': mock_datastore_search_01},
                          {'json': mock_datastore_search_02}])

        mock_datastore_delete = "tests/mock_responses/datastore_delete.json"
        mock_datastore_delete = \
            json.load(io.open(mock_datastore_delete, encoding='utf-8'))
        mock_request.post('https://demo.ckan.org/api/3/action/datastore_delete',  # noqa
                         json=mock_datastore_delete)

        # Delete buckets
        self.storage.delete()

        # delete endpoint was called
        history = mock_request.request_history
        assert len(history) == 5
        delete_request = history[4]
        assert delete_request.url == \
            'https://demo.ckan.org/api/3/action/datastore_delete'
        assert delete_request.json()['resource_id'] == \
            'bd79c992-40f0-454a-a0ff-887f84a792fb'

    @requests_mock.mock()
    def test_storage_create_exists(self, mock_request):
        '''Datastore Records already exists of resource. Raise exception'''

        mock_package_show_fp = "tests/mock_responses/package_show.json"
        mock_package_show = \
            json.load(io.open(mock_package_show_fp, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/package_show',
                         json=mock_package_show)

        # First response gets the _table_metadata results
        mock_datastore_search_fp_01 = \
            "tests/mock_responses/datastore_search_table_metadata_01.json"
        mock_datastore_search_01 = \
            json.load(io.open(mock_datastore_search_fp_01, encoding='utf-8'))
        # Second response ensures the _table_metadata results are exhausted
        mock_datastore_search_fp_02 = \
            "tests/mock_responses/datastore_search_table_metadata_02.json"
        mock_datastore_search_02 = \
            json.load(io.open(mock_datastore_search_fp_02, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search',
                         [{'json': mock_datastore_search_01},
                          {'json': mock_datastore_search_02}])

        articles_schema = \
            json.load(io.open('data/articles.json', encoding='utf-8'))
        comments_schema = \
            json.load(io.open('data/comments.json', encoding='utf-8'))

        with self.assertRaises(tableschema.exceptions.StorageError):
            self.storage.create(['79843e49-7974-411c-8eb5-fb2d1111d707',
                                 'bd79c992-40f0-454a-a0ff-887f84a792fb'],
                                [articles_schema, comments_schema])

    @requests_mock.mock()
    def test_storage_create(self, mock_request):

        mock_package_show_fp = "tests/mock_responses/package_show.json"
        mock_package_show = \
            json.load(io.open(mock_package_show_fp, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/package_show',
                         json=mock_package_show)

        # First response has no records
        mock_datastore_search_fp_02 = \
            "tests/mock_responses/datastore_search_table_metadata_02.json"
        mock_datastore_search_02 = \
            json.load(io.open(mock_datastore_search_fp_02, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search',
                         json=mock_datastore_search_02)

        mock_datastore_create_fp = "tests/mock_responses/datastore_create.json"
        mock_datastore_create = \
            json.load(io.open(mock_datastore_create_fp, encoding='utf-8'))
        mock_request.post(
            'https://demo.ckan.org/api/3/action/datastore_create',
            json=mock_datastore_create)

        articles_schema = \
            json.load(io.open('data/articles.json', encoding='utf-8'))

        self.storage.create('79843e49-7974-411c-8eb5-fb2d1111d707',
                            articles_schema)
        # create endpoint was called
        history = mock_request.request_history
        assert len(history) == 3
        create_request = history[2]
        assert create_request.url == \
            'https://demo.ckan.org/api/3/action/datastore_create'

    @requests_mock.mock()
    def test_storage_write(self, mock_request):

        # Response gets the resource results
        mock_datastore_search_fp_03 = \
            "tests/mock_responses/datastore_search_describe.json"
        mock_datastore_search_03 = \
            json.load(io.open(mock_datastore_search_fp_03, encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search?resource_id=79843e49-7974-411c-8eb5-fb2d1111d707',  # noqa
                         json=mock_datastore_search_03)

        mock_datastore_upsert_fp = \
            "tests/mock_responses/datastore_upsert.json"
        mock_datastore_upsert = \
            json.load(io.open(mock_datastore_upsert_fp, encoding='utf-8'))
        mock_request.post('https://demo.ckan.org/api/3/action/datastore_upsert',  # noqa
                         json=mock_datastore_upsert)

        # Write data
        articles_data = \
            Stream('data/articles.csv', headers=1, encoding='utf-8').open()
        self.storage.write('79843e49-7974-411c-8eb5-fb2d1111d707',
                           articles_data)
        # create endpoint was called
        history = mock_request.request_history
        assert len(history) == 2
        write_request = history[1]
        assert write_request.url == \
            'https://demo.ckan.org/api/3/action/datastore_upsert'

    @requests_mock.mock()
    def test_storage_read_iter(self, mock_request):

        # Response gets the resource descriptors
        mock_datastore_search_describe_fp = \
            "tests/mock_responses/datastore_search_describe.json"
        mock_datastore_search_describe = \
            json.load(io.open(mock_datastore_search_describe_fp,
                              encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search?resource_id=79843e49-7974-411c-8eb5-fb2d1111d707&limit=0',  # noqa
                         json=mock_datastore_search_describe)

        # Response gets the resource results
        mock_datastore_search_rows_fp = \
            "tests/mock_responses/datastore_search_rows.json"
        mock_datastore_search_rows = \
            json.load(io.open(mock_datastore_search_rows_fp, encoding='utf-8'))
        mock_datastore_search_rows_empty_fp = \
            "tests/mock_responses/datastore_search_rows_empty.json"
        mock_datastore_search_rows_empty = \
            json.load(io.open(mock_datastore_search_rows_empty_fp,
                              encoding='utf-8'))
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search?resource_id=79843e49-7974-411c-8eb5-fb2d1111d707',  # noqa
                         json=mock_datastore_search_rows, complete_qs=True)
        mock_request.get('https://demo.ckan.org/api/3/action/datastore_search?resource_id=79843e49-7974-411c-8eb5-fb2d1111d707&offset=100',  # noqa
                         json=mock_datastore_search_rows_empty,
                         complete_qs=True)

        expected_data = [
            {
                'id': 1, 'parent': None, 'name': 'Taxes', 'current': True,
                'rating': Decimal('9.5'),
                'created_year': 2015,
                'created_date': datetime.date(2015, 1, 1),
                'created_time': datetime.time(3, 0),
                'created_datetime': datetime.datetime(2015, 1, 1, 3, 0),
                'stats': {'chars': 560, 'height': 54.8},
                'persons': ['mike', 'alice'],
                'location': {'type': 'Point', 'coordinates': [50.0, 50.0]}
            }, {
                'id': 2, 'parent': 1,
                'name': '中国人',
                'current': False,
                'rating': Decimal('7'),
                'created_year': 2015,
                'created_date': datetime.date(2015, 12, 31),
                'created_time': datetime.time(15, 45, 33),
                'created_datetime':
                    datetime.datetime(2015, 12, 31, 15, 45, 33),
                'stats': {'chars': 970, 'height': 40},
                'persons': ['chen', 'alice'],
                'location': {'type': 'Point', 'coordinates': [33.33, 33.33]}
            }, {
                'id': 3, 'parent': 1, 'name': None,
                'current': False, 'rating': None,
                'created_year': None, 'created_date': None,
                'created_time': None, 'created_datetime': None,
                'stats': None, 'persons': None, 'location': None}]

        assert self.storage.read('79843e49-7974-411c-8eb5-fb2d1111d707') == \
            expected_data
