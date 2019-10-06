# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import json
from tabulator import Stream
from dotenv import load_dotenv; load_dotenv('.env')  # noqa

from tableschema_ckan_datastore import Storage

"""
This live example assumes a CKAN dataset with `DATASET_ID` exists at
`BASE_URL`, with two resources, `CKAN_RESOURCES`, writable by api_key specified
in the env var `CKAN_API_KEY`.
"""

# edit these if necessary
DATASET_ID = 'test-dataset-010203'
BASE_URL = 'https://demo.ckan.org'
CKAN_RESOURCES = ['79843e49-7974-411c-8eb5-fb2d1111d707',
                  'bd79c992-40f0-454a-a0ff-887f84a792fb']


# Get resources
articles_schema = json.load(io.open('data/articles.json', encoding='utf-8'))
comments_schema = json.load(io.open('data/comments.json', encoding='utf-8'))
articles_data = Stream('data/articles.csv', headers=1, encoding='utf-8').open()
comments_data = Stream('data/comments.csv', headers=1).open()


# Storage
storage = Storage(base_url=BASE_URL, dataset_id=DATASET_ID,
                  api_key='env:CKAN_API_KEY')

buckets = storage.buckets
# print(buckets)

# # Delete buckets
# print('deleting buckets')
# storage.delete(CKAN_RESOURCES, ignore=True)

# Create buckets (bucket names are CKAN resource ids)
storage.create(CKAN_RESOURCES, [articles_schema, comments_schema], force=True)

# Write data to bucket
print('Writing to buckets')
storage.write('79843e49-7974-411c-8eb5-fb2d1111d707', articles_data)
storage.write('bd79c992-40f0-454a-a0ff-887f84a792fb', comments_data)

# List buckets
buckets = storage.buckets
# print(buckets)

for bucket in buckets:
    print(storage.describe(bucket))
    print(storage.read(bucket))
