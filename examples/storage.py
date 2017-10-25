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


# Get resources
articles_schema = json.load(io.open('data/articles.json', encoding='utf-8'))
comments_schema = json.load(io.open('data/comments.json', encoding='utf-8'))
articles_data = Stream('data/articles.csv').open()
comments_data = Stream('data/comments.csv').open()

# Engine
base_url = 'https://demo.ckan.org'
ckan_resources = ['79843e49-7974-411c-8eb5-fb2d1111d707',
                  'bd79c992-40f0-454a-a0ff-887f84a792fb']

# Storage
storage = Storage(base_url=base_url, dataset_id='test-dataset-010203',
                  api_key='env:CKAN_API_KEY')

buckets = storage.buckets
print(buckets)

# Delete buckets
print('deleting buckets')
storage.delete(ckan_resources, ignore=True)

# Create buckets (bucket names are CKAN resource ids)
storage.create(ckan_resources, [articles_schema, comments_schema], force=True)

# Write data to bucket
storage.write('articles', articles_data)
storage.write('comments', comments_data)

# List buckets
buckets = storage.buckets
print(buckets)

for bucket in buckets:
    print(storage.describe(bucket))
    print(storage.read(bucket))
