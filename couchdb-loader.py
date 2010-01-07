#!/usr/bin/env python
# encoding: utf-8
"""
Simple script to import geonames data into couchdb
"""

import sys
import os
import shutil
import tempfile
from couchdb import Server, ResourceNotFound
from uuid import uuid4
import time

start = time.time()



data_file_path = "data/test_large.txt"
field_keys = [
			'geonameid',
			'name',
			'asciiname',
			'alternatenames',
			'lat',
			'long',
			'feature_class',
			'feature_code',
			'country_code',
			'cc2',
			'admin1',
			'admin2',
			'admin3',
			'admin4',
			'population',
			'elevation',
			'gtopo30',
			'timezone',
			'last_updated']

class GeonamesLoader:
	
	total_inserts = 0
	
	def __init__(self):
		
		uri = 'http://localhost:5984/'
		self.cache_dir = tempfile.mkdtemp(prefix='couchdb')
		self.server = Server(uri, cache=self.cache_dir)

		try:
			self.db = self.server['geonames']
		except ResourceNotFound:
			self.db = self.server.create('geonames')

	def load_data(self, data_file_path, field_keys):
		
		data_file = open(data_file_path, 'r')
		docs = []
		max_docs_per_insert = 1000
		i = 0

		for line in data_file:
			values = line.decode('utf8')
			values = values.rstrip().split("\t")
			doc = dict(zip(field_keys, values))

			# remove blank values
			for k, v in doc.items():
				if len(v) == 0:
					del doc[k]

			doc['_id'] = uuid4().hex
			docs.append(doc)
			
			if i == max_docs_per_insert:
				self.bulk_insert_docs(docs)
				docs = []
				i = 0
				
			i = i+1
		
		self.bulk_insert_docs(docs)
		data_file.close()
		
	def bulk_insert_docs(self, docs):
		num_docs = len(docs)
		if num_docs != 0:
			self.db.update(docs)
			print "inserted %d docs" % num_docs
			
			self.total_inserts = self.total_inserts + num_docs
		
	def __del__(self):
		shutil.rmtree(self.cache_dir)	

geonames_loader = GeonamesLoader()

print "Loading data..."
geonames_loader.load_data(data_file_path, field_keys)
print "..... done."

print "Loaded ",geonames_loader.total_inserts," docs in ", time.time() - start, "seconds."
