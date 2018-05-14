# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 10:07:02 2017

@author: RAVITHEJA
"""

import os


mongo_config = {
    'mongo_uri': os.getenv('MONGO_URI', 'localhost:27017'),
    'ssl_required': os.getenv('MONGO_SSL_REQUIRED', False),
    'requires_auth': os.getenv('REQUIRES_AUTH', 'false'),
    'mongo_username': os.getenv('MONGO_USER', 'ravithejab@gmail.com'),
    'mongo_password': os.getenv('MONGO_PASSWORD', 'sl03pois!'),
    'mongo_auth_source': os.getenv('MONGO_AUTH_SOURCE', 'dbadmin'),
    'mongo_auth_mechanism': os.getenv('MONGO_AUTH_MECHANISM', 'MONGODB-CR'),
    'db_name': os.getenv('MONGO_DB_NAME', 'datagov_finance2'),
    'mongo_index_name': os.getenv('MONGO_INDEX_NAME', 'csrt'),
    'meta_colln_name': os.getenv('METADATA_COLLN_NAME', 'METADATA')
}
