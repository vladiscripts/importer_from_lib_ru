#!/usr/bin/env python3
import requests
# from vladi_helpers import vladi_helpers
from vladi_helpers.vladi_helpers import url_params_str_to_dict, url_params_str_to_list, \
    cookies_string_from_Chrome_to_list
from vladi_helpers.file_helpers import json_save_to_file, json_load_from_file, file_savetext, file_readtext
from vladi_helpers.vladi_helpers import url_params_str_to_dict, url_params_str_to_list
import sqlite3
import json
from lxml.html import fromstring  # import html5lib
import parsel
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urlencode, urlunsplit
import pandas as pd
import os, io
from pathlib import Path
import dataset
from sqlalchemy import create_engine, Column, Integer, BigInteger, SmallInteger, String, Date, ForeignKey, Numeric, \
    Boolean
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT
from threading import RLock

db_lock = RLock()

# crawler.db = dataset.connect('sqlite:///db.sqlite', engine_kwargs=dict(echo=False))
# db = dataset.connect('sqlite:////home/vladislav/var/from_lib_ru.sqlite')
db = dataset.connect('sqlite:////home/vladislav/var/db/from_lib_ru.sqlite')
T = db.types

db_authors = db['authors']
db_authors.create_column('slug', type=String, unique=True)

db_titles = db['titles']
db_titles.create_column('slug', T.string)
db_titles.create_column('author_id', T.integer)
db_titles.create_column('year', T.integer)
db_titles.create_column('size', T.integer)
# db_titles.create_column('html', type=String)

texts_categories_names = db['texts_categories_names']
db_texts_categories = db['texts_categories']

db_htmls = db['htmls']
db_htmls.create_column('tid', T.integer, unique=True)
db_htmls.create_column('html', MEDIUMTEXT)
db_htmls.create_column('content', MEDIUMTEXT)
db_htmls.create_column('wiki', MEDIUMTEXT)
db_htmls.create_column('wikified', MEDIUMTEXT)

# db_wiki = db['wiki']
# db_wiki.create_column('tid', T.integer, unique=True)
# db_wiki.create_column('text', T.text)
# db_wiki.create_column('wiki', T.text)

db_all_tables = db['all_tables']
