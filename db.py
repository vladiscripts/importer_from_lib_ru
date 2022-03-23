#!/usr/bin/env python3
# import requests
# import sqlite3
import json
# from lxml.html import fromstring  # import html5lib
# import parsel
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urlencode, urlunsplit
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
# db = dataset.connect('sqlite:////home/vladislav/var/db/from_lib_ru.sqlite',
#                      engine_kwargs={'connect_args': {'check_same_thread': False}})
# db1 = dataset.connect('sqlite:////home/vladislav/var/db/from_lib_ru.sqlite',
#                       engine_kwargs={'connect_args': {'check_same_thread': False}})
db_host, db_user, db_pw = os.getenv('DB_HOST'),  os.getenv('DB_USER'),  os.getenv('DB_PASSWORD')
db_url = f'mysql+pymysql://{db_user}:{db_pw}@{db_host}/lib_ru'
db = dataset.connect(db_url, engine_kwargs=dict(echo=False))  #pool_size=10, max_overflow=20
T = db.types

authors = db['authors']
authors.create_column('slug', T.string, unique=True)

authors_with_cat = db['authors_with_cat']
authors_categories = db['authors_categories']

titles = db['titles']
titles.create_column('slug', T.string)
titles.create_column('author_id', T.integer, nullable=False)
titles.create_column('year', T.integer)
titles.create_column('size', T.integer)
# titles.create_column('html', type=String)

texts_categories_names = db.create_table('texts_categories_names')
texts_categories_names.create_column('slug', T.text, unique=True, nullable=False)
texts_categories_names.create_column('name', T.text)

texts_categories = db.create_table('texts_categories')
texts_categories.create_column('tid', T.integer)
texts_categories.create_column('category_id', T.integer)
texts_categories.create_index(['tid', 'category_id'], unique=True)

htmls = db.create_table('htmls')
htmls.create_column('tid', T.integer, unique=True, nullable=False)
htmls.create_column('html', T.text)
# htmls.create_column('content', LONGTEXT)
htmls.create_column('wiki', T.text)
htmls.create_column('wikified', T.text)
htmls.create_column('wiki_page', T.text)
htmls.create_column('wiki_title', T.text)

desc = db.create_table('desc_')
desc.create_column('tid', T.integer, unique=True, nullable=False)

# db_wiki = db.create_table('wiki', primary_id='tid', primary_increment=False)
# db_wiki.create_column('wiki', T.integer, unique=True, nullable=False)
# db_wiki.create_column('text', T.text)

images = db.create_table('images')
images.create_column('tid', T.integer, nullable=False)
images.create_column('urn', T.string(length=500), nullable=False)
images.create_column('filename', T.string(length=500), nullable=False)
images.create_column('name_ws', T.string(length=500), unique=True)

wikisource_listpages = db.create_table('wikisource_listpages', primary_id='id', primary_increment=True)

all_tables = db['all_tables']

sql = '''
CREATE VIEW all_tables  as
      select
             a.slug as author_slug,
             a.name,
             a.name_for_WS,
             a.family_parsed_for_WS,
             a.names_parsed_for_WS,
             a.live_time,
             a.town,
             a.litarea,
             a.image_url,
             a.desc as author_desc,
             t.slug as title_slug,
             t.author_id,
             t.year,
             t.size,
             t.title,
             t.desc as title_desc,
             t.oo,
             'http://az.lib.ru' || a.slug || '/' || t.slug
                    as text_url,
             h.tid,
             h.html,
             h.wiki,
             h.wikified,
             h.author,
             h.translator,
             h.desc,
             h.year,
             h.author_tag,
             h.year_tag,
             h.annotation_tag
      from authors a
               left join titles t on a.id = t.author_id
               left join htmls h on t.id = h.tid
'''
# db.execute(sql)
