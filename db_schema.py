# import sqlite3
import json
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urlencode, urlunsplit
import os, io
from pathlib import Path
import dataset
# import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, BigInteger, SmallInteger, Enum, String, Text, Date, Numeric, Boolean
from sqlalchemy import ForeignKey, ForeignKeyConstraint, MetaData, Table
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT
from sqlalchemy.schema import Index, CreateSchema
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utils.functions import database_exists, create_database

from db_connector import *

db_name = 'lib_ru'
db_ = DB(db_name, use_os_env=True, use_orm=False)

metadata = MetaData(bind=db_.engine)

_authors = Table(
    'authors', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('slug', String(255), nullable=False, unique=True),
    Column('name', String(1000), nullable=False),
    Column('family_parsed', String(255)),
    Column('names_parsed', String(255)),
    Column('live_time', String(255)),
    Column('town', Text),
    Column('litarea', String(255)),
    Column('image_url_filename', String(255)),
    Column('desc', Text),
    Column('name_WS', String(1000)),
    Column('image_filename_wiki', Text),
    Column('image_urls', Text),
    Column('images', Text),
    Column('filename', Text),
    Column('is_author', Boolean, default=1, nullable=False),
    Column('uploaded', Boolean, default=0, nullable=False),
    Column('image_url', Text),
    Column('year_dead', Integer),
)

_titles = Table(
    'titles', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('slug', String(255), nullable=False),
    Column('author_id', Integer, ForeignKey('authors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False),
    Column('year', Integer),
    Column('size', Integer),
    Column('title', Text),
    Column('desc', Text),
    Column('oo', Boolean, default=0),
    Column('do_upload', Boolean, default=0, nullable=False),
    Column('is_same_title_in_ws_already', Boolean, default=0),
    Column('uploaded', Boolean, default=0, nullable=False),
    Column('text_url', Text),
    Column('title_ws', String(500), unique=True),
    Index('titles_author_id_slug_uindex', 'author_id', 'slug', unique=True)
)

_wiki = Table(
    'wikified', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('tid', Integer, ForeignKey('titles.id', ondelete='CASCADE', onupdate='CASCADE'), unique=True,
           nullable=False),
    Column('text', LONGTEXT),
    Column('desc', Text),
)

_wikisource_listpages = Table(
    'wikisource_listpages', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('pagename', String(400), nullable=False, unique=True),
    comment='23.01.2022'
)

_texts_categories_names = Table(
    'texts_categories_names', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('slug', String(255), nullable=False, unique=True),
    Column('name', String(500), unique=True),
    Column('name_ws', String(500), unique=True),
)

_texts_categories = Table(
    'texts_categories', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('tid', Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('category_id', Integer, ForeignKey('texts_categories_names.id', onupdate='CASCADE', ondelete='CASCADE')),
    Index('ix_texts_categories_890bc0857c960d5b', 'tid', 'category_id', unique=True)
)

_images = Table(
    'images', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('tid', Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False),
    Column('urn', String(500), nullable=False),
    Column('filename', String(500), nullable=False),
    Column('name_ws', String(500)),
    Index('images_tid_name_ws_uindex', 'tid', 'name_ws', unique=True)
)

_htmls = Table(
    'htmls', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('tid', Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False, unique=True),
    Column('html', LONGTEXT),
    Column('wiki', LONGTEXT),
)

_desc = Table(
    'desc_', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('tid', Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False,
           unique=True),
    Column('author', Text),
    Column('translator', Text),
    Column('year', Text),
    Column('desc', Text),
    Column('author_tag', Text),
    Column('year_tag', Text),
    Column('annotation_tag', Text),
)

_authors_categories = Table(
    'authors_categories', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name_site', String(500), nullable=False, unique=True),
    Column('name_ws', String(500)),
    Column('text_cat_by_author', String(500)),
    Column('text_lang_by_author', String(100)),
)

# all_tables = Table(
#     'all_tables', metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('name_site', String(500), nullable=False, unique=True),
#     Column('name_ws', String(500)),
#     Column('text_cat_by_author', String(500)),
#     Column('text_lang_by_author', String(100)),
# )


"""
CREATE OR REPLACE VIEW all_tables as
select
#        'http://az.lib.ru' || a.slug || '/' || t.slug as text_url,
       t.slug as slug_text,
       t.text_url,
       t.id as tid,
       t.year,
       t.size,
       t.title,
       t.title_ws,
       t.desc as text_desc_raw,
       d.desc as text_desc,
       w.desc as text_desc_wikified,
       t.oo,
       t.uploaded as uploaded_text,
       t.do_upload,
       t.is_same_title_in_ws_already,
       a.slug as slug_author,
       a.id as author_id,
       a.name,
       a.family_parsed,
       a.names_parsed,
       a.name_WS,
       a.live_time,
       a.town,
       a.litarea,
       a.image_url_filename,
       a.image_filename_wiki,
       a.desc as author_desc,
       a.is_author,
       a.uploaded as uploaded_author,
       a.year_dead,
       h.html,
       h.wiki,
       w.text as wikified,
       d.tid as desc_tid,
       d.author_tag,
       ac.name_ws as author_cat,
       ac.text_lang_by_author as lang
#        h.wikified
from authors a
         left join titles t on a.id = t.author_id
         left join htmls h on t.id = h.tid
         left join desc_ d on t.id = d.tid
         left join wikified w on t.id = w.tid
         left join authors_categories ac on a.litarea = ac.name_site;



"""

# db_host, db_user, db_pw = os.getenv('DB_HOST'),  os.getenv('DB_USER'),  os.getenv('DB_PASSWORD')
# db_url = f'mysql+pymysql://{db_user}:{db_pw}@{db_host}/lib_ru'
# db = dataset.connect(db_url, engine_kwargs=dict(echo=False))

db = db_.connect

authors = db['authors']
authors_with_cat = db['authors_with_cat']
authors_categories = db['authors_categories']
titles = db['titles']
texts_categories_names = db.create_table('texts_categories_names')
texts_categories = db.create_table('texts_categories')
htmls = db.create_table('htmls')
desc = db.create_table('desc_')
wiki = db.create_table('wikified')
images = db.create_table('images')
wikisource_listpages = db.create_table('wikisource_listpages')
all_tables = db['all_tables']
