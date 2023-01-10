# import sqlite3
import json
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urlencode, urlunsplit
import os, io
from pathlib import Path
import dataset
# import sqlalchemy
from sqlalchemy import Column, Integer, BigInteger, SmallInteger, String, Text, Date, Numeric, Boolean, DateTime
from sqlalchemy import ForeignKey, ForeignKeyConstraint, MetaData, Table
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT, TINYINT
from sqlalchemy.schema import Index, CreateSchema
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utils.functions import database_exists, create_database
from sqlalchemy_utils import create_view
from sqlalchemy.future import select
from sqlalchemy import func

from db.connector import *
from .schema import *


# db_host, db_user, db_pw = os.getenv('DB_HOST'),  os.getenv('DB_USER'),  os.getenv('DB_PASSWORD')
# db_url = f'mysql+pymysql://{db_user}:{db_pw}@{db_host}/lib_ru'
# db = dataset.connect(db_url, engine_kwargs=dict(echo=False))

connect = dataset.connect(str(db_.engine.url), engine_kwargs=dict(echo=db_.engine.echo))
dbd = connect

dbd.authors = dbd['authors']
dbd.authors_with_cat = dbd['authors_with_cat']
dbd.authors_categories = dbd['authors_categories']
dbd.titles = dbd['titles']
dbd.texts_categories_names = dbd.create_table('texts_categories_names')
dbd.texts_categories = dbd.create_table('texts_categories')
dbd.htmls = dbd.create_table('htmls')
dbd.desc = dbd.create_table('desc_')
dbd.wiki = dbd.create_table('wikified')
dbd.images = dbd.create_table('images')
dbd.wikisource_listpages = dbd.create_table('ws_listpages_20220321')
dbd.all_tables = dbd['all_tables']
