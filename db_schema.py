# import sqlite3
import json
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urlencode, urlunsplit
import os, io
from pathlib import Path
import dataset
# import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, BigInteger, SmallInteger, String, Text, Date, Numeric, Boolean, DateTime
from sqlalchemy import ForeignKey, ForeignKeyConstraint, MetaData, Table
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT
from sqlalchemy.schema import Index, CreateSchema
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utils.functions import database_exists, create_database

from db_connector import *

# naming_convention = {
#     "ix": "ix_%(column_0_label)s",
#     "uq": "uq_%(table_name)s_%(column_0_name)s",
#     "ck": "ck_%(table_name)s_%(constraint_name)s",
#     "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
#     "pk": "pk_%(table_name)s"
# }
naming_convention = {
    "ix": "%(table_name)s_%(column_0_label)s_index",
    "uq": "%(table_name)s_%(column_0_name)s_uindex",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "%(column_0_name)s_%(referred_table_name)s_fk",
    "pk": "%(table_name)s_pk"
}

db_name = 'lib_ru'
db_ = DB(db_name, use_os_env=True, use_orm=False)
session = db_.s

metadata = MetaData(bind=db_.engine, naming_convention=naming_convention)
Base = declarative_base(metadata=metadata)


class Authors(Base):
    __tablename__ = 'authors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(String(255), nullable=False, unique=True)
    name = Column(String(1000), nullable=False)
    family_parsed = Column(String(255))
    names_parsed = Column(String(255))
    live_time = Column(String(255))
    town = Column(Text)
    litarea = Column(String(255))
    desc = Column(Text)
    name_WS = Column(String(255))
    year_dead = Column(Integer)
    is_author = Column(Boolean, default=1, nullable=False)
    do_upload = Column(Boolean, default=1, nullable=False)
    uploaded = Column(Boolean, default=0, nullable=False)
    image_url = Column(Text)
    image_url_filename = Column(String(255))
    image_filename_wiki = Column(Text)
    filename = Column(Text)


class Titles(Base):
    __tablename__ = 'titles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pid_ws = Column(Integer, unique=True)
    slug = Column(String(255), nullable=False)
    author_id = Column(Integer, ForeignKey('authors.id', ondelete='CASCADE', onupdate='CASCADE'),
                       nullable=False, index=True)
    year = Column(Integer)
    size = Column(Integer)
    title_old = Column(Text)
    title = Column(String(500))
    desc = Column(Text)
    text_length = Column(Integer)
    text_url = Column(String(500), unique=True)
    oo = Column(Boolean, default=0)
    is_already_this_title_in_ws = Column(Boolean, default=0)
    do_upload = Column(Boolean, default=0, nullable=False, index=True)
    uploaded = Column(Boolean, default=0, nullable=False, index=True)
    do_update_as_named_proposed = Column(Boolean, default=0, nullable=False, index=True)
    updated_as_named_proposed = Column(Boolean, default=0, nullable=False, index=True)
    created_before_0326 = Column(Boolean, index=True)
    mybot_creater = Column(Boolean, index=True)
    img_renamed = Column(Boolean)
    title_ws_proposed = Column(String(255), unique=True)
    title_ws_as_uploaded = Column(String(255), unique=True)
    title_ws_as_uploaded_2 = Column(String(255), unique=True)
    title_ws_proposed_identical_level = Column(SmallInteger, index=True)
    renamed_manually = Column(Boolean)
    do_update_2 = Column(Boolean)
    is_lastedit_by_user = Column(Boolean)
    time_update = Column(DateTime)



Index('titles_author_id_slug_uindex', Titles.author_id, Titles.slug, unique=True)


class Wiki(Base):
    __tablename__ = 'wikified'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tid = Column(Integer, ForeignKey('titles.id', ondelete='CASCADE', onupdate='CASCADE'), unique=True, nullable=False)
    text = Column(LONGTEXT)
    desc = Column(Text)
    text_len = Column(Integer)
    is_new_text_differed = Column(Boolean)


class WikisourceListpages(Base):
    __tablename__ = 'ws_listpages_20220321'
    __comment__ = '23.01.2022'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pagename = Column(String(400), nullable=False, unique=True)


class TextsCategoriesNames(Base):
    __tablename__ = 'texts_categories_names'
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(String(255), nullable=False, unique=True)
    name = Column(String(500), unique=True)
    name_ws = Column(String(255), unique=True)


class TextsCategories(Base):
    __tablename__ = 'texts_categories'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tid = Column(Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'))
    category_id = Column(Integer,
                         ForeignKey('texts_categories_names.id', onupdate='CASCADE', ondelete='CASCADE'))


Index('ix_texts_categories_890bc0857c960d5b', TextsCategories.tid, TextsCategories.category_id, unique=True)


class Images(Base):
    __tablename__ = 'images'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tid = Column(Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False)
    urn = Column(String(500), nullable=False, unique=True)
    filename = Column(String(500), nullable=False)
    name_ws = Column(String(255), unique=True)
    downloaded = Column(Boolean, default=0, nullable=False)
    do_upload = Column(Boolean, default=1, nullable=False)
    uploaded = Column(Boolean, default=0, nullable=False)
    renamed_org_ext = Column(String(5))


Index('images_tid_name_ws_uindex', Images.tid, Images.name_ws, unique=True)


class Htmls(Base):
    __tablename__ = 'htmls'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tid = Column(Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False, unique=True)
    html = Column(LONGTEXT)
    wiki = Column(LONGTEXT)
    # wiki2 = Column(LONGTEXT)
    wiki_converted = Column(Boolean, default=0, nullable=False)
    wiki_differ_wiki2 = Column(Boolean, default=0, nullable=False)
    is_wikified = Column(Boolean)


class Desc(Base):
    __tablename__ = 'desc_'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tid = Column(Integer, ForeignKey('titles.id', onupdate='CASCADE', ondelete='CASCADE'), nullable=False, unique=True)
    author = Column(Text)
    translator = Column(Text)
    year = Column(Text)
    desc = Column(Text)
    author_tag = Column(Text)
    year_tag = Column(Text)
    annotation_tag = Column(Text)


class AuthorsCategories(Base):
    __tablename__ = 'authors_categories'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name_site = Column(String(500), nullable=False, unique=True)
    name_ws = Column(String(500))
    text_cat_by_author = Column(String(500))
    text_lang_by_author = Column(String(100))


class WSlistpages_uploaded(Base):
    __tablename__ = 'ws_listpages_uploaded'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pagename = Column(String(400), nullable=False, unique=True)


class WSpages_w_images(Base):
    __tablename__ = 'ws_pages_w_images'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pagename = Column(String(400), nullable=False, unique=True)


class WSpages_w_images_errors(Base):
    __tablename__ = 'ws_pages_w_images_errors'
    __comment__ = '-intersect -cat:"Страницы с неработающими файловыми ссылками" -cat:"Импорт/lib.ru"'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pagename = Column(String(400), nullable=False, unique=True)

class WSpages_w_img_err(Base):
    __tablename__ = 'ws_pages_w_img_err'
    __comment__ = 'with /img/'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pagename = Column(String(400), nullable=False, unique=True)


# all_tables = Table(
#     'all_tables', metadata,
#     id = Column( Integer, primary_key=True, autoincrement=True),
#     name_site = Column( String(500), nullable=False, unique=True),
#     name_ws = Column( String(500)),
#     text_cat_by_author = Column( String(500)),
#     text_lang_by_author = Column( String(100)),
# )

from sqlalchemy_utils import create_view
from sqlalchemy import select, func

# view
a = Authors
t = Titles
h = Htmls
d = Desc
w = Wiki
ac = AuthorsCategories
view_stmt = db_.s.query(
    t.slug.label('slug_text'),
    t.text_url,
    t.id.label('tid'),
    t.pid_ws,
    t.year,
    t.size,
    w.text_len,
    t.title,
    t.title_old,
    t.desc.label('text_desc_raw'),
    d.desc.label('text_desc'),
    w.desc.label('text_desc_wikified'),
    t.oo,
    t.is_already_this_title_in_ws,
    a.do_upload.label('do_upload_author'),
    t.do_upload,
    t.do_update_2,
    t.uploaded.label('uploaded_text'),
    t.do_update_as_named_proposed,
    t.updated_as_named_proposed,
    t.title_ws_proposed,
    t.title_ws_as_uploaded,
    t.title_ws_as_uploaded_2,
    t.title_ws_proposed_identical_level,
    t.created_before_0326,
    t.mybot_creater,
    t.renamed_manually,
    t.is_lastedit_by_user,
    a.slug.label('slug_author'),
    a.id.label('author_id'),
    a.name,
    a.family_parsed,
    a.names_parsed,
    a.name_WS,
    a.live_time,
    a.town,
    a.litarea,
    d.translator,
    a.image_url_filename,
    a.image_filename_wiki,
    a.desc.label('author_desc'),
    a.is_author,
    a.uploaded.label('uploaded_author'),
    a.year_dead,
    h.html,
    h.wiki,
    w.text.label('wikified'),
    h.wiki_differ_wiki2,
    h.wiki_converted,
    d.tid.label('desc_tid'),
    d.author_tag,
    ac.name_ws.label('author_cat'),
    ac.text_lang_by_author.label('lang'),
    t.time_update,
    h.is_wikified,
    w.is_new_text_differed,
).join(Authors).join(Titles).join(Htmls).join(Desc).join(Wiki).join(AuthorsCategories) \
    .statement
all_tables = create_view('all_tables', view_stmt, metadata)


class AllTables(Base):
    __table__ = all_tables


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
wikisource_listpages = db.create_table('ws_listpages_20220321')
all_tables = db['all_tables']
