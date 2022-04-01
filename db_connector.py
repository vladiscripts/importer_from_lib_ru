# import sqlite3
import json
from urllib.parse import urlsplit, parse_qs, parse_qsl, unquote, quote, urlencode, urlunsplit
import os, io
from pathlib import Path
import dataset
# import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utils.functions import database_exists, create_database
from threading import RLock


class DB:
    def __init__(self, db_name: str, use_os_env: bool = True, use_orm=True):
        self.db_name = db_name
        engine_url = self.make_data_url(use_os_env)
        self.engine = create_engine(engine_url, echo=False)
        self.conn = self.engine.connect()
        if not database_exists(engine_url):
            create_database(engine_url)
        self.Session = sessionmaker(bind=self.engine)
        self.s = self.Session()

        if use_orm:
            Base.metadata.create_all(self.engine)

        self.connect = dataset.connect(str(self.engine.url), engine_kwargs=dict(echo=self.engine.echo))
        #   #pool_size=10, max_overflow=20
        # self.table_input = self.connect[table_input_name]
        # self.table_output = self.connect[table_output_name]

        # lock = RLock()

    def make_data_url(self, use_os_env: bool, driver_name: str = 'mysql+pymysql') -> str:
        """
        Create an engine string (schema + netloc), like "mysql+pymysql://USER:PASSWORD@HOST"
        :param use_os_env: Use OS envs 'DB_USER', 'DB_PASSWORD', 'DB_HOST', instead the `cfg.py` file
        """
        if use_os_env:
            import os
            try:
                user = os.environ['DB_USER']
                password = os.environ['DB_PASSWORD']
                host = os.environ['DB_HOST']
            except KeyError:
                raise RuntimeError("Set the 'DB_USER', 'DB_PASSWORD', 'DB_HOST' OS env variables")
        else:
            from cfg import user, password, host
        url = f'{driver_name}://{user}:{password}@{host}/{self.db_name}'
        return url

# db_ = DB(db_name)

# db = dataset.connect('sqlite:////home/vladislav/var/db/from_lib_ru.sqlite'), #  engine_kwargs={'connect_args': {'check_same_thread': False}})
