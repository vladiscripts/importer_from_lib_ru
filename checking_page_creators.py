#!/usr/bin/env python3
import re
import time
import dateutil.parser
from pathlib import Path
from typing import Optional, Union, Sequence, List, Tuple
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import pywikibot as pwb

import db_schema as db


def process_page(title, tid):
    print(title)
    page = pwb.Page(SITE, title)
    if page.exists():
        # is_libru_in_summary_created = 'lib.ru' in page.oldest_revision['comment'].lower()
        db.titles.update(dict(
            id=r.id,
            created_before_0326=page.oldest_revision['timestamp'] < date_of_start_bot_uploading,
            mybot_creater=page.oldest_revision['user'] == 'TextworkerBot'
        ), ['id'])
    else:
        print('not exists:', title)


if __name__ == '__main__':

    SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')
    date_of_start_bot_uploading = dateutil.parser.parse('2022-03-26')

    ta = db.all_tables
    cola = ta.table.c

    offset = 0
    limit = 100
    while True:
        stmt = db.db_.s.query(db.Titles).filter(
            db.Titles.uploaded == 1,
            # db.Titles.title_ws_proposed.isnot(None),
            db.Titles.title_ws_as_uploaded.isnot(None),
            db.Titles.created_before_0326.is_(None),
        ).offset(offset).limit(limit)
        res = stmt.all()
        for r in res:
            title = r.title_ws_as_uploaded
            process_page(title, r.id)
        if len(res) < limit:
            break
        offset += limit
