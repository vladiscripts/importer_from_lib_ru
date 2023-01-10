#!/usr/bin/env python3
import time
from datetime import datetime
from math import floor
import re
import os
import threading, queue
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import concurrent.futures
from multiprocessing import cpu_count, Queue  # Вернет количество ядер процессора
import multiprocessing
import socket
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.sql import or_
from pathlib import Path
from typing import Optional, Union, Tuple, List
from pydantic import BaseModel, ValidationError, Field, validator, root_validator, Extra
from pydantic.dataclasses import dataclass
import html as html_
import mwparserfromhell as mwp
import pypandoc
from bs4 import BeautifulSoup, Tag

import db_schema as db


# from get_parsed_html import get_html, get_content_from_html, get_content_from_html_soup
# from html2wiki import LibRu
# from parser_html_to_wiki import *

class Image(BaseModel):
    tid: int
    urn: str
    filename: str
    name_ws: str


class H(BaseModel):
    tid: int
    html: str
    soup: Optional[BeautifulSoup]
    wikicode: mwp.wikicode.Wikicode = None
    wiki: Optional[str] = None
    wiki2: Optional[str] = None
    wiki_new: Optional[str] = None
    images: List[Image] = []

    class Config:
        # validate_assignment = True
        extra = Extra.ignore
        arbitrary_types_allowed = True


def process_images(h):
    """ to simple names of images """
    for f in h.wikicode.filter_wikilinks(matches=lambda x: x.title.lower().startswith('file')):
        link = re.sub(r'^[Ff]ile:', '', str(f.title))
        if link == 'StrangeNoGraphicData':
            continue
        # if not link.startswith('/img/'):  # удалить ссылки на картинки которые не начинаются на '/img/
        if link.startswith('http://'):  # удалить ссылки на картинки которые не начинаются на '/img/
            del (f)
            continue
        p = Path(link)
        replaces = {'.png': '---.jpg', '.gif': '----.jpg'}
        if p.suffix in replaces:
            p = p.with_name(p.name.replace(p.suffix, replaces[p.suffix]))
        try:
            name_ws = f'{p.parts[-3]}_{p.parts[-2]}_{p.name}'
        except IndexError as e:
            continue
        f.title = 'File:' + name_ws
        img = Image(tid=h.tid, urn=link, filename=p.name, name_ws=name_ws)
        h.images.append(img)
    return h


def feeder(offset=0) -> Optional[List[dict]]:
    ta = db.AllTables

    # stmt = db.db_.s.query(db.Htmls.tid, db.Htmls.html, db.Htmls.wiki) \
    #     .select_from(db.Titles).join(db.Htmls).join(db.Wiki) \
    #     .outerjoin(db.WSpages_w_img_err, db.Titles.title_ws_as_uploaded_2 == db.WSpages_w_img_err.pagename)
    # # .outerjoin(db.Images)
    # stmt = stmt.filter(db.WSpages_w_img_err.pagename.isnot(None))

    # stmt = db.db_.s.query(db.WSpages_w_img_err.pagename).select_from(db.WSpages_w_img_err).outerjoin(db.Titles).outerjoin(db.Images).filter(
    stmt = sa.select(db.Titles.id.label('tid'), db.WSpages_w_images_errors.pagename, db.Htmls.html, db.Htmls.wiki) \
        .select_from(db.WSpages_w_images_errors).outerjoin(db.Titles).outerjoin(db.Htmls).outerjoin(db.Images)
    stmt = stmt.where(
        # db.AllTables.tid > 98000,
        # ta.tid == 90475,
        db.Titles.id == 101104,
        # db.Titles.id.in_([89713, 94626]),
        # db.Titles.title == 'Маленький Мук',
        # db.Titles.title_ws_as_uploaded == 'Цезарь Каскабель (Верн)',
        # or_(ta.time_update.isnot(None), ta.wiki_converted == 0),
        # ta.uploaded_text == 0,
        # ta.wiki_converted == 0,
        # ta.do_upload == 1,
        db.Images.id.is_(None),
        db.Htmls.wiki.isnot(None),
        # ta.wikified.like('%StrangeNoGraphicData%'),
        # or_(
        #     db.Htmls.wiki.not_like('%:' + db.Images.name_ws + '|%'),
        #     db.Wiki.text.not_like('%:' + db.Images.name_ws + '|%'),
        # ),
        # html={'like': '%%'},  # wiki2={'like': '%[[File:%'},
    )  # .limit(chunk_size).offset(offset)  # .limit(10)  # .order_by(db.Titles.id.desc())
    res = db.db_.s.execute(stmt).fetchall()
    # res = stmt.all()
    # res = [{c.name: getattr(r, c.name) for c in r.__table__.columns} for r in res]
    return res


def db_save(h) -> None:
    rows = [img.dict() for img in h.images]
    with db.dbd as tx1:
        tx1['images'].delete(tid=h.tid)
        for row in rows:
            tx1['images'].insert_ignore(row, ['urn'])


def work_row(r):
    h = H.parse_obj(r)
    h.wikicode = mwp.parse(h.wiki)
    h.html = None
    h.wiki = None
    h = process_images(h)
    if h.images:
        print('converted, to db', h.tid)
        db_save(h)
    else:
        print('no images', h.tid)


def main():
    rows = feeder()
    for r in rows:
        work_row(r)


if __name__ == '__main__':
    main()
