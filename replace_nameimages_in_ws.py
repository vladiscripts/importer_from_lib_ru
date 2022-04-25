#!/usr/bin/env python3
import os
import shlex, subprocess
from pathlib import Path
import requests
from urllib.parse import urlparse, parse_qs, parse_qsl, unquote, quote, urlsplit, urlunsplit
import argparse
import pywikibot as pwb

import db_schema as db

base_args = ['python3', '/home/vladislav/usr/pwb/core/pwb.py', 'replace', '-family:wikisource', '-lang:ru',
             '-always', '-summary:images renamed']


def run_command(arg: list):
    process = subprocess.Popen(arg, stdout=subprocess.PIPE)
    output = process.communicate()
    return output


def replace_images_of_page(pagename, repl_str):
    '-summary:"[[Викитека:Проект:Импорт текстов/Lib.ru]]: нарушение АП"'
    cmd_list = base_args + [f'-page:{pagename}'] + repl_str
    print(cmd_list)
    run_command(cmd_list)


SITE = pwb.Site('ru', 'wikisource', user='TextworkerBot')


def replace_on_page(tid, pagename, replaces_pairs):
    print(pagename)
    page = pwb.Page(SITE, pagename)
    if page.exists():
        text_new = page.text
        is_updated = False
        for old, new in replaces_pairs:
            if old in text_new or old.replace('_', ' ') in text_new:
                new = new.replace('_', ' ')
                text_new = text_new.replace(old, new)
                text_new = text_new.replace(old.replace('_', ' '), new)
            elif new in text_new or new.replace('_', ' ') in text_new:
                is_updated = True

        if page.text != text_new:
            print('changing:', pagename)
            page.text = text_new
            page.save(summary='images renamed')
            db.titles.update({'id': tid, 'img_renamed': True}, ['id'])
        elif is_updated:
            db.titles.update({'id': tid, 'img_renamed': True}, ['id'])


def get_replaces(r):
    replaces_pairs_raw = []

    stmt = db.db_.s.query(db.Images).filter(
        db.Images.tid == r.id,
    )
    res = stmt.all()
    for r in res:
        p = Path(r.urn)
        old_name_ws = f'{p.parts[-2]}_{p.name}'
        name_ws = f'{p.parts[-3]}_{p.parts[-2]}_{p.name}'
        replaces_pairs_raw.append([old_name_ws, name_ws])

    replaces_pairs2 = [(f'":{old}|" ":{new}|"') for old, new in replaces_pairs_raw]
    replaces_list = [f':{x}|' for pair in replaces_pairs_raw for x in pair]
    replaces_pairs = [(f':{old}|', f':{new}|') for old, new in replaces_pairs_raw]
    return replaces_list, replaces_pairs2, replaces_pairs


def run():
    stmt = db.db_.s.query(db.Titles).join(db.Htmls).join(db.Images).filter(
        db.Titles.uploaded == 1,
        db.Htmls.wiki_differ_wiki2 == 1,
        # db.Titles.id == 149520,
        # db.Titles.title == 'Маленький Мук',
    )  # .order_by(db.Titles.id.desc())
    res = stmt.all()
    for r in res:
        replaces_list, replaces_pairs2, replaces_pairs = get_replaces(r)
        title = r.title_ws_as_uploaded or r.title_ws_proposed
        # replace_images_of_page(title, replaces_list)
        replace_on_page(r.id, title, replaces_pairs)


if __name__ == '__main__':
    run()
