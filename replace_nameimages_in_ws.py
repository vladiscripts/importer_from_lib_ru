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
        db.Images.tid == r.cid,
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
        replace_on_page(r.cid, title, replaces_pairs)


def replace_img_names_in_db():
    stmt = db.db_.s.query(db.Titles, db.Images, db.Htmls, db.Wiki).join(db.Htmls).join(db.Wiki).join(db.Images).filter(
        db.Titles.do_upload == 1,
        # db.Htmls.wiki_differ_wiki2 == 1,
        db.Titles.id == 89713,
        # db.Titles.title == 'Маленький Мук',
        db.Htmls.wiki.not_like(':' + db.Images.name_ws + '|'),
        db.Wiki.text.not_like(':' + db.Images.name_ws + '|'),
    )  # .order_by(db.Titles.id.desc())
    res = stmt.all()
    # l = len(res)
    for r in res:
        i = r.Images
        iname= str(i.name_ws)
        p = Path(i.name_ws)
        ss = [
            f":{re.sub('(?!---).jpg', '----.jpg', iname)}|",
            f":{iname.replace('.jpg', '---.jpg')}|",
            f":{iname.replace('----.jpg', '.jpg')}|",
            f":{iname.replace('---.jpg', '.jpg')}|",
            ]
        for s in ss:
            if not f':{i.name_ws}|' in r.Htmls.wiki:
                if s in r.Htmls.wiki:
                    r.Htmls.wiki.replace(s, i.name_ws)

            if not f':{i.name_ws}|' in r.Wiki.text:
                if s in r.Wiki.text:
                    r.Wiki.text.replace(s, i.name_ws)

            # r.Htmls.wiki = r.Htmls.wiki.replace(
            #     f':{p.stem.replace()}.jpg|',
            #     f':{r.Images.name_ws}|',
            # )

            # replaces_list, replaces_pairs2, replaces_pairs = get_replaces(r)
            # title = r.title_ws_as_uploaded or r.title_ws_proposed
            # # replace_images_of_page(title, replaces_list)
            # replace_on_page(r.id, title, replaces_pairs)

            print()



if __name__ == '__main__':
    # run()
    replace_img_names_in_db()
