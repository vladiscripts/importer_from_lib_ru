#!/usr/bin/env python3
import os
import shlex, subprocess
import requests
from urllib.parse import urlparse, parse_qs, parse_qsl, unquote, quote, urlsplit, urlunsplit
from sqlalchemy.orm import aliased

import db_schema as db

path_to_images = '/home/vladislav/workspace/4wiki/lib_ru/images_texts/'


def run(filename, desc):
    print(filename)
    # command = 'python /home/vladislav/usr/pwb/core/pwb.py upload -family:wikisource -lang:ru -keep -recursive -noverify -async -abortonwarn ' \
    #           f'-summary:"+" "{path_to_images}{filename}" "{desc}"'
    # '-descfile:"/home/vladislav/workspace/4wiki/lib_ru/desc_ws_upload_for_text_images.wiki" ' \
    cmd_list = ['python', '/home/vladislav/usr/pwb/core/pwb.py', 'upload', '-family:wikisource', '-lang:ru',
                '-keep', '-recursive', '-noverify', '-async', 
                '-ignorewarn', '-always',
                #  '-abortonwarn', 
                '-summary:"+"',
                f'{path_to_images}{filename}',
                f'{desc}'
                ]  # shlex.split(command)
    j = ' '.join(cmd_list)
    process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
    output = process.communicate()
    # stdout, stderr = output
    # print(stdout)
    # print(stderr)
    # code = process.wait()
    # assert code == 0, f'posting subprocess exited with status {code}'
    return True


def make_desc(db_row):
    title_ws = db_row.Titles.title_ws_as_uploaded or db_row.Titles.title_ws_proposed
    description = f"Иллюстрация к произведению «[[s:ru:{title_ws}|{db_row.Titles.title}]]»."
    source = db_row.Titles.text_url
    date = f'{db_row.Titles.year}'
    desc = "\n=={{int:filedesc}}==\n{{Information" \
           f"\n|description= {description}" \
           f"\n|date= {date}""" \
           f"\n|source= {source}" \
           "\n|author= " \
           "\n|permission=\n|other versions=" \
           "\n}}\n\n{{PD-old-70}}\n"
    return desc


class H:
    s = requests.Session()
    # url_args = '/w/api.php?action=query&format=json&prop=pageprops&utf8=1&titles='
    url_args = '/w/api.php?action=query&format=json&prop=pageprops&utf8=1&titles='

    def is_page_exists(self, title):
        for baseurl in ['https://commons.wikimedia.org', 'https://ru.wikisource.org']:
            try:
                r = self.s.get(baseurl + self.url_args + quote('File:' + title))
            except requests.exceptions.ConnectionError as e:
                self.s = requests.Session()
            r = self.s.get(baseurl + self.url_args + quote('File:' + title))
            if not '-1' in r.json()['query']['pages']:
                return True


h = H()
offset = 0
limit = 300
# select * from images as i1 join images as i2 on i1.name_ws = i2.name_ws where i1.urn != i2.urn
i1 = aliased(db.Images)
i2 = aliased(db.Images)
stmt_images_doubles = db.db_.s.query(i1.name_ws).join(i2, i1.name_ws == i2.name_ws).filter(i1.tid != i2.tid).group_by(
    i1.filename)
while True:
    stmt = db.db_.s.query(db.Images, db.Titles, db.Authors) \
        .select_from(db.Images).join(db.Titles).join(db.Authors).join(db.Htmls).filter(
        db.Titles.uploaded == True,
        # db.Titles.year <= 1917,
        db.Htmls.wiki_differ_wiki2 == 1,
        # db.Images.name_ws.like('text_1772_voina_s_polskimi_konfedertami_s07.jpg'),
        db.Images.downloaded == True, db.Images.do_upload == True, db.Images.uploaded == False,
        # db.Images.name_ws.not_in(stmt_images_doubles),
        db.Images.name_ws.notlike('%png'), db.Images.name_ws.notlike('%gif')) \
        .limit(limit).offset(offset)
    res = stmt.all()
    for r in res:
        if not h.is_page_exists(r.Images.name_ws):
            desc = make_desc(r)
            run(r.Images.name_ws, desc)
            u = db.images.update({db.Images.uploaded.name: True, 'id': r.Images.id}, ['id'])
    if len(res) < limit:
        break
    offset += limit
