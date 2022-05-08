#!/usr/bin/env python3
import os
from pathlib import Path
import shlex, subprocess
import requests
from urllib.parse import urlparse, parse_qs, parse_qsl, unquote, quote, urlsplit, urlunsplit
import sqlalchemy as sa
from sqlalchemy.orm import aliased

import db_schema as db
from db_schema import Images, Titles, Htmls, Wiki

path_to_images = '/home/vladislav/workspace/4wiki/lib_ru/images_texts/'
os.chdir(path_to_images)


def run(filename, desc):
    print(filename)
    # command = 'python /home/vladislav/usr/pwb/core/pwb.py upload -family:wikisource -lang:ru -keep -recursive -noverify -async -abortonwarn ' \
    #           f'-summary:"+" "{path_to_images}{filename}" "{desc}"'
    # '-descfile:"/home/vladislav/workspace/4wiki/lib_ru/desc_ws_upload_for_text_images.wiki" ' \
    cmd_list = ['python3', '/home/vladislav/usr/pwb/core/pwb.py', 'upload', '-family:wikisource', '-lang:ru',
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


def rename_img_files(filename):
    """ На lib.ru файлы PNG на самом деле являются файлами JPG. Wiki определяет эту ошибку и не загружает такие файлы.
    Поэтому их надо переименовывать.
    """
    path_to_images = '/home/vladislav/workspace/4wiki/lib_ru/images_texts/'
    os.chdir(path_to_images)
    rows = Images.find(png2jpg_renamed=1)
    for r in rows:
        n = r['name_ws']
        print(n)
        pairs = [
            (n.replace('---.jpg', '.png'), n.replace('.png', '---.jpg')),
            (n.replace('---.jpg', '.jpg'), n.replace('.jpg', '---.jpg')),
            (n.replace('----.jpg', '.gif'), n.replace('.gif', '----.jpg')),
        ]
        if not Path(n).exists():
            for check, new_filename in pairs:
                check_f = Path(check)
                if check_f.exists():
                    check_f.rename(new_filename)
                    print('file renamed', new_filename)
                    continue


class H:
    s = requests.Session()
    # url_args = '/w/api.php?action=query&format=json&prop=pageprops&utf8=1&titles='
    url_args = '/w/api.php?action=query&format=json&prop=pageprops&utf8=1&titles='

    def is_page_exists(self, title):
        url_args = self.url_args + quote('File:' + title)
        for baseurl in ['https://commons.wikimedia.org', 'https://ru.wikisource.org']:
            try:
                r = self.s.get(baseurl + url_args)
            except requests.exceptions.ConnectionError as e:
                self.s = requests.Session()
            r = self.s.get(baseurl + url_args)
            if '-1' not in r.json()['query']['pages']:
                return True


h = H()
offset = 0
limit = 300
# select * from images as i1 join images as i2 on i1.name_ws = i2.name_ws where i1.urn != i2.urn
# i1 = aliased(Images)
# i2 = aliased(Images)
# stmt_images_doubles = db.session.query(i1.name_ws).join(i2, i1.name_ws == i2.name_ws).filter(i1.tid != i2.tid).group_by(i1.filename)
while True:
    stmt = db.session.query(Images, Titles, Htmls) \
        .select_from(Images).join(Titles).join(Htmls).filter(
        Titles.uploaded == True,
        # Titles.year <= 1917,
        # Htmls.wiki_differ_wiki2 == 1,
        # Images.name_ws.like('text_1772_voina_s_polskimi_konfedertami_s07.jpg'),
        # Images.downloaded == True,
        Images.do_upload == True, Images.uploaded == False,
        # Images.name_ws.not_in(stmt_images_doubles),
        # Images.name_ws.notlike('%png'),
        # Images.name_ws.notlike('%gif'),
    ).limit(limit).offset(offset)
    res = stmt.all()
    for r in res:
        iid = r.Images.id
        text_url = r.Titles.text_url
        text_pagename = r.Titles.title_ws_as_uploaded_2
        filename = r.Images.name_ws
        pagename = f'File:{r.Images.name_ws}'
        if not Path(filename).exists():
            continue
        if not h.is_page_exists(filename):
            print(f'uploading {filename}')
            desc = make_desc(r)
            # filename = rename_img_file(filename)
            run(filename, desc)
        if h.is_page_exists(filename):
            print(f'h.is_page_exists({filename})')
            # db.images.update({Images.uploaded.name: True, Images.downloaded.name: True, 'id': r.Images.id}, ['id'])
            db.session.query(Images).filter(Images.id == iid).update({Images.uploaded: True, Images.downloaded: True})
            # stmt = sa.update(Images).values({Images.uploaded: True, Images.downloaded:True}).where(Images.id == iid)
            # db.session.execute(stmt)
            db.session.commit()
    if len(res) < limit:
        break
    offset += limit
