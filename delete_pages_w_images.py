#!/usr/bin/env python3
import os
import shlex, subprocess
import requests
from urllib.parse import urlparse, parse_qs, parse_qsl, unquote, quote, urlsplit, urlunsplit
import argparse

import db_schema as db


def get_cli_args():
    aparser = argparse.ArgumentParser()
    aparser.add_argument('-t', '--title', dest='title', help='Title page name in Wikisource')
    aparser.add_argument('-a', '--author', dest='author', help='Author page name in Wikisource')
    args = aparser.parse_args()
    return args


delete_base_args = ['python3', '/home/vladislav/usr/pwb/core/pwb.py', 'delete', '-family:wikisource', '-lang:ru',
                    '-user:Vladis13', '-always']


def run_command(arg: list):
    process = subprocess.Popen(arg, stdout=subprocess.PIPE)
    output = process.communicate()
    return output


def delete_images_of_page(pagename, tid):
    '-summary:"[[Викитека:Проект:Импорт текстов/Lib.ru]]: нарушение АП"'
    cmd_list = delete_base_args + [f'-imagesused:{pagename}', '-grep:az.lib.ru',  # '-titleregex:"Text "',
                                   '-summary:автоимпорт с az.lib.ru: нарушение АП или неиспользуемая иллюстрация произведения']
    print('images of', pagename)
    run_command(cmd_list)
    # print(stdout, stderr)


def delete_page(pagename, tid):
    cmd_list = delete_base_args + [f'-page:{pagename}', '-summary:автоимпорт с az.lib.ru: нарушение АП']
    print(pagename)
    run_command(cmd_list)
    # print(stdout, stderr=output)


def delete_page_w_images(pagename, tid=None):
    if not tid:
        r = db.titles.find_one(title_ws_as_uploaded=pagename)
        if not r:
            print('page was not found')
            return
        tid = r['id']
    delete_images_of_page(pagename, tid)
    db.images.update({'tid': tid, 'do_upload': False, 'uploaded': False}, ['tid'])

    delete_page(pagename, tid)
    db.titles.update({'id': tid, 'do_upload': False, 'uploaded': False, 'do_update_as_named_proposed': False,
                      'updated_as_named_proposed': False}, ['id'])


def delete_pages_by_author(author):
    r = db.authors.find_one(name_WS=author)
    if not r:
        print('author page was not found')
        return
    db.authors.update({'id': r['id'], 'do_upload': False}, ['id'])
    res = db.titles.find(author_id=r['id'])
    for r in res:
        delete_page_w_images(r['title_ws_as_uploaded'], r['id'])


if __name__ == '__main__':
    args = get_cli_args()
    if args.title:
        delete_page_w_images(pagename=args.title)
    elif args.author:
        delete_pages_by_author(author=args.author)
