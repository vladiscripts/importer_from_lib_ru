#!/usr/bin/env python3
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from typing import List, Union
from urllib.parse import urlparse, urljoin
# from vladi_helpers import vladi_helpers
# from vladi_helpers.vladi_helpers import url_params_str_to_dict, url_params_str_to_list
# from vladi_helpers.file_helpers import json_save_to_file, json_load_from_file, file_savetext, file_readtext
# import parsel
# from pathlib import Path
# import datetime
# import json
# just for help in debug and showing results.
# needed to use splash rendering for jscript
# from scrapy_splash import SplashRequest, SplashFormRequest
# from w3lib.html import remove_tags
from pathlib import Path
# from mapping_fields import mapper, d, gender_id_map

import db_schema as db
from ..items import Image as Item


def make_url(slug, filename=None):
    host = 'http://az.lib.ru'
    url = f'{host}{slug}'
    if filename:
        url = f'{url}/{filename}'
    return url


class ScrapySpider(CrawlSpider):
    name = "get_images"

    # custom_settings = {"IMAGES_STORE": 'images_female'}

    def start_requests(self):
        # slugs = self.crawler.db.get_slugs_of_category(only_empty=False)
        # with open(self.input_csv, encoding='utf-8') as f:
        #     reader = csv.reader(f)
        #     self.slugs = set(row[0] for row in reader if row[0])
        #     self.slugs = self.crawler.db.check_processed(self.slugs)
        yield scrapy.Request('http://az.lib.ru', callback=self.parse_item)
        # for r in db.authors.find(image_url_filename={'!=': None}):
        #     url = make_url(r['slug'], r['image_url_filename'])
        #     yield scrapy.Request(url, callback=self.parse_item, cb_kwargs={'db_row': r})

    def _parse_item(self, response):
        # if ws_a := db.wikisource_listpages.find(pagename=r['name']):
        i = Item()
        i['image_urls'] = [image]
        i['id'] = db_row['id']
        # i['image_filename_wiki'] = db_row['name'] + ' libru' + Path(db_row['image_url_filename']).suffix
        # i['author_name'] = db_row['name']
        # i['slug'] = db_row['slug']
        # i['image_url_filename'] =  db_row['image_url_filename']
        i['filename'] = db_row['slug'].split('/')[-1] + Path(db_row['image_url_filename']).suffix
        yield i

    def parse_item(self, response):
        i = Item()
        i['image_urls'] = [make_url(r['slug'], r['image_url_filename'])
                           for r in db.authors.find(image_url_filename={'!=': None})]
        yield i
