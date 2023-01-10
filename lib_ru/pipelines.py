# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from urllib.parse import urlparse, parse_qs, parse_qsl, unquote, quote, urlsplit, urlunsplit
from pathlib import Path
import threading, queue
import scrapy
from scrapy.pipelines.images import ImagesPipeline

import db.schema as db
from lib_ru.items import *

lock = threading.RLock()

class LibRuPipeline(object):
    cache_works = {}
    cache_text_categories = {}

    def process_item(self, item, spider):

        if isinstance(item, Text):
            db.htmls.upsert(item, ['tid'])

        elif isinstance(item, AuthorItem):
            with lock:
                works = item['works']
                del item['works']
                db.authors.upsert(item, ['slug'])
                a = db.authors.find_one(slug=item['slug'])
                for w in works:
                    w.update({'author_id': a['id']})
                    db.titles.upsert(w, ['author_id', 'slug'])

        elif isinstance(item, AuthorAboutItem):
            db.authors.upsert(item, ['slug'])

        elif isinstance(item, Image):
            db.authors.update(item, ['id'])

        elif isinstance(item, ImageText):
            # db.authors.update(item, ['id'])
            pass

        return item


class TextsImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        if isinstance(item, ImageText):
            yield scrapy.Request(item['image_url'])

    def item_completed(self, results, item, info):
        for ok, x in results:
            if ok:
                db.images.update({'id': item['id'], 'downloaded': True}, ['id'])
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        return  item['filename']


class MyImagesPipeline(ImagesPipeline):

    # def get_media_requests(self, item, info):
    #     # for image_url in item['image_urls']:
    #     #     yield scrapy.Request(image_url)
    #     image_url = item['image_urls']
    #     yield scrapy.Request(image_url)

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield scrapy.Request(image_url)

    def item_completed(self, results, item, info):
        # image_paths = [x['path'] for ok, x in results if ok]
        # if not image_paths:
        #     raise DropItem("Item contains no images")
        # item['imagenext'] = image_paths[0]
        # item['image_paths'] = image_paths

        # if isinstance(item, dict) or self.images_result_field in item.fields:
        # item[self.images_result_field] = [x for ok, x in results if ok]
        # for ok, result in results:
        #     # item['imagenext'] = result['path'].replace('full/', '')
        #     # item['imagenext_url'] = result['url']
        #     item['image_url'] = result['image_urls'][0]
        #     # item={result['image_urls']:
        #     break

        # if 'imagenext' in item:
        #     item.pop('image_urls')
        #     info.spider.crawler.db.add_merge_processed(item)

        for ok, x in results:
            if ok:
                db.images.update({'id': item['id'], 'downloaded': True}, ['id'])

        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        return self.store.basedir + item['filename']
        # image_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        # return f'full/{image_guid}.jpg'
    #
    # def file_path(self, request, response=None, info=None, *, item=None):
    #     # i['id'] = db_row['id']
    #     # i['image_filename_wiki'] = db_row['name'] + ' libru' + Path(db_row['image_url_filename']).suffix
    #     # i['author_name'] = db_row['name']
    #     # i['slug'] = db_row['slug']
    #     # i['image_url_filename'] =  db_row['image_url_filename']
    #     path = Path(urlparse(request.url).path)
    #     slug = str(path.parent)
    #     filename_new = slug.split('/')[-1] + path.suffix
    #     return self.store.basedir + '/' + filename_new
