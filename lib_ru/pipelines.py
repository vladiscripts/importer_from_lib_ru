# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.images import ImagesPipeline
from urllib.parse import urlparse, parse_qs, parse_qsl, unquote, quote, urlsplit,urlunsplit
from pathlib import Path

import db
from lib_ru.items import *


class LibRuPipeline(object):
    cache_works = {}
    cache_text_categories = {}

    def process_item(self, item, spider):

        match item:
            case Text():
                db.htmls.upsert(item, ['tid'])

            case AuthorItem():
                with db.db_lock:
                    a = db.authors.find_one(slug=item['slug'])

                    if not a:
                        works = item['works']
                        del item['works']
                        db.authors.upsert(item, ['slug'], ensure=True)

                        a = db.authors.find_one(slug=item['slug'])
                        for w in works:  w.update({'author_id': a['id']})
                        db.titles.insert_many(works, ensure=True)

            case AuthorAboutItem():
                db.authors.upsert(item, ['slug'], ensure=True)

            case Image():
                db.authors.update(item, ['id'], ensure=True)

            # case WorksItem():
            #     with db.db_lock:
            #         # for parse_about_page()
            #         item['slug'] = item['author_slug']
            #         del item['author_slug']
            #         db.authors.upsert(item, ['slug'], ensure=True)

        return item


class MyImagesPipeline(ImagesPipeline):

    # def get_media_requests(self, item, info):
    #     # for image_url in item['image_urls']:
    #     #     yield scrapy.Request(image_url)
    #     image_url = item['image_urls']
    #     yield scrapy.Request(image_url)

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

        return item

    # def file_path(self, request, response=None, info=None, *, item=None):
    #     return self.store.basedir + item['filename']
    #     # image_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
    #     # return f'full/{image_guid}.jpg'
    #
    def file_path(self, request, response=None, info=None, *, item=None):
        # i['id'] = db_row['id']
        # i['image_filename_wiki'] = db_row['name'] + ' libru' + Path(db_row['image_url_filename']).suffix
        # i['author_name'] = db_row['name']
        # i['slug'] = db_row['slug']
        # i['image_url_filename'] =  db_row['image_url_filename']
        path =Path( urlparse(request.url).path)
        slug = str(path.parent)
        filename_new = slug.split('/')[-1] + path.suffix
        return self.store.basedir +'/'+ filename_new
