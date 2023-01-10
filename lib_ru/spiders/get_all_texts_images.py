from scrapy import Request
from scrapy.spiders import CrawlSpider

import db.schema as db
from lib_ru.items import ImageText


class TextImagesSpider(CrawlSpider):
    name = "get_all_texts_images"
    custom_settings = {'IMAGES_STORE': 'images_texts', 'HTTPCACHE_ENABLED': False}

    def start_requests(self):
        query = db.db_.s.query(db.Images).filter(db.Images.downloaded == 0)  # limit(5)
        for db_row in query.all():
            yield Request('http://localhost', callback=self.parse_item, dont_filter=True, cb_kwargs={'db_row': db_row})

    def parse_item(self, r, db_row):
        i = ImageText()
        i['image_url'] = 'http://az.lib.ru' + db_row.urn
        i['id'] = db_row.cid
        i['tid'] = db_row.tid
        i['filename'] = db_row.name_ws
        yield i
