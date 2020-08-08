# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from navernews.db_auth import connection_db
from navernews.db_mongo import connection_mongodb
import pymysql, pymongo
from scrapy.exporters import JsonItemExporter, CsvItemExporter


class NavernewsPipeline:
    def process_item(self, item, spider):
        return item


class NewsPipeline(object):
    def __init__(self):
        self.db_conn = connection_db()

    def process_item(self, item, spider):
        self.db_conn = connection_db()

        if not self.db_conn.open:
            print("데이터베이스 연결이 닫혀있습니다. 재연결 합니다.")
            self.db_conn = connection_db()

        try:
            with self.db_conn.cursor(pymysql.cursors.DictCursor) as cursor:
                sql = f"INSERT INTO news.`{item['news_media_code']}` (" \
                      f"news_id, news_title, news_content, " \
                      f"news_author, news_date, news_category, " \
                      f"news_original_url, news_site, news_naver_url) " \
                      f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

                try:
                    cursor.execute(sql, (item['news_id'], item['news_title'], item['news_content'], item['news_author'],
                                         item['news_date'], item['news_category'], item['news_original_url'],
                                         item['news_site'], item['news_naver_url']))
                    self.db_conn.commit()
                    print(f'* [기사제목] : {item["news_title"]}\n'
                          f'* [언 론 사] : {item["news_site"]}\n'
                          f'* [카테고리] : {item["news_category"]}\n'
                          f'* [발행날짜] : {item["news_date"]}\n'
                          f'* [네이버 URL] : {item["news_naver_url"]}\n'
                          f'* [원  본 URL] : {item["news_original_url"]}\n')
                    # print(f'* [Content] : {item["news_content"]}')
                except pymysql.err.InternalError:
                    pass
                except pymysql.err.IntegrityError:
                    pass
        finally:
            self.db_conn.close()

        return item


class MongodbPipeline(object):
    def __init__(self):
        pass

    def process_item(self, item, spider):
        conn = connection_mongodb()
        db = conn['news'][item['news_media_code']]
        db.create_index([('news_id', pymongo.ASCENDING)], unique=True)

        try:
            db.insert_one(item)
            print(f'* [기사제목] : {item["news_title"]}\n'
                  f'* [언 론 사] : {item["news_site"]}\n'
                  f'* [카테고리] : {item["news_category"]}\n'
                  f'* [발행날짜] : {item["news_date"]}\n'
                  f'* [네이버 URL] : {item["news_naver_url"]}\n'
                  f'* [원  본 URL] : {item["news_original_url"]}\n')
        except pymongo.errors.DuplicateKeyError:
            print("이미 저장된 기사입니다.")

        return item