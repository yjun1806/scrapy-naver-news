import scrapy
from scrapy.spiders import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor
from allcrawler.items import NewsItem
from urllib.parse import urlparse, parse_qs
import re
from w3lib.html import remove_tags_with_content
import datetime
from allcrawler.db_auth import connection_db
from navernews.info import naver_news_code
from navernews.tools import clean_html
import pymysql
import csv


# Linkextractor를 사용하는 버전
class News(CrawlSpider):
    name = 'news'  # 스파이더 이름 지정, 호출시에 사용하는 용도이다. 각 스파이더를 구분지어주기도 함
    custom_settings = {  # 기본적인 세팅값을 설정한다. 이 부분에 설정하는 것은 스파이더마다 설정값을 다르게 한다는 의미이다.
        'ROBOTSTXT_OBEY': False,  # Robots.txt 파일의 권장사항을 지킬것인지 설정
        'LOG_ENABLED': True,  # 로그를 사용할 것인지
        'RETRY_TIMES': 3,  # 요청에 실패했을때 재요청을 몇번 시도할 것인지
        'DOWNLOAD_DELAY': 0.2,  # 다운로드시 딜레이는 얼마나 줄 것인지
        'DOWNLOAD_TIMEOUT': 10,  # 다운로드시 시간초과를 몇 초까지 기다릴 것인지
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # 다운로드 딜레이를 랜덤하게 주는 것
        'LOG_LEVEL': 'INFO',  # 로그 레벨 설정
        # 'LOG_FILE': 'LOGS/news.log',  # ERROR 로그를 어디에 저장할 것인지
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
        },
        'ITEM_PIPELINES': {  # Spider가 사용할 Pipeline 설정
            'navernews.pipelines.NewsPipeline': 300
        },
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,  # default: 16
        'CONCURRENT_REQUESTS_PER_IP': 16,  # default: 16
        'CONCURRENT_REQUESTS': 32,  # default: 16
    }

    def __init__(self, category=None, *a, **kw):  # Spider가 생성될때의 초기화
        print("*" * 100)
        print("뉴스 스파이더를 실행합니다.")
        news_category = category
        print(f"크롤링하는 언론사는 [{naver_news_code[news_category]}]({news_category}) 입니다.")
        self.allowed_domains = ['news.naver.com']
        self.start_urls = [
            f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec&listType=summary&oid={news_category}"]

        print("데이터베이스 연결")
        db_connect = connection_db()

        if db_connect.open:
            print("데이터베이스 연결 성공")
            try:
                with db_connect.cursor(pymysql.cursors.DictCursor) as cursor:
                    print("데이터베이스에 테이블이 있는지 확인합니다.")
                    sql = f"SELECT EXISTS(SELECT 1 FROM Information_schema.tables " \
                          "WHERE table_schema = 'news' AND TABLE_NAME = %s) AS flag"

                    try:
                        cursor.execute(sql, (news_category))
                        result = cursor.fetchall()

                        if result[0]['flag'] == 0:
                            print(f"[{naver_news_code[news_category]}]의 {news_category} 테이블이 없습니다. 생성합니다.")
                            sql = f'''
                                    CREATE TABLE if not exists news.`{news_category}` (
                                        `inx` INT(11) NOT NULL AUTO_INCREMENT,
                                        `news_id` VARCHAR(255) NOT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_title` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_content` LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_author` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_date` TIMESTAMP NULL DEFAULT NULL,
                                        `news_category` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_original_url` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_site` VARCHAR(30) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_naver_url` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        PRIMARY KEY (`inx`),
                                        UNIQUE INDEX `inx` (`inx`),
                                        UNIQUE INDEX `naver_news_id_uindex` (`news_id`)
                                    )
                                    COLLATE='utf8mb4_unicode_ci'
                                    ENGINE=InnoDB
                                    '''
                            cursor.execute(sql)

                        else:
                            print(f"[{naver_news_code[news_category]}]의 {news_category} 테이블이 이미 있습니다.")
                            print(f"데이터를 {news_category} 테이블에 저장합니다.")

                    except pymysql.err.InternalError:
                        pass
                    except pymysql.err.IntegrityError:
                        pass
            finally:


                self.rules = (Rule(LinkExtractor(allow=(f"oid={news_category}"),
                                                 deny=('news.naver.com/main/tool/print.nhn',
                                                       'media.naver.com/channel/promotion.nhn'),
                                                 restrict_xpaths=(
                                                     '//*[@id="main_content"]/div[@class="list_body newsflash_body"]/ul[@class="type06_headline"]',
                                                     '//*[@id="main_content"]/div[@class="list_body newsflash_body"]/ul[@class="type06"]',
                                                     '//*[@id="main_content"]/div[@class="paging"]',
                                                     '//*[@id="main_content"]/div[@class="pagenavi_day"]')),
                                   callback=self.parse_link, follow=True),)

            print("*" * 100)
        else:
            print("데이터베이스 연결 실패")
            print("프로그램 종료")
            exit(2)

        super().__init__(*a, **kw)

    def parse_link(self, response):

        if "list.nhn" in response.url:
            # 리스트 페이지를 크롤링 한경우, 아무 작업도 하지 않는다.
            # 어차피 LinkExtractor 가 링크는 추출하므로 따로 파싱작업을 할 필요가 없다.
            print(f"{'*' * 50} [ 기사목록 ] {'*' * 50}")
            print(f"[{response.url}] 에서 가져왔습니다.")

        else:
            article_type = response.xpath('/html/head/meta[@property="me:feed:serviceId"]/@content').extract_first()
            items = NewsItem()
            # 네이버 뉴스에서의 고유의 뉴스기사 ID값 - 일반, 스포츠, 연예 모두 동일
            urlparsing = urlparse(response.url)
            url_query = parse_qs(urlparsing.query)
            items['news_id'] = url_query['aid'][0]
            items['news_media_code'] = url_query['oid'][0]  # 뉴스 언론사 코드

            # 뉴스기사의 제목 - 일반, 스포츠, 연예 모두 동일
            items['news_title'] = response.xpath(
                '/html/head/meta[@property="og:title"]/@content').extract_first()

            # 뉴스기사의 네이버뉴스 URL - 일반, 소포츠, 연예 모두 동일
            items['news_naver_url'] = response.url

            if article_type is None:
                article_type = response.xpath('/html/head/meta[@name="twitter:site"]/@content').extract_first()

                # 뉴스기사를 발행한 언론사 이름 - 일반, 연예 동일
                items['news_site'] = response.xpath(
                    '/html/head/meta[@name="twitter:creator"]/@content').extract_first()

                if article_type == "네이버 TV연예":
                    print(f"{'*' * 50} [ 연예기사 ] {'*' * 50}")

                    # 뉴스기사의 카테고리
                    items['news_category'] = "TV연예"

                    # 뉴스기사 발행 시간
                    date = response.xpath('//*[@id="content"]/div[@class="end_ct"]/div/'
                                          'div[@class="article_info"]/span[@class="author"]/em/text()').extract_first()
                    try:
                        date = date.replace("오전", "AM")
                        date = date.replace("오후", "PM")
                        date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                        items['news_date'] = date_time_obj
                    except AttributeError:
                        items['news_date'] = date

                    # 뉴스기사의 언론사 URL
                    items['news_original_url'] = response.xpath('//*[@id="content"]/div[@class="end_ct"]/div/'
                                                                'div[@class="article_info"]/a/@href').extract_first()

                    # 뉴스기사의 내용
                    content_tmp = response.xpath('//*[@id="articeBody"]/text()').extract()
                    for line in content_tmp:  # 저작권 라인 제거
                        if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                            inx = content_tmp.index(line)
                            del content_tmp[inx]
                    items['news_content'] = " ".join((" ".join(content_tmp)).split())

                else:
                    print(f"{'*' * 50} [ 일반기사 ] {'*' * 50}")

                    # 뉴스기사의 카테고리
                    items['news_category'] = response.xpath(
                        '/html/head/meta[@property="me2:category2"]/@content').extract_first()

                    # 만약 카테고리가 속보인 경우 원본 기사에서 지정한 카테고리로 설정한다.
                    if items['news_category'] is None or items['news_category'] == "속보":
                        items['news_category'] = response.xpath(
                            '//*[@id="articleBody"]/div[@class="guide_categorization"]/a/em/text()').extract_first()

                    # 뉴스기사 발행 시간
                    date = response.xpath('//*[@id="main_content"]/div[@class="article_header"]'
                                          '/div[@class="article_info"]'
                                          '/div/span[@class="t11"][1]/text()').extract_first()
                    try:
                        date = date.replace("오전", "AM")
                        date = date.replace("오후", "PM")
                        date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                        items['news_date'] = date_time_obj
                    except AttributeError:
                        items['news_date'] = date

                    # 뉴스기사의 언론사 URL
                    items['news_original_url'] = response.xpath('//*[@id="main_content"]/div[@class="article_header"]'
                                                                '/div[@class="article_info"]/div/a[1]/@href').extract_first()

                    # 뉴스기사의 내용
                    content_tmp = response.xpath('//*[@id="articleBodyContents"]/text()').extract()
                    for line in content_tmp:  # 저작권 라인 제거
                        if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                            inx = content_tmp.index(line)
                            del content_tmp[inx]
                    items['news_content'] = " ".join((" ".join(content_tmp)).split())

            else:
                print(f"{'*' * 50} [ 스 포 츠 ] {'*' * 50}")

                # 뉴스기사를 발행한 언론사 이름
                site_tmp = response.xpath(
                    '/html/head/meta[@property="og:article:author"]/@content').extract_first()
                items['news_site'] = site_tmp.replace("네이버 스포츠 | ", "")

                # 뉴스기사의 카테고리
                items['news_category'] = "스포츠"

                # 뉴스기사 발행 시간
                date = response.xpath('//*[@id="content"]/div/div[@class="content"]/'
                                      'div/div[@class="news_headline"]/div[@class="info"]/span[1]/text()').extract_first()
                try:
                    date = date.replace("기사입력 ", "")
                    date = date.replace("오전", "AM")
                    date = date.replace("오후", "PM")
                    date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                    items['news_date'] = date_time_obj
                except AttributeError:
                    items['news_date'] = date

                # 뉴스기사의 언론사 원본 URL
                items['news_original_url'] = response.xpath('//*[@id="content"]/div/div[@class="content"]/'
                                                            'div/div[@class="news_headline"]/div[@class="info"]/a/@href').extract_first()

                # 뉴스기사의 내용
                content_tmp = response.xpath('//*[@id="newsEndContents"]/text()').extract()
                for line in content_tmp:  # 저작권 라인 제거
                    if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                        inx = content_tmp.index(line)
                        del content_tmp[inx]
                items['news_content'] = " ".join((" ".join(content_tmp)).split())

            # 뉴스기사를 작성한 기자
            items['news_author'] = ""

            yield items


class News_manual(scrapy.Spider):
    """
    뉴스기사 크롤링 스파이더, LinkExtractor를 사용하지 않는 버전
    정해진 기간의 뉴스 기사를 크롤링하며, 어떤 언론사를 크롤링할지는 입력받는다.
    -a category=(언론사 코드)
    -a start=(시작 날짜)
    -a end=(끝나는 날짜)

    이때 시작날짜보다 끝나는 날짜가 더 과거여야 한다.
    예를 들어 1/1 ~ 3/24일 기간의 뉴스를 크롤링할 경우
    start=20200324 end=20200101로 설정해야 한다.
    그 이유는 최신뉴스 -> 과거뉴스 순으로 크롤링하도록 구성했기 때문..
    """
    name = 'newsmanual'  # 스파이더 이름 지정, 호출시에 사용하는 용도이다. 각 스파이더를 구분지어주기도 함

    custom_settings = {  # 기본적인 세팅값을 설정한다.
        'ROBOTSTXT_OBEY': False,  # Robots.txt 파일의 권장사항을 지킬것인지 설정
        'LOG_ENABLED': True,  # 로그를 사용할 것인지
        'RETRY_TIMES': 3,  # 요청에 실패했을때 재요청을 몇번 시도할 것인지
        'DOWNLOAD_DELAY': 0.25,  # 다운로드시 딜레이는 얼마나 줄 것인지 -> 이 속도를 줄이면 속도가 빨라진다.
        'DOWNLOAD_TIMEOUT': 10,  # 다운로드시 시간초과를 몇 초까지 기다릴 것인지
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # 다운로드 딜레이를 랜덤하게 주는 것
        'LOG_LEVEL': 'INFO',  # 로그 레벨 설정
        # 'LOG_FILE': 'LOGS/news.log',  # ERROR 로그를 어디에 저장할 것인지
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
        },
        'RANDOM_UA_DESKTOP_ONLY ': True,
        'ITEM_PIPELINES': {  # Spider가 사용할 Pipeline 설정
            'navernews.pipelines.NewsPipeline': 300
        },
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,  # default: 16
        'CONCURRENT_REQUESTS_PER_IP': 16,  # default: 16
        'CONCURRENT_REQUESTS': 32,  # default: 16
    }

    def __init__(self, category=None, start=None, end=None, *a, **kw):  # Spider가 생성될때의 초기화
        """

        :param category: 크롤링하는 언론사 코드
        :param start: 크롤링 시작하는 날짜(미래) - 미래에서 과거로 크롤링함, 2020/03/24 형식
        :param end: 크롤링 끝나는 날짜(과거)
        :param a:
        :param kw:
        """

        print("*" * 100)
        print("뉴스 스파이더를 실행합니다.")
        self.news_category = category
        print(f"크롤링하는 언론사는 [{naver_news_code[self.news_category]}]({self.news_category}) 입니다.")
        print(f"크롤링하는 기간은 {start} ~ {end} 입니다.")
        self.start_date = start
        self.end_date = end
        print("데이터베이스 연결")

        db_connect = connection_db()

        self.article_id_list = dict()

        if db_connect.open:
            print("데이터베이스 연결 성공")
            try:
                with db_connect.cursor(pymysql.cursors.DictCursor) as cursor:
                    print("데이터베이스에 테이블이 있는지 확인합니다.")
                    sql = f"SELECT EXISTS(SELECT 1 FROM Information_schema.tables " \
                          "WHERE table_schema = 'news' AND TABLE_NAME = %s) AS flag"

                    try:
                        cursor.execute(sql, (self.news_category))
                        result = cursor.fetchall()

                        if result[0]['flag'] == 0:
                            print(f"[{naver_news_code[self.news_category]}]의 {self.news_category} 테이블이 없습니다. 생성합니다.")
                            sql = f'''
                                    CREATE TABLE if not exists news.`{self.news_category}` (
                                        `inx` INT(11) NOT NULL AUTO_INCREMENT,
                                        `news_id` VARCHAR(255) NOT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_title` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_content` LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_author` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_date` TIMESTAMP NULL DEFAULT NULL,
                                        `news_category` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_original_url` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_site` VARCHAR(30) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        `news_naver_url` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
                                        PRIMARY KEY (`inx`),
                                        UNIQUE INDEX `inx` (`inx`),
                                        UNIQUE INDEX `naver_news_id_uindex` (`news_id`)
                                    )
                                    COLLATE='utf8mb4_unicode_ci'
                                    ENGINE=InnoDB
                                    '''
                            cursor.execute(sql)

                        else:
                            print(f"[{naver_news_code[self.news_category]}]의 {self.news_category} 테이블이 이미 있습니다.")
                            print(f"데이터를 {self.news_category} 테이블에 저장합니다.")

                            with db_connect.cursor(pymysql.cursors.Cursor) as curs:
                                sql = f'SELECT news_id FROM news.`{self.news_category}`'
                                curs.execute(sql)
                                result = curs.fetchall()
                                for idx in result:
                                    self.article_id_list[idx[0]] = ""
                                print(f'데이터베이스에 이미 저장된 게시글 수 : {len(self.article_id_list)}개')

                    except pymysql.err.InternalError:
                        pass
                    except pymysql.err.IntegrityError:
                        pass
            finally:
                db_connect.close()
                pass
            print("*" * 100)
        else:
            print("데이터베이스 연결 실패")
            print("프로그램 종료")
            exit(2)

        super().__init__(*a, **kw)

    def start_requests(self):
        yield scrapy.Request(
            f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
            f"&listType=summary&oid={self.news_category}&date={self.start_date}&page=1",
            callback=self.parse_link,
            meta={'page': 1, 'date': self.start_date, 'end_date': self.end_date})

    def parse_link(self, response):
        # lint.nhn의 뉴스 기사 링크를 추출하는 파서
        # 추출시 페이징부분도 처리를 해서 리스트 페이지도 추출한다.
        crawl_date = response.meta['date']  # 현재 리스트 페이지의 참조 날짜
        crawl_page = response.meta['page']  # 현재 리스트 페이지의 참조 페이지
        end_date = response.meta['end_date']

        headline_list = response.xpath('//*[@id="main_content"]/'
                                       'div[@class="list_body newsflash_body"]/'
                                       'ul[@class="type06_headline"]/li')
        underline_list = response.xpath('//*[@id="main_content"]/'
                                        'div[@class="list_body newsflash_body"]/'
                                        'ul[@class="type06"]/li')

        print(
            f'[{crawl_date}]의 [{crawl_page}]페이지를 크롤링하는 중입니다.. [{len(headline_list) + len(underline_list)}]개의 기사를 가져왔습니다.')
        # 이 부분에서 리스트 페이지의 뉴스기사 링크를 추출한다.
        # parse_article로 값을 넘겨 뉴스 정보들을 추출해야 한다.
        for row in headline_list:
            link = row.xpath('dl/dt[1]/a/@href').extract_first()
            urlparsing = urlparse(link)
            url_query = parse_qs(urlparsing.query)
            arti_id = url_query['aid'][0]
            if arti_id not in self.article_id_list:
                yield scrapy.Request(link, callback=self.parse_article)

        for row in underline_list:
            link = row.xpath('dl/dt[1]/a/@href').extract_first()
            urlparsing = urlparse(link)
            url_query = parse_qs(urlparsing.query)
            arti_id = url_query['aid'][0]
            if arti_id not in self.article_id_list:
                yield scrapy.Request(link, callback=self.parse_article)

        # 페이징 처리 부분이다.
        paging = response.xpath('//*[@id="main_content"]/div[@class="paging"]/a[@class="nclicks(fls.page)"]')
        last_page = response.xpath(
            '//*[@id="main_content"]/div[@class="paging"]/a[@class="next nclicks(fls.page)"]/text()').extract_first()
        page_list = []
        for a in paging:
            val = a.xpath('text()').extract_first()
            page_list.append(int(val))
        try:
            max_page = max(page_list)
        except ValueError:
            max_page = 1

        if int(crawl_page) < max_page:
            next_page = int(crawl_page) + 1
            yield scrapy.Request(
                f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
                f"&listType=summary&oid={self.news_category}&date={crawl_date}&page={next_page}",
                callback=self.parse_link,
                meta={'page': next_page, 'date': crawl_date, 'end_date': end_date})
        elif int(crawl_page) > max_page:
            if last_page is not None:
                next_page = int(crawl_page) + 1
                yield scrapy.Request(
                    f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
                    f"&listType=summary&oid={self.news_category}&date={crawl_date}&page={next_page}",
                    callback=self.parse_link,
                    meta={'page': next_page, 'date': crawl_date, 'end_date': end_date})
            else:
                print(f'[{crawl_date}]날짜의 뉴스 기사 크롤링 완료')

        if crawl_page == 1:
            date_list = response.xpath(
                '//*[@id="main_content"]/div[@class="pagenavi_day"]/a[@class="nclicks(fls.date)"]')
            for dt in date_list:
                url = dt.xpath('@href').extract_first()
                urlparsing = urlparse(url)
                url_query = parse_qs(urlparsing.query)
                datee = url_query['date'][0]

                if int(crawl_date) > int(datee) >= int(end_date):
                    yield scrapy.Request(
                        f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
                        f"&listType=summary&oid={self.news_category}&date={datee}&page=1",
                        callback=self.parse_link,
                        meta={'page': 1, 'date': datee, 'end_date': end_date})
                    return

    def parse_article(self, response):
        if "list.nhn" in response.url:
            # 리스트 페이지를 크롤링 한경우, 아무 작업도 하지 않는다.
            # 어차피 LinkExtractor 가 링크는 추출하므로 따로 파싱작업을 할 필요가 없다.
            print(f"{'*' * 50} [ 기사목록 ] {'*' * 50}")
            print(f"[{response.url}] 에서 가져왔습니다.")

        else:
            article_type = response.xpath('/html/head/meta[@property="me:feed:serviceId"]/@content').extract_first()
            items = NewsItem()
            # 네이버 뉴스에서의 고유의 뉴스기사 ID값 - 일반, 스포츠, 연예 모두 동일
            urlparsing = urlparse(response.url)
            url_query = parse_qs(urlparsing.query)
            items['news_id'] = url_query['aid'][0]
            items['news_media_code'] = url_query['oid'][0]  # 뉴스 언론사 코드

            # 뉴스기사의 제목 - 일반, 스포츠, 연예 모두 동일
            items['news_title'] = response.xpath(
                '/html/head/meta[@property="og:title"]/@content').extract_first()

            # 뉴스기사의 네이버뉴스 URL - 일반, 소포츠, 연예 모두 동일
            items['news_naver_url'] = response.url

            if article_type is None:
                article_type = response.xpath('/html/head/meta[@name="twitter:site"]/@content').extract_first()

                # 뉴스기사를 발행한 언론사 이름 - 일반, 연예 동일
                items['news_site'] = response.xpath(
                    '/html/head/meta[@name="twitter:creator"]/@content').extract_first()

                if article_type == "네이버 TV연예":
                    print(f"{'*' * 50} [ 연예기사 ] {'*' * 50}")

                    # 뉴스기사의 카테고리
                    items['news_category'] = "TV연예"

                    # 뉴스기사 발행 시간
                    date = response.xpath('//*[@id="content"]/div[@class="end_ct"]/div/'
                                          'div[@class="article_info"]/span[@class="author"]/em/text()').extract_first()
                    try:
                        date = date.replace("오전", "AM")
                        date = date.replace("오후", "PM")
                        date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                        items['news_date'] = date_time_obj
                    except AttributeError:
                        items['news_date'] = date

                    # 뉴스기사의 언론사 URL
                    items['news_original_url'] = response.xpath('//*[@id="content"]/div[@class="end_ct"]/div/'
                                                                'div[@class="article_info"]/a/@href').extract_first()

                    # 연예뉴스 내용
                    content_tmp = response.xpath('//*[@id="articeBody"]/div/text()').extract()
                    for line in content_tmp:  # 저작권 라인 제거
                        if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                            inx = content_tmp.index(line)
                            del content_tmp[inx]
                    join_content = " ".join((" ".join(content_tmp)).split())

                    if join_content == "":
                        content_tmp = response.xpath('//*[@id="articeBody"]/text()').extract()
                        for line in content_tmp:  # 저작권 라인 제거
                            if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                                inx = content_tmp.index(line)
                                del content_tmp[inx]
                        join_content = " ".join((" ".join(content_tmp)).split())

                    items['news_content'] = join_content

                else:
                    print(f"{'*' * 50} [ 일반기사 ] {'*' * 50}")

                    # 뉴스기사의 카테고리
                    items['news_category'] = response.xpath(
                        '/html/head/meta[@property="me2:category2"]/@content').extract_first()

                    # 만약 카테고리가 속보인 경우 원본 기사에서 지정한 카테고리로 설정한다.
                    if items['news_category'] is None or items['news_category'] == "속보":
                        items['news_category'] = response.xpath(
                            '//*[@id="articleBody"]/div[@class="guide_categorization"]/a/em/text()').extract_first()

                    # 뉴스기사 발행 시간
                    date = response.xpath('//*[@id="main_content"]/div[@class="article_header"]'
                                          '/div[@class="article_info"]'
                                          '/div/span[@class="t11"][1]/text()').extract_first()
                    try:
                        date = date.replace("오전", "AM")
                        date = date.replace("오후", "PM")
                        date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                        items['news_date'] = date_time_obj
                    except AttributeError:
                        items['news_date'] = date

                    # 뉴스기사의 언론사 URL
                    items['news_original_url'] = response.xpath('//*[@id="main_content"]/div[@class="article_header"]'
                                                                '/div[@class="article_info"]/div/a[1]/@href').extract_first()

                    # 뉴스기사의 내용
                    content_tmp = response.xpath('//*[@id="articleBodyContents"]/div/text()').extract()
                    for line in content_tmp:  # 저작권 라인 제거
                        if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                            inx = content_tmp.index(line)
                            del content_tmp[inx]
                    join_content = " ".join((" ".join(content_tmp)).split())

                    if join_content == "":
                        content_tmp = response.xpath('//*[@id="articleBodyContents"]/text()').extract()
                        for line in content_tmp:  # 저작권 라인 제거
                            if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                                inx = content_tmp.index(line)
                                del content_tmp[inx]
                        join_content = " ".join((" ".join(content_tmp)).split())

                    if join_content == "":
                        content_tmp = response.xpath('//*[@id="articleBodyContents"]/div/span/text()').extract()
                        for line in content_tmp:  # 저작권 라인 제거
                            if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                                inx = content_tmp.index(line)
                                del content_tmp[inx]
                        join_content = " ".join((" ".join(content_tmp)).split())

                    items['news_content'] = join_content

            else:
                print(f"{'*' * 50} [ 스 포 츠 ] {'*' * 50}")

                # 뉴스기사를 발행한 언론사 이름
                site_tmp = response.xpath(
                    '/html/head/meta[@property="og:article:author"]/@content').extract_first()
                items['news_site'] = site_tmp.replace("네이버 스포츠 | ", "")

                # 뉴스기사의 카테고리
                items['news_category'] = "스포츠"

                # 뉴스기사 발행 시간
                date = response.xpath('//*[@id="content"]/div/div[@class="content"]/'
                                      'div/div[@class="news_headline"]/div[@class="info"]/span[1]/text()').extract_first()
                try:
                    date = date.replace("기사입력 ", "")
                    date = date.replace("오전", "AM")
                    date = date.replace("오후", "PM")
                    date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                    items['news_date'] = date_time_obj
                except AttributeError:
                    items['news_date'] = date

                # 뉴스기사의 언론사 원본 URL
                items['news_original_url'] = response.xpath('//*[@id="content"]/div/div[@class="content"]/'
                                                            'div/div[@class="news_headline"]/div[@class="info"]/a/@href').extract_first()

                # 스포츠기사 내용
                content_tmp = response.xpath('//*[@id="newsEndContents"]/div/text()').extract()

                for line in content_tmp:  # 저작권 라인 제거
                    if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                        inx = content_tmp.index(line)
                        del content_tmp[inx]
                join_content = " ".join((" ".join(content_tmp)).split())

                if join_content == "":
                    content_tmp = response.xpath('//*[@id="newsEndContents"]/text()').extract()
                    for line in content_tmp:  # 저작권 라인 제거
                        if "무단전재 및 재배포 금지" in line or "무단 전재 및 재배포 금지" in line or "ⓒ" in line:
                            inx = content_tmp.index(line)
                            del content_tmp[inx]
                    join_content = " ".join((" ".join(content_tmp)).split())

                items['news_content'] = join_content

            # 뉴스기사를 작성한 기자
            items['news_author'] = ""

            yield items


class News_manual_mongo(scrapy.Spider):
    """
    뉴스기사 크롤링 스파이더, LinkExtractor를 사용하지 않는 버전
    정해진 기간의 뉴스 기사를 크롤링하며, 어떤 언론사를 크롤링할지는 입력받는다.
    -a category=(언론사 코드)
    -a start=(시작 날짜)
    -a end=(끝나는 날짜)

    이때 시작날짜보다 끝나는 날짜가 더 과거여야 한다.
    예를 들어 1/1 ~ 3/24일 기간의 뉴스를 크롤링할 경우
    start=20200324 end=20200101로 설정해야 한다.
    그 이유는 최신뉴스 -> 과거뉴스 순으로 크롤링하도록 구성했기 때문..
    """
    name = 'newsmongo'  # 스파이더 이름 지정, 호출시에 사용하는 용도이다. 각 스파이더를 구분지어주기도 함

    custom_settings = {  # 기본적인 세팅값을 설정한다.
        'ROBOTSTXT_OBEY': False,  # Robots.txt 파일의 권장사항을 지킬것인지 설정
        'LOG_ENABLED': True,  # 로그를 사용할 것인지
        'RETRY_TIMES': 3,  # 요청에 실패했을때 재요청을 몇번 시도할 것인지
        'DOWNLOAD_DELAY': 0.1,  # 다운로드시 딜레이는 얼마나 줄 것인지 -> 이 속도를 줄이면 속도가 빨라진다.
        'DOWNLOAD_TIMEOUT': 5,  # 다운로드시 시간초과를 몇 초까지 기다릴 것인지
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # 다운로드 딜레이를 랜덤하게 주는 것
        'LOG_LEVEL': 'INFO',  # 로그 레벨 설정
        # 'LOG_FILE': 'LOGS/news.log',  # ERROR 로그를 어디에 저장할 것인지
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
        },
        'RANDOM_UA_DESKTOP_ONLY ': True,
        'ITEM_PIPELINES': {  # Spider가 사용할 Pipeline 설정
            'navernews.pipelines.MongodbPipeline': 300
        },
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,  # default: 16
        'CONCURRENT_REQUESTS_PER_IP': 16,  # default: 16
        'CONCURRENT_REQUESTS': 32,  # default: 16
    }

    def __init__(self, category=None, start=None, end=None, *a, **kw):  # Spider가 생성될때의 초기화
        """

        :param category: 크롤링하는 언론사 코드
        :param start: 크롤링 시작하는 날짜(미래) - 미래에서 과거로 크롤링함, 2020/03/24 형식
        :param end: 크롤링 끝나는 날짜(과거)
        :param a:
        :param kw:
        """

        print("*" * 100)
        print("뉴스 스파이더를 실행합니다.")
        self.news_category = category
        print(f"크롤링하는 언론사는 [{naver_news_code[self.news_category]}]({self.news_category}) 입니다.")
        print(f"크롤링하는 기간은 {start} ~ {end} 입니다.")
        self.start_date = start
        self.end_date = end
        self.article_id_list = dict()

        super().__init__(*a, **kw)

    def start_requests(self):
        yield scrapy.Request(
            f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
            f"&listType=summary&oid={self.news_category}&date={self.start_date}&page=1",
            callback=self.parse_link,
            meta={'page': 1, 'date': self.start_date, 'end_date': self.end_date})

    def parse_link(self, response):
        # lint.nhn의 뉴스 기사 링크를 추출하는 파서
        # 추출시 페이징부분도 처리를 해서 리스트 페이지도 추출한다.
        crawl_date = response.meta['date']  # 현재 리스트 페이지의 참조 날짜
        crawl_page = response.meta['page']  # 현재 리스트 페이지의 참조 페이지
        end_date = response.meta['end_date']

        headline_list = response.xpath('//*[@id="main_content"]/'
                                       'div[@class="list_body newsflash_body"]/'
                                       'ul[@class="type06_headline"]/li')
        underline_list = response.xpath('//*[@id="main_content"]/'
                                        'div[@class="list_body newsflash_body"]/'
                                        'ul[@class="type06"]/li')

        print(
            f'[{crawl_date}]의 [{crawl_page}]페이지를 크롤링하는 중입니다.. [{len(headline_list) + len(underline_list)}]개의 기사를 가져왔습니다.')
        # 이 부분에서 리스트 페이지의 뉴스기사 링크를 추출한다.
        # parse_article로 값을 넘겨 뉴스 정보들을 추출해야 한다.
        for row in headline_list:
            link = row.xpath('dl/dt[1]/a/@href').extract_first()
            urlparsing = urlparse(link)
            url_query = parse_qs(urlparsing.query)
            arti_id = url_query['aid'][0]
            if arti_id not in self.article_id_list:
                yield scrapy.Request(link, callback=self.parse_article)

        for row in underline_list:
            link = row.xpath('dl/dt[1]/a/@href').extract_first()
            urlparsing = urlparse(link)
            url_query = parse_qs(urlparsing.query)
            arti_id = url_query['aid'][0]
            if arti_id not in self.article_id_list:
                yield scrapy.Request(link, callback=self.parse_article)

        # 페이징 처리 부분이다.
        paging = response.xpath('//*[@id="main_content"]/div[@class="paging"]/a[@class="nclicks(fls.page)"]')
        last_page = response.xpath(
            '//*[@id="main_content"]/div[@class="paging"]/a[@class="next nclicks(fls.page)"]/text()').extract_first()
        page_list = []
        for a in paging:
            val = a.xpath('text()').extract_first()
            page_list.append(int(val))
        try:
            max_page = max(page_list)
        except ValueError:
            max_page = 1

        if int(crawl_page) < max_page:
            next_page = int(crawl_page) + 1
            yield scrapy.Request(
                f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
                f"&listType=summary&oid={self.news_category}&date={crawl_date}&page={next_page}",
                callback=self.parse_link,
                meta={'page': next_page, 'date': crawl_date, 'end_date': end_date})
        elif int(crawl_page) > max_page:
            if last_page is not None:
                next_page = int(crawl_page) + 1
                yield scrapy.Request(
                    f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
                    f"&listType=summary&oid={self.news_category}&date={crawl_date}&page={next_page}",
                    callback=self.parse_link,
                    meta={'page': next_page, 'date': crawl_date, 'end_date': end_date})
            else:
                print(f'[{crawl_date}]날짜의 뉴스 기사 크롤링 완료')

        if crawl_page == 1:
            date_list = response.xpath(
                '//*[@id="main_content"]/div[@class="pagenavi_day"]/a[@class="nclicks(fls.date)"]')
            for dt in date_list:
                url = dt.xpath('@href').extract_first()
                urlparsing = urlparse(url)
                url_query = parse_qs(urlparsing.query)
                datee = url_query['date'][0]

                if int(crawl_date) > int(datee) >= int(end_date):
                    yield scrapy.Request(
                        f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec"
                        f"&listType=summary&oid={self.news_category}&date={datee}&page=1",
                        callback=self.parse_link,
                        meta={'page': 1, 'date': datee, 'end_date': end_date})
                    return

    def parse_article(self, response):
        if "list.nhn" in response.url:
            # 리스트 페이지를 크롤링 한경우, 아무 작업도 하지 않는다.
            # 어차피 LinkExtractor 가 링크는 추출하므로 따로 파싱작업을 할 필요가 없다.
            print(f"{'*' * 30} [ 기사목록 ] {'*' * 30}")
            print(f"[{response.url}] 에서 가져왔습니다.")

        else:
            article_type = response.xpath('/html/head/meta[@property="me:feed:serviceId"]/@content').extract_first()
            items = NewsItem()
            # 네이버 뉴스에서의 고유의 뉴스기사 ID값 - 일반, 스포츠, 연예 모두 동일
            urlparsing = urlparse(response.url)
            url_query = parse_qs(urlparsing.query)
            items['news_id'] = url_query['aid'][0]
            items['news_media_code'] = url_query['oid'][0]  # 뉴스 언론사 코드

            # 뉴스기사의 제목 - 일반, 스포츠, 연예 모두 동일
            items['news_title'] = response.xpath(
                '/html/head/meta[@property="og:title"]/@content').extract_first()

            # 뉴스기사의 네이버뉴스 URL - 일반, 소포츠, 연예 모두 동일
            items['news_naver_url'] = response.url

            if article_type is None:
                article_type = response.xpath('/html/head/meta[@name="twitter:site"]/@content').extract_first()

                # 뉴스기사를 발행한 언론사 이름 - 일반, 연예 동일
                items['news_site'] = response.xpath(
                    '/html/head/meta[@name="twitter:creator"]/@content').extract_first()

                if article_type == "네이버 TV연예":
                    print(f"{'*' * 30} [ 연예기사 ] {'*' * 30}")

                    # 뉴스기사의 카테고리
                    items['news_category'] = "TV연예"

                    # 뉴스기사 발행 시간
                    date = response.xpath('//*[@id="content"]/div[@class="end_ct"]/div/'
                                          'div[@class="article_info"]/span[@class="author"]/em/text()').extract_first()
                    try:
                        date = date.replace("오전", "AM")
                        date = date.replace("오후", "PM")
                        date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                        items['news_date'] = date_time_obj
                    except AttributeError:
                        items['news_date'] = date

                    # 뉴스기사의 언론사 URL
                    ori_url = response.xpath('//*[@id="content"]/div[@class="end_ct"]/div/'
                                             'div[@class="article_info"]/a/@href').extract_first()
                    try:
                        if 'http://' in ori_url or 'https://' in ori_url:
                            items['news_original_url'] = ori_url
                        else:
                            items['news_original_url'] = ""
                    except TypeError:
                        items['news_original_url'] = ""

                    # 연예뉴스 내용
                    content_tmp = response.xpath('//*[@id="articeBody"]').extract_first()
                    items['news_content'] = clean_html(content_tmp)

                else:
                    print(f"{'*' * 30} [ 일반기사 ] {'*' * 30}")

                    # 뉴스기사의 카테고리
                    items['news_category'] = response.xpath(
                        '/html/head/meta[@property="me2:category2"]/@content').extract_first()

                    # 만약 카테고리가 속보인 경우 원본 기사에서 지정한 카테고리로 설정한다.
                    if items['news_category'] is None or items['news_category'] == "속보":
                        items['news_category'] = response.xpath(
                            '//*[@id="articleBody"]/div[@class="guide_categorization"]/a/em/text()').extract_first()

                    # 뉴스기사 발행 시간
                    date = response.xpath('//*[@id="main_content"]/div[@class="article_header"]'
                                          '/div[@class="article_info"]'
                                          '/div/span[@class="t11"][1]/text()').extract_first()
                    try:
                        date = date.replace("오전", "AM")
                        date = date.replace("오후", "PM")
                        date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                        items['news_date'] = date_time_obj
                    except AttributeError:
                        items['news_date'] = date

                    # 뉴스기사의 언론사 URL
                    ori_url = response.xpath('//*[@id="main_content"]/div[@class="article_header"]'
                                             '/div[@class="article_info"]/div/a[1]/@href').extract_first()
                    try:
                        if 'http://' in ori_url or 'https://' in ori_url:
                            items['news_original_url'] = ori_url
                        else:
                            items['news_original_url'] = ""
                    except TypeError:
                        items['news_original_url'] = ""

                    # 뉴스기사의 내용
                    news_content = response.xpath('//*[@id="articleBodyContents"]').extract_first()
                    items['news_content'] = clean_html(news_content)

            else:
                print(f"{'*' * 30} [ 스 포 츠 ] {'*' * 30}")

                # 뉴스기사를 발행한 언론사 이름
                site_tmp = response.xpath(
                    '/html/head/meta[@property="og:article:author"]/@content').extract_first()
                items['news_site'] = site_tmp.replace("네이버 스포츠 | ", "")

                # 뉴스기사의 카테고리
                items['news_category'] = "스포츠"

                # 뉴스기사 발행 시간
                date = response.xpath('//*[@id="content"]/div/div[@class="content"]/'
                                      'div/div[@class="news_headline"]/div[@class="info"]/span[1]/text()').extract_first()
                try:
                    date = date.replace("기사입력 ", "")
                    date = date.replace("오전", "AM")
                    date = date.replace("오후", "PM")
                    date_time_obj = datetime.datetime.strptime(date, '%Y.%m.%d. %p %I:%M')
                    items['news_date'] = date_time_obj
                except AttributeError:
                    items['news_date'] = date

                # 뉴스기사의 언론사 URL
                ori_url = response.xpath('//*[@id="content"]/div/div[@class="content"]/'
                                         'div/div[@class="news_headline"]/div[@class="info"]/a/@href').extract_first()
                try:
                    if 'http://' in ori_url or 'https://' in ori_url:
                        items['news_original_url'] = ori_url
                    else:
                        items['news_original_url'] = ""
                except TypeError:
                    items['news_original_url'] = ""

                # 스포츠기사 내용
                content_tmp = response.xpath('//*[@id="newsEndContents"]').extract_first()
                items['news_content'] = clean_html(content_tmp)

            # 뉴스기사를 작성한 기자
            items['news_author'] = ""

            yield items
