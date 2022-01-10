import scrapy
import mysql
from mysql.connector import Error
from urllib.parse import unquote
from timeit import default_timer as timer

start = timer()
import time


class indeedgr(scrapy.Spider):
    name = "indeed"
    allowed_domains = ['gr.indeed.com']
    start_urls = ['https://gr.indeed.com/jobs?l=Ελλάδα&radius=100&ts=1565553176523&pts=1564835113484&rq=1&fromage=last']

    # initializing crawler variables
    def __init__(self):
        # this here connect to database and q all url that have been crawled and store it into records.
        count_new = 0
        count_exist = 0
        self.count_new = count_new
        self.count_exist = count_exist
        self.runonce = 0
        self.i = 0

    # Crawl each category list of job links
    def parse(self, response):
        isExists = response.xpath("//div[@class='pagination']//a/@href").extract()
        url = "https://gr.indeed.com"
        search = "/jobs?q=&l=Ελλάδα&radius=100&fromage=last&start="
        my_list = isExists[0]
        if my_list == []:
            print("empty")

        if self.i <= 40:
            self.i = self.i + 10
            url_fin = url + search + str(self.i)
            # print(url_fin)
            job_content = response.xpath(
                "//div[@class='jobsearch-SerpJobCard unifiedRow row result']//h2[@class='title']//a/@href").extract()
            for con in range(len(job_content)):
                urlnext = url + job_content[con]
                yield response.follow(urlnext, callback=self.parse_page)

            next_page = response.urljoin(url_fin)
            if next_page:
                request = scrapy.Request(url_fin, callback=self.parse, dont_filter=True)
                yield request

        else:
            try:
                url_fin = url + isExists[5]
                # print(url_fin)
                job_content = response.xpath(
                    "//div[@class='jobsearch-SerpJobCard unifiedRow row result']//h2[@class='title']//a/@href").extract()
                for con in range(len(job_content)):
                    urlnext = url + job_content[con]
                    yield response.follow(urlnext, callback=self.parse_page)
                next_page = response.urljoin(url_fin)
                if next_page:
                    request = scrapy.Request(url_fin, callback=self.parse, dont_filter=True)
                    yield request


            except IndexError:
                pass

    def parse_page(self, response):
        # this is the list with the job positions from the database
        job_url = response.url
        job_url = unquote(job_url)
        job_html = response.body
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        try:
            connection = mysql.connector.connect(host='snf-876565.vm.okeanos.grnet.gr',
                                                 database='crawlerdb',
                                                 user='root',
                                                 password='10dm1@b0320')
            cursor = connection.cursor(prepared=True)
            c = connection.cursor()
            c.execute("SELECT count(job_url) FROM store_all WHERE job_url LIKE %s ", ("%" + job_url + "%",))

            data = c.fetchall()
            data = data[0][0]
            data2 = int(data)
            if data2 >= 1:
                self.count_exist += 1

            else:
                try:
                    cursor = connection.cursor(prepared=True)
                    sql_insert_query = """ INSERT INTO 
                                                                                         `store_all`(
                                                                                          `job_url`,
                                                                                          `job_description`,
                                                                                          `Date`
                                                                                          )VALUES (%s,%s,%s)"""
                    insert_tuple = (job_url, job_html, timestamp)
                    result = cursor.execute(sql_insert_query, insert_tuple)
                    connection.commit()
                    # print("Record inserted successfully into table")
                    self.count_new = self.count_new + 1
                    end = timer()
                    print("Indeed: Time cpu run:", end - start, ",", "New entries :", self.count_new, "& job url",
                          job_url, ",", "Exist item's", self.count_exist, "pages crawled")
                except mysql.connector.Error as error:
                    connection.rollback()
                    print("Failed to insert into MySQL table {}".format(error))

        except mysql.connector.Error as error:
            connection.rollback()
            self.count_exist += 1
            print("Failed to insert into MySQL table {}".format(error))
