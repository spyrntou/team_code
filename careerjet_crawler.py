# import scrapy
# import mysql
# from mysql.connector import Error
# from urllib.parse import unquote
# import time
#
# start_urls = []
# from timeit import default_timer as timer
#
# start = timer()
#
# f = open("c:/users/administrator/Documents/GitHub/crawler/crawler/crawler/spiders/starting_urls_careerjet.txt", "r")
# for line in f:
#     start_urls.append(line.strip())
# class ifexist():
#     def ifexist(data):
#         try:
#             connection = mysql.connector.connect(host='snf-876565.vm.okeanos.grnet.gr',
#                                                  database='crawlerdb',
#                                                  user='root',
#                                                  password='10dm1@b0320')
#             cursor = connection.cursor(prepared=True)
#             sql_insert_query = "SELECT `job_url` FROM `store_all` where `job_url` like '%" + str(data) + "%'"
#             cursor = connection.cursor()
#             cursor.execute(sql_insert_query)
#             result = cursor.fetchall()
#             return result
#             connection.commit()
#         except mysql.connector.Error as error:
#             connection.rollback()
#             print("Failed to insert into MySQL table {}".format(error))
#
#
# class careerjet(scrapy.Spider):
#     name = "careerjet"
#     allowed_domains = ['www.careerjet.gr']
#     start_urls = start_urls
#     list_items = (len(start_urls))
#     def __init__(self):
#         # this here connect to database and q all url that have been crawled and store it into records.
#         count_new = 0
#         count_exist = 0
#         self.count_new = count_new
#         self.count_exist = count_exist
#         try:
#             self.connection = mysql.connector.connect(host='snf-876565.vm.okeanos.grnet.gr',
#                                                  database='crawlerdb',
#                                                  user='root',
#                                                  password='10dm1@b0320')
#             cursor = self.connection.cursor(prepared=True)
#             sql_select_Query = "SELECT job_url FROM store_all"
#             self.cursor = self.connection.cursor()
#             self.cursor.execute(sql_select_Query)
#             records = self.cursor.fetchall()  # this is dangerous should be replaced!!!!
#             self.records = records
#         except mysql.connector.Error as error:
#             self.connection.rollback()
#             print("Failed to connect {}".format(error))
#
#     def parse(self, response):
#         job_url = response.url
#         job_url = unquote(job_url)
#         list_items = (len(start_urls))
#         print(list_items,self.count_new)
#         url_from_database = ifexist.ifexist(job_url)
#         if len(url_from_database) < 1:
#             job_html = response.body
#             timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
#             try:
#                 self.cursor = self.connection.cursor(prepared=True)
#                 sql_insert_query = """ INSERT INTO
#                                                             `store_all`(
#                                                              `job_url`,
#                                                              `job_description`,
#                                                              `Date`
#                                                              )VALUES (%s,%s,%s)"""
#                 insert_tuple = (job_url, job_html, timestamp)
#                 result = self.cursor.execute(sql_insert_query, insert_tuple)
#                 self.connection.commit()
#                 # print("Record inserted successfully into table")
#                 self.count_new = self.count_new + 1
#             except mysql.connector.Error as error:
#                 self.connection.rollback()
#                 print("Failed to insert into MySQL table {}".format(error))
#             end = timer()
#             print("Careerjet : Time cpu run:", end - start, ",", "New entries :", self.count_new, "& job url", job_url,
#                   ",",
#                   "Exist item's", self.count_exist)
#         else:
#             self.count_new = self.count_new + 1
#             print("exist")
#
# # C:\Users\spyrntou\AppData\Local\Programs\Python\Python38\Scripts\scrapy crawl skywalker    terminal run
