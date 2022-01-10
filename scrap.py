from kariera import kariera

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.settings import Settings
import threading


settings = Settings() #scrapy https://docs.scrapy.org/en/latest/topics/practices.html
runner = CrawlerRunner(settings) #to get a Settings instance with your project settings

d2 = runner.crawl(kariera)

reactor.run()


#d.addBoth(lambda _: reactor.stop())
#d1.addBoth(lambda _: reactor.stop())
#d2.addBoth(lambda _: reactor.stop())
#d3.addBoth(lambda _: reactor.stop())
 # the script will block here until the crawling is finished. The reactor is the core of the event loop within Twisted