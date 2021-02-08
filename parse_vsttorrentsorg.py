import scrapy
import json
import mysql.connector
import urllib.parse
import time
import re
class ekitapsiteParser(scrapy.Spider):
    name = 'vsttorrentsorg'
    
    #########################################################################################################

    def start_requests(self):
        for id in range(1,38):
            yield scrapy.Request(url="https://vstplugs.com/category/wav/page/"+ str(id) +"/", callback=self.parse_url, meta={'handle_httpstatus_all': True})
    
    def parse_url(self, response):
        for pack in response.xpath("//h3[@class='primewp-fp04-post-title']"):
            pack_name = pack.xpath('a/text()').get()
            pack_href = pack.xpath('a/@href').get()

            with open(".//vstplugscom.txt", 'a', encoding='utf-8') as f:
                f.write(pack_name + " - " + pack_href + "\n")