import scrapy
import json
import urllib.parse
import subprocess
import sys
import mysql.connector
class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    
    last_sk = None
    last_hash = None
    last_cookie = None
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="yadi"
    )

    #########################################################################################################

    def is_folder(self, response):
        if response.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' folder-content ')]").get() is not None:
            return True
        else:
            return False

    def get_sk(self, response):
        try:
            self.last_sk = json.loads(response.css("script#store-prefetch ::text").extract_first())['environment']['sk']
        except:
            pass
        
        return self.last_sk

    def get_hash(self, response):
        try:
            json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())

            for resource in list(json_obj['resources'])[0:1]:
                self.last_hash = json_obj['resources'][resource]['hash']
        except:
            pass
        
        return self.last_hash
    
    def get_cookie(self, response):
        try:
            self.last_cookie = response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")
        except:
            pass
        
        return self.last_cookie

    #########################################################################################################
    
    def start_requests(self):
        with open('.\\yadi-check-results\\clean.txt') as f:
            disk_list = f.read().splitlines()
        
        for disk in disk_list:
            yield scrapy.Request(url=disk, callback=self.parse_disk, meta={'handle_httpstatus_all': True, 'offset': 0, 'disk_url': disk, 'main_url': disk})
            
    def parse_disk(self, response):
        if self.is_folder(response):
            print(self.get_sk(response) + " " + self.get_hash(response))
        else:
            #json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
            #item = list(json_obj['resources'].values())[0]
            print(self.get_sk(response) + " " + self.get_hash(response))