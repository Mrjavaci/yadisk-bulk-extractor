import scrapy
import json
import urllib.parse
import subprocess
import sys
import mysql.connector
class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    activeCookie = ''
    offsets = []
    reqs = []
    cookie = ''
    
    disk_data = []
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="scrapy"
    )

    #########################################################################################################

    def is_folder(self, response):
        if response.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' folder-content ')]").get() is not None:
            return True
        else:
            return False

    def get_sk(self, response):
        json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
        
        return json_obj['environment']['sk']

    def get_hash(self, response):
        json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
        temp_hash = ''

        for resource in list(json_obj['resources'])[0:1]:
            temp_hash = json_obj['resources'][resource]['hash']

        return temp_hash
    
    def get_cookie(self, response):
        return response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")

    #########################################################################################################
    
    def start_requests(self):
        with open('C:\\Projects\\python\\yadisk-bulk-extractor\\yadi-check-results\\clean.txt') as f:
            disk_list = f.read().splitlines()
        
        for disk in disk_list:
            yield scrapy.Request(url=disk, callback=self.parse_disk, meta={'handle_httpstatus_all': True, 'offset': 0, 'disk_url': disk, 'main_url': disk})
            
    def parse_disk(self, response):
        if self.is_folder(response):
            print(self.get_cookie(response))
            pass
        else:
            json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
            item = list(json_obj['resources'].values())[0]