import scrapy
import json
import urllib.parse
import subprocess
import sys
import mysql.connector
import os.path

class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    activeCookie = ''
    offsets = []
    reqs = []
    cookie = ''
    
    disk_data = []
    
    def start_requests(self):
        file_path = 'C:\\Projects\\python\\yadisk-bulk-extractor\\files_will_be_checked.txt'
        if os.path.isfile(file_path):
            with open(file_path) as f:
                file_list = f.read().splitlines()
            
            for file in file_list:
                yield scrapy.Request(url=file, callback=self.parse_disk, meta={'handle_httpstatus_all': True})
        else:
            print("\n\ndosya bulunamadı\n\n")
    def parse_disk(self, response):
        if response.status == 301:
            yield scrapy.Request(url=response.headers['location'].decode('utf-8'), callback=self.parse_disk, meta={'handle_httpstatus_all': True})
        else:
            error = response.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' error ')]").get()

            if error is None:
                with open("C:\\Projects\\python\\yadisk-bulk-extractor\\yadi-check-results\\clean.txt", 'a') as f:
                    f.write(response.url + "\n")
                pass
            else:
                with open("C:\\Projects\\python\\yadisk-bulk-extractor\\yadi-check-results\\error.txt", 'a') as f:
                    f.write(response.url + "\n")
                pass