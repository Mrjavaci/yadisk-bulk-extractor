import scrapy
import json
import urllib.parse
import mysql.connector
import time
class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    
    last_sk = None
    last_hash = None
    last_cookie = None
    offset = 0
    
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
            self.offset = 0
            yield scrapy.Request(url=disk, callback=self.parse_disk, meta={'handle_httpstatus_all': True})
    
    def parse_folders(self, response):
        try:
            newSk = json.loads(response.body.decode('utf-8'))['newSk']
            req_obj = json.dumps({"hash":self.get_hash(response),"offset":self.offset,"sk":newSk,"withSizes":True,"options":{"hasExperimentVideoWithoutPreview":'true'}})
            
            yield scrapy.Request(
                method='POST', 
                callback=self.parse_folders,
                meta={'handle_httpstatus_all': True}, 
                url='https://yadi.sk/public/api/fetch-list', 
                body=urllib.parse.quote(req_obj), 
                headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
            )
        except:
            if self.offset == 0:
                self.offset = 20
            else:
                self.offset = self.offset + 20
                
            #buradan devam...
    
    def parse_disk(self, response):
        if self.is_folder(response):
            req_obj = json.dumps({"hash":self.get_hash(response),"offset":self.offset,"sk":self.get_sk(response),"withSizes":True,"options":{"hasExperimentVideoWithoutPreview":'true'}})
            
            yield scrapy.Request(
                method='POST', 
                callback=self.parse_folders,
                meta={'handle_httpstatus_all': True}, 
                url='https://yadi.sk/public/api/fetch-list', 
                body=urllib.parse.quote(req_obj), 
                headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
            )
        else:
            #json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
            #item = list(json_obj['resources'].values())[0]
            #print(self.get_sk(response) + " " + self.get_hash(response))
            pass