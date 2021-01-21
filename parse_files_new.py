import scrapy
import json
import mysql.connector
import urllib.parse
class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="yadi"
    )
    
    last_cookie = None

    #########################################################################################################

    def get_cookie(self, response):
        try:
            self.last_cookie = response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")
        except:
            pass
        
        return self.last_cookie

    def handle_file(self, shit):
        print(shit)

    #########################################################################################################
    
    def start_requests(self):
        with open('.\\yadi-check-results\\clean.txt') as f:
            disk_list = f.read().splitlines()
        
        for disk in disk_list:

            yield scrapy.Request(url=disk, callback=self.parse_url, meta={'handle_httpstatus_all': True, 'active_main_disk': disk})
        
    
    def parse_url(self, response):
        json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
        resources = list(json_obj['resources'].values())
        
        if len(resources) is 1:
            for resource in resources:
                print(resource['name'])
        else:
            yield scrapy.Request(
                method='POST', 
                callback=self.fetch_list,
                meta={'handle_httpstatus_all': True, 'offset': 0, 'hash': resources[0]['hash'], 'sk': json_obj['environment']['sk']}, 
                url='https://yadi.sk/public/api/fetch-list', 
                body=urllib.parse.quote(json.dumps({'offset': 0, 'hash': resources[0]['hash'], 'sk': json_obj['environment']['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
            )
        
    def fetch_list(self, response):
        try:
            newSk = json.loads(response.body.decode('utf-8'))['newSk']
            
            yield scrapy.Request(
                method='POST', 
                callback=self.fetch_list,
                meta={'handle_httpstatus_all': True, 'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk':newSk}, 
                url='https://yadi.sk/public/api/fetch-list', 
                body=urllib.parse.quote(json.dumps({'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk': newSk, "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
            )
        except:
            res_body = json.loads(response.body.decode('utf-8'))
            
            is_completed = res_body['completed']
            resources = res_body['resources']
            resources.pop(0)
            
            for resource in resources:
                if resource['type'] == 'file':
                    print(resource['name'])
                else:
                    yield scrapy.Request(
                        method='POST', 
                        callback=self.fetch_list,
                        meta={'handle_httpstatus_all': True, 'offset': 0, 'hash': resource['path'], 'sk': response.meta['sk']}, 
                        url='https://yadi.sk/public/api/fetch-list', 
                        body=urllib.parse.quote(json.dumps({'offset': 0, 'hash': resource['path'], 'sk': response.meta['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                        headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
                    )
            
            if is_completed is False:
                yield scrapy.Request(
                        method='POST', 
                        callback=self.fetch_list,
                        meta={'handle_httpstatus_all': True, 'offset': response.meta['offset'] + 20, 'hash': response.meta['hash'], 'sk': response.meta['sk']}, 
                        url='https://yadi.sk/public/api/fetch-list', 
                        body=urllib.parse.quote(json.dumps({'offset': response.meta['offset'] + 20, 'hash': response.meta['hash'], 'sk': response.meta['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                        headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
                    )