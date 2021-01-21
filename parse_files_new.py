import scrapy
import json
import time
import mysql.connector
import urllib.parse
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
            print(disk + ": opening...")
            time.sleep(1)

            yield scrapy.Request(url=disk, callback=self.parse_disk, meta={'handle_httpstatus_all': True, 'active_main_disk': disk})
        
    
    def parse_disk(self, response):
            json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
            res = list(json_obj['resources'].values())
            
            if len(res) is not 1 and res[0]['completed'] is False:
                print(response.meta['active_main_disk'] + ": fetching lists... (0)")
                time.sleep(1)

                yield scrapy.Request(
                        method='POST', 
                        callback=self.parse_json,
                        meta={'handle_httpstatus_all': True, 'offset': 0, 'hash': res[0]['hash'], 'sk': json_obj['environment']['sk'], 'active_main_disk': response.meta['active_main_disk']}, 
                        url='https://yadi.sk/public/api/fetch-list', 
                        body=urllib.parse.quote(json.dumps({'offset': 0, 'hash': res[0]['hash'], 'sk': json_obj['environment']['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                        headers={"Content-Type": "text/plain", "Cookies": self.cookie}
                )
            else:
                print(response.meta['active_main_disk'] + ": completed...")
                time.sleep(1)

                res.pop(0)
                self.handle_resources(res)
                #resource(s) will be writed to sql
                #print(response.url + " true aga: " + str(len(res)))
                pass

    def parse_json(self, response):
        try:
            newSk = json.loads(response.body.decode('utf-8'))['newSk']
            yield scrapy.Request(
                method='POST', 
                callback=self.parse_json,
                meta={'handle_httpstatus_all': True, 'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk': newSk, 'active_main_disk': response.meta['active_main_disk']}, 
                url='https://yadi.sk/public/api/fetch-list', 
                body=urllib.parse.quote(json.dumps({'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk': newSk, "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                headers={"Content-Type": "text/plain", "Cookies": self.cookie}
            )
        except:
            res_body = json.loads(response.body.decode('utf-8'))
            is_completed = res_body['completed']
            resources = res_body['resources']
            resources.pop(0)
            self.handle_resources(resources)

            if response.meta['offset'] == 0:
                response.meta['offset'] = 20
            else:
                response.meta['offset'] = response.meta['offset'] + 20

            print(response.meta['active_main_disk'] + ": fetching lists... ("+ (response.meta['offset']) +")")
            time.sleep(1)
            
            if is_completed is False:
                yield scrapy.Request(
                    method='POST', 
                    callback=self.parse_json,
                    meta={'handle_httpstatus_all': True, 'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk': response.meta['sk'], 'active_main_disk': response.meta['active_main_disk']}, 
                    url='https://yadi.sk/public/api/fetch-list', 
                    body=urllib.parse.quote(json.dumps({'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk': response.meta['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                    headers={"Content-Type": "text/plain", "Cookies": self.cookie}
                )

    def handle_resources(self, resources):
        for resource in resources:
            if resource['type'] == 'dir':
                print('https://disk.yandex.com.tr/public?hash=' + resource['path'] + ": opening...")
                time.sleep(1)

                yield scrapy.Request(url='https://disk.yandex.com.tr/public?hash=' + resource['path'], callback=self.parse_disk, meta={'handle_httpstatus_all': True})
            else:
                print(resource['name'])
