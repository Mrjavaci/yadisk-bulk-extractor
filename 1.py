#!/usr/bin/python
# -*- coding: utf-8 -*-

import scrapy
import json
import urllib.parse
import subprocess

class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    activeCookie = ''
    offsets = []
    reqs = []
    cookie = ''
    
    disk_data = []
    
    def start_requests(self):
        with open('D:\\Projects\\Python\\yadisk\\files.txt') as f:
            disk_list = f.read().splitlines()
        
        for disk in disk_list[0:1]:
            yield scrapy.Request(url=disk, callback=self.parse_disk, meta={'handle_httpstatus_all': True, 'offset': 0, 'disk_url': disk, 'main_url': disk})
            
    def parse_disk(self, response):
        json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
        
        temp_offset = response.meta['offset']
        disk_url = response.meta['disk_url']
        
        #print(disk_url + ": " + "Opening...")
        
        temp_sk = json_obj['environment']['sk']
        
        if self.cookie == '':
            self.cookie = response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")
        
        temp_hash = ''
        
        try:
            temp_hash = response.meta['hash']
        except:
            for resource in list(json_obj['resources'])[0:1]:
                temp_hash = json_obj['resources'][resource]['hash']
        
        yield scrapy.Request(
                method='POST', 
                callback=self.parse_json,
                meta={'handle_httpstatus_all': True, 'is_again': False, 'offset': temp_offset, 'hash': temp_hash, 'sk': temp_sk, 'disk_url': disk_url, 'main_url':response.meta['main_url']}, 
                url='https://yadi.sk/public/api/fetch-list', 
                body=urllib.parse.quote(json.dumps({"hash":temp_hash,"offset":temp_offset,"withSizes":'true',"sk":temp_sk,"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                headers={"Content-Type": "text/plain", "Cookies": self.cookie, "Host": "yadi.sk", "Origin": "https://yadi.sk", "Referer": disk_url}
        )

    
    def parse_json(self,response):
            
        res_body = json.loads(response.body.decode('utf-8'))
        
        temp_offset = response.meta['offset']
        temp_offset = int(response.meta['offset']) + 20
        disk_url = response.meta['disk_url']
        
        temp_sk = response.meta['sk']
        temp_hash = response.meta['hash']
        
        is_again = response.meta['is_again']
        is_safe = True
        
        #if is_again is False:
        #    if temp_offset == 0:
        #        print("______________________________\n\n"+ disk_url + ": " + "(Page 1)...______________________________\n")
        #    else:
        #        print("______________________________\n\n"+ disk_url + ": " + "(Page "+ str(int(temp_offset / 20)) +")...\n______________________________\n")
        
        try:
            if res_body['error'] is True:
                print(temp_hash)
                print(disk_url)
                is_safe = False
        except:
            pass
        
        if is_safe is False:
            try:
                if res_body['newSk'] is not None:
                    temp_sk = res_body['newSk']
                    
                    yield scrapy.Request(
                            method='POST', 
                            callback=self.parse_json,
                            meta={'handle_httpstatus_all': True, 'is_again': True, 'offset': temp_offset, 'hash': temp_hash, 'sk': temp_sk, 'disk_url': disk_url, 'main_url':response.meta['main_url']}, 
                            url='https://yadi.sk/public/api/fetch-list', 
                            body=urllib.parse.quote(json.dumps({"hash":temp_hash,"offset":temp_offset,"withSizes":'true',"sk":temp_sk,"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                            headers={"Content-Type": "text/plain", "Cookies": self.cookie, "Host": "yadi.sk", "Origin": "https://yadi.sk", "Referer": disk_url}
                    )
            except:
                pass
        else:
            is_completed = res_body['completed']
            
            resources = res_body['resources']
            resources.pop(0)
            
            self.disk_data = self.disk_data + resources
            
            if is_completed is False:
                #print('https://yadi.sk/public/api/fetch-list')
                yield scrapy.Request(
                        method='POST', 
                        callback=self.parse_json,
                        meta={'handle_httpstatus_all': True, 'is_again': False, 'offset': temp_offset, 'hash': temp_hash, 'sk': temp_sk, 'disk_url': disk_url, 'main_url':response.meta['main_url']}, 
                        url='https://yadi.sk/public/api/fetch-list', 
                        body=urllib.parse.quote(json.dumps({"hash":temp_hash,"offset":temp_offset,"withSizes":'true',"sk":temp_sk,"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                        headers={"Content-Type": "text/plain", "Cookies": self.cookie, "Host": "yadi.sk", "Origin": "https://yadi.sk", "Referer": disk_url}
                )
            else:
                disk_resources = self.disk_data
                self.disk_data = []
                
                for resource in disk_resources:
                    resource_name = resource["name"]
                    new_disk_url = disk_url + "/" + urllib.parse.quote(resource_name)
                    main_disk_url = response.meta["main_url"]
                    #print(new_disk_url)
                    
                    if disk_url == main_disk_url:
                        temp_hash_2 = temp_hash + ':/' + resource['name']
                    else:
                        temp_hash_2 = temp_hash + '/' + resource['name']
                    
                    if resource["type"] == 'dir':
                        yield scrapy.Request(url=new_disk_url, callback=self.parse_disk, meta={'handle_httpstatus_all': True, 'offset': 0, 'disk_url': new_disk_url, 'hash': temp_hash_2, 'main_url':response.meta['main_url']}, headers={"Referer": disk_url})
                    else:
                        main_disk_url = main_disk_url
                        folders_string = new_disk_url.replace(main_disk_url, '').replace(urllib.parse.quote(resource_name), '')
                        filename_final = urllib.parse.quote(resource_name)
                        
                        print(main_disk_url + folders_string + filename_final)