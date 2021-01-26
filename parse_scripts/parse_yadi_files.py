import scrapy
import json
import mysql.connector
import urllib.parse
import time
class yaDiskParserSpider(scrapy.Spider):
    name = 'yaDiskParser'
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="scrapy"
    )
    
    last_cookie = None

    #########################################################################################################

    def get_cookie(self, response):
        try:
            self.last_cookie = response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")
        except:
            pass
        
        return self.last_cookie

    def handle_file(self, resource, response):
        file_id = resource['id']
        file_name = resource['name']
        file_size = resource['meta']["size"]
        file_ext = resource['meta']["ext"]
        file_diskname = response.meta['amd_name']
        file_diskurl= "/".join(response.meta['active_main_disk'].split('/')[0:5])

        file_folders = response.meta['active_main_disk'].split('/')[5:]
        if len(response.meta['active_main_disk'].split('/')[5:]) == 0:
            file_folders = '/'
        else:
            file_folders = '/' + "/".join(response.meta['active_main_disk'].split('/')[5:])

        file_url = None
        try:
            file_url = resource['meta']['short_url']
        except:
            file_url = response.meta['active_main_disk'] + '/' + file_name

        file_hash = None
        try:
            file_hash = resource['path']
        except:
            file_hash = resource['hash']
        
        if file_ext in ['zip','rar','pdf','epub','doc','docx','mp3', 'cbr', 'mobi']:
            try:
                cursor = self.conn.cursor()
                cursor.execute("INSERT INTO yadisk (id,diskname,filename,directory,filesize,diskurl,fileurl,hash,ext) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                    (file_id, urllib.parse.unquote(file_diskname), urllib.parse.unquote(file_name), urllib.parse.unquote(file_folders), str(file_size), file_diskurl, file_url, file_hash, urllib.parse.unquote(file_ext))
                )
                self.conn.commit()
                print(file_name + ": eklendi...")
            except:
                pass

    #########################################################################################################
    
    def start_requests(self):
        with open('.\\yadi-check-results\\clean.txt') as f:
            disk_list = f.read().splitlines()
        
        for disk in disk_list:
            yield scrapy.Request(url=disk, callback=self.parse_url, meta={'handle_httpstatus_all': True, 'active_main_disk': disk})
    
    def parse_url(self, response):
        json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
        resources = list(json_obj['resources'].values())
        response.meta['amd_name'] = resources[0]['name']
        
        if len(resources) is 1:
            for resource in resources:
                self.handle_file(resource, response)
        else:
            yield scrapy.Request(
                method='POST', 
                callback=self.fetch_list,
                meta={'handle_httpstatus_all': True, 'offset': 0, 'hash': resources[0]['hash'], 'sk': json_obj['environment']['sk'], 'active_main_disk': response.meta['active_main_disk'], 'amd_name': resources[0]['name']}, 
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
                meta={'handle_httpstatus_all': True, 'offset': response.meta['offset'], 'hash': response.meta['hash'], 'sk':newSk, 'active_main_disk': response.meta['active_main_disk'], 'amd_name': response.meta['amd_name']}, 
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
                    self.handle_file(resource, response)
                else:
                    yield scrapy.Request(
                        method='POST', 
                        callback=self.fetch_list,
                        meta={'handle_httpstatus_all': True, 'offset': 0, 'hash': resource['path'], 'sk': response.meta['sk'], 'active_main_disk': response.meta['active_main_disk'] + '/' + resource['name'], 'amd_name': response.meta['amd_name']}, 
                        url='https://yadi.sk/public/api/fetch-list', 
                        body=urllib.parse.quote(json.dumps({'offset': 0, 'hash': resource['path'], 'sk': response.meta['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                        headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
                    )
            
            if is_completed is False:
                yield scrapy.Request(
                    method='POST', 
                    callback=self.fetch_list,
                    meta={'handle_httpstatus_all': True, 'offset': response.meta['offset'] + 20, 'hash': response.meta['hash'], 'sk': response.meta['sk'], 'active_main_disk': response.meta['active_main_disk'], 'amd_name': response.meta['amd_name']}, 
                    url='https://yadi.sk/public/api/fetch-list', 
                    body=urllib.parse.quote(json.dumps({'offset': response.meta['offset'] + 20, 'hash': response.meta['hash'], 'sk': response.meta['sk'], "withSizes":'true',"options":{"hasExperimentVideoWithoutPreview":'true'}})), 
                    headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
                )