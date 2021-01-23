import scrapy
import json
import mysql.connector
import urllib
import wget 
import os 

from mega import Mega

class getDownloadsSpider(scrapy.Spider):
    name = 'getDownloadsSpider'
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="scrapy"
    )
    
    last_cookie = None
    
    mega = Mega()
    m = mega.login("dejavuonthamic@gmail.com", "0F8&xmIuEevYvy7n5")

    #########################################################################################################

    def get_cookie(self, response):
        try:
            self.last_cookie = response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")
        except:
            pass
        
        return self.last_cookie
    
    def start_requests(self):
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="root",
            database="scrapy"
        )

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM `scrapy`.`yadisk` WHERE `safe` = 1 AND `ext` = 'pdf' AND `filename` LIKE '% - %' AND `filename` ORDER BY filename ASC, length(filename) DESC, filesize DESC LIMIT 3500 OFFSET 0")
        rows = cursor.fetchall()
        for book in rows:
            yield scrapy.Request(url=book[7], callback=self.parse_url, meta={'handle_httpstatus_all': True, 'id': book[0], 'name': book[3], 'hash': book[8]})
    
    def parse_url(self, response):
        json_obj = json.loads(response.css("script#store-prefetch ::text").extract_first())
        sk = json_obj['environment']['sk']
        hash = response.meta['hash']
        
        yield scrapy.Request(
            method='POST', 
            callback=self.finalize,
            meta={'handle_httpstatus_all': True, "hash":hash, 'name': response.meta['name'], 'id': response.meta['id']}, 
            url='https://yadi.sk/public/api/download-url', 
            body=str(urllib.parse.quote(json.dumps({"hash":hash, "sk":sk, "options":{"hasExperimentVideoWithoutPreview":True}}))), 
            headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
        )
    
    def finalize(self, response):
        res_body = json.loads(response.body.decode('utf-8'))
        
        if res_body['error'] is True:
            yield scrapy.Request(
                method='POST',
                callback=self.finalize,
                meta={'handle_httpstatus_all': True, "hash":response.meta['hash'], 'name': response.meta['name'], 'id': response.meta['id']}, 
                url='https://yadi.sk/public/api/download-url', 
                body=str(urllib.parse.quote(json.dumps({"hash":response.meta['hash'], "sk":res_body['newSk'], "options":{"hasExperimentVideoWithoutPreview":True}}))), 
                headers={"Content-Type": "text/plain", "Cookies": self.get_cookie(response)}
            )
        else:
            try:
                folder = self.mega.find(str(response.meta['name'][0]).lower())
                if folder is None:
                    self.mega.create_folder(str(response.meta['name'][0]).lower())
                    
                folder = self.mega.find(str(response.meta['name'][0]).lower())
                file_url = res_body['data']['url']
                testfile = urllib.request.urlretrieve(file_url, response.meta['name'])
                file = self.mega.upload(response.meta['name'], folder[0])
                os.remove(response.meta['name'])
                cursor = self.conn.cursor()
                cursor.execute("UPDATE yadisk SET downloaded = 1 WHERE id = " + response.meta['id'])
                self.conn.commit()
                print(response.meta['name'] + " uploaded")
            except:
                pass