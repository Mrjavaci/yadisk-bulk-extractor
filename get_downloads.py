import scrapy
import json
import mysql.connector
import urllib
import wget 
import os 
import threading
import requests

class getDownloadsSpider(scrapy.Spider):
    name = 'getDownloadsSpider'
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="scrapy"
    )
    
    last_cookie = None
    
    #mega = Mega()
    #m = mega.login("dejavuonthamic@gmail.com", "0F8&xmIuEevYvy7n5")

    #########################################################################################################

    def download(self, link, filelocation):
        r = requests.get(link, stream=True)
        with open(filelocation, 'wb') as f:
            for chunk in r.iter_content(1024):
                if chunk:
                    f.write(chunk)

    def createNewDownloadThread(self, link, filelocation):
        download_thread = threading.Thread(target=self.download, args=(link,filelocation))
        download_thread.start()

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
        cursor.execute("select * from yadisk where filename like '% - %' and (downloaded is null or downloaded = 0) and safe = 1 order by filename asc, length(filename) desc, filesize desc")
        rows = cursor.fetchall()
        for book in rows:
            yield scrapy.Request(url=book[6], callback=self.parse_url, meta={'handle_httpstatus_all': True, 'id': book[0], 'name': book[2], 'hash': book[7]})
    
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
                if not os.path.exists('./books/' + str(response.meta['name'][0]).lower()):
                    os.makedirs('./books/' + str(response.meta['name'][0]).lower())
                #folder = self.mega.find(str(response.meta['name'][0]).lower())
                #if folder is None:
                #    self.mega.create_folder(str(response.meta['name'][0]).lower())
                #    
                #folder = self.mega.find(str(response.meta['name'][0]).lower())
                file_url = res_body['data']['url']
                self.createNewDownloadThread(file_url, './books/' + str(response.meta['name'][0]).lower() + '/' + response.meta['name'])
                #testfile = urllib.request.urlretrieve(file_url, './books/' + str(response.meta['name'][0]).lower() + '/' + response.meta['name'])
                #wget.download(file_url, out='./books/' + str(response.meta['name'][0]).lower())
                #file = self.mega.upload(response.meta['name'], folder[0])
                #os.remove(response.meta['name'])
                cursor = self.conn.cursor()
                cursor.execute("UPDATE yadi SET downloaded = 1 WHERE id = '" + response.meta['id'] + "'")
                self.conn.commit()
                print(response.meta['name'] + " downloaded")
