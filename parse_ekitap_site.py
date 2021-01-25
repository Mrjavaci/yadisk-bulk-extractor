import scrapy
import json
import mysql.connector
import urllib.parse
import time
import re
class ekitapsiteParser(scrapy.Spider):
    name = 'ekitapsite'
    
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="scrapy"
    )
    
    #########################################################################################################

    def start_requests(self):
        for id in range(1,125485):
            yield scrapy.Request(url="https://ekitap.site/kitap?no=" + str(id), callback=self.parse_url, meta={'handle_httpstatus_all': True})
    
    def parse_url(self, response):
        author_name = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[2]/a/text()").get()
        book_name = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[5]/text()").get()
        language = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[8]/a/text()").get()
        categories = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[@class='kitap_bilgi']/a/text()").getall()
        file_info = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[12]/text()").get()
        file_ext = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[12]/a/text()").get()
        year = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[14]/text()").get()
        publisher = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[14]/a/text()").get()
        download_url = response.xpath("//html/body/div[@class='container']/div[@class='topla'][2]/div[1]/div/div[1]/div[@id='kitap_detay']/p[16]/a/@href").get()
        page_size = file_info.split(';')[1].strip().replace('sayfa','').strip()
        file_size = file_info.split(';')[2].strip().replace('(','').replace(')','').replace('MB','').strip()

        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO ekitapsite (name,author,publisher,categories,filesize,pagesize,language,year,download_url,type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (book_name, author_name, publisher, ",".join(categories),str(file_size).replace(',','.'),page_size,language,year,download_url,file_ext)
        )
        self.conn.commit()
        print(book_name + ": eklendi...")