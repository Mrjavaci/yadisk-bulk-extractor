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
        for id in range(1,125500):
            yield scrapy.Request(url="https://ekitap.site/kitap?no=" + str(id), callback=self.parse_url, meta={'handle_httpstatus_all': True, 'id': id})
    
    def parse_url(self, response):
        try:
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

            book_name = book_name.replace('(MK)','')
            book_name = book_name.replace('(TR)','')

            book_obj = {
                'ekitap_id': str(response.meta['id']).strip() if response.meta['id'] is not None else None,
                'book_name': str(book_name).strip() if book_name is not None else None,
                'author_name': str(author_name).strip() if author_name is not None else None,
                'publisher': str(publisher).strip() if publisher is not None else None,
                'language': str(language).strip() if language is not None else None,
                'categories': str(",".join(categories)).strip() if categories is not None else None,
                'filesize': str(file_size).replace(',','.').strip() if file_size is not None else None,
                'pagesize': str(page_size).strip() if page_size is not None and '?' not in page_size else None,
                'year': str(year).strip() if year is not None else None,
                'type': str(file_ext).strip() if file_ext is not None else None,
                'download_url': str(download_url).strip() if download_url is not None else None
            }

            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO ekitapsite (ekitap_id,name,author,publisher,categories,filesize,pagesize,language,year,download_url,type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                (book_obj['ekitap_id'], book_obj['book_name'], book_obj['author_name'], book_obj['publisher'], book_obj['categories'], book_obj['filesize'], book_obj['pagesize'], book_obj['language'], book_obj['year'], book_obj['download_url'], book_obj['type'])
            )
            self.conn.commit()
            print(book_obj['ekitap_id'] + ": "+ book_obj['book_name'] + ": eklendi.")
        except:
            with open(".//errors.txt", 'a') as f:
                f.write(response.meta['id'] + "\n")