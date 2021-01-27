from book import Book
import mysql.connector
from typing import List
from datetime import datetime
import time

class BookManager:
    def __init__(self, options=None):
        self.conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="root",
            database="scrapy"
        )

        self.table_name = 'ekitapsite'

    def set_book(self, book)-> Book:
        b = Book()

        b.id = book[0]
        b.ekitap_id = book[1]
        b.name = book[2]
        b.parsed_name = book[3] 
        b.author = book[4] 
        b.publisher = book[5] 
        b.categories = book[6] 
        b.language = book[7] 
        b.year = book[8] 
        b.type = book[9] 
        b.pagesize = book[10] 
        b.filesize = book[11] 
        b.download_url = book[12]

        return b

    def get_books(self) -> List[Book]:
        start_time = datetime.now()
        print("__________\n")
        print("Query started: " + 'SELECT * FROM '+self.table_name+" WHERE type = 'PDF'")
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM '+self.table_name+" WHERE type = 'PDF'")
        result = cursor.fetchall()

        books = list()

        for book in result:
            #if file is pdf
            if book[9] == 'PDF':
                books.append(self.set_book(book))
        
        end_time = datetime.now()
        print('Query finished, duration: {}'.format(end_time - start_time))
        print("__________\n")
        time.sleep(2)

        return books