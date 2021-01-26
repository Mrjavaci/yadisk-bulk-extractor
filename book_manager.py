from book import Book
import mysql.connector
from typing import List

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
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM '+self.table_name+' LIMIT 1')
        result = cursor.fetchall()

        books = list()

        for book in result:
            books.append(self.set_book(book))
        
        return books