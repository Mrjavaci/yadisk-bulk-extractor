from book import Book
import json
import re
from mega import Mega
from book_manager import BookManager

def check_dates_between_hyphens_in_title(book: Book):
    if re.match('\((\d+)(?=-)-(?<=-)(\d+)\)', book.name, flags=re.UNICODE | re.UNICODE):
        dates = re.findall('\((\d+)(?=-)-(?<=-)(\d+)\)', book.name, flags=re.UNICODE | re.UNICODE)
        
        print(list(dates))

bm = BookManager()
books = bm.get_books()
for book in books:
    check_dates_between_hyphens_in_title(book)