from book import Book
import json
import re
from mega import Mega
from book_manager import BookManager
from typing import List

m = Mega()
mega = m.login()
mega.download_url('https://mega.nz/file/4p4nUKza#P1kVvSYp-luhtsZVJwTVa9bIu_WWN35FDLVLvNWqn-8')
exit()

############################## Finished ##############################
def check_dates_between_hyphens_in_title(bm: BookManager, book: Book):
    if re.match('\((\d+)(?=-)-(?<=-)(\d+)\)', book.name, flags=re.UNICODE | re.IGNORECASE):
        #matches with only first result, maybe it'll updated with multiple match support
        dates = list(re.findall('\((\d+)(?=-)-(?<=-)(\d+)\)', book.name, flags=re.UNICODE | re.IGNORECASE)[0])
        dates_string = '('+dates[0]+'-'+dates[1]+')'
        
        if book.year == dates_string:
            if book.parsed_name is not None:
                book.parsed_name = book.parsed_name.replace(dates_string, '').strip()
            else:
                book.parsed_name = book.name.replace(dates_string, '').strip()

        if bm.update_book(book) is True:
            print(str(book.id) + ": Updated successfully.")
        else:
            print(str(book.id) + ": Something went wrong.")

def check_between_paranthesises(bm: BookManager, books: List[Book]):
    found = []

    for book in books:
        if len(re.findall('\(.*?\)', book.name)) > 0:
            phrases = re.findall('\(.*?\)', book.name)

            for phrase in phrases:
                if phrase not in found:
                    found.append(phrase)
    
    print(found)


bm = BookManager()
check_between_paranthesises(bm, bm.get_books())