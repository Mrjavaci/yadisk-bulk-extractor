import html
import json
from mega import Mega
from book_manager import BookManager

mega = Mega()
m = mega.login()

m.import_public_url('https://mega.nz/file/41tyWZCZ#fBwJBhXPaSGme0IsrMxdyxblvoueNAeYjHeVhmzO6hE')

files = m.get_files().items()

for file in files:
    filetype = list(file[1].items())[3][1]
    org_filename = list(list(file[1].items())[4][1].values())[0]
    filename = org_filename.encode('iso-8859-1').decode('utf-8')

    #if item is file:
    if filetype == 0:
        file = m.find(org_filename)
        #put temp_output_file.close() on 746th row on mega.py file for fixing perm. issue
        path = m.download(file, dest_path='', dest_filename=filename)
        #download_url can be used too until perm. issue fixed
        print(path)

#bm = BookManager()
#
#for book in bm.get_books():
#    print(book.download_url)