from book_manager import BookManager

bm = BookManager()

for book in bm.get_books():
    print(book.id)