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
        #query = 'SELECT * FROM '+self.table_name+" WHERE type = 'PDF'"
        query = "SELECT a.*, t.count FROM ekitapsite a LEFT JOIN ( SELECT count( author ) AS COUNT, author FROM ekitapsite WHERE type = 'PDF' AND author != 'altKitap' AND author != 'Anonim' AND author != 'Anonymous' AND author != 'Belirtilmemiş' AND author != 'Bilinmiyor' AND author != 'Collective' AND author != 'esseGesse' AND author != 'Kolektif' AND author != 'Unknown' AND author != 'yazar belirtilmemiş' AND author IN ( 'Çetin Altan', 'Ömer Hayyam', 'Ömer Seyfettin', 'Özdemir Asaf', 'Ümit Yaşar Oğuzcan', 'Üstün Dökmen', 'İbn Sina', 'İbni Haldun', 'İhsan Oktay Anar', 'İlber Ortaylı', 'İmam Gazali', 'İskender Pala', 'İsmet Özel', 'İsmet İnönü', 'A. M. Celal Şengör', 'Abdülkadir Geylani', 'Abdullah Öcalan', 'Adolf Hitler', 'Agatha Christie', 'Ahmet Ümit', 'Ahmet Şerif İzgören', 'Ahmet Altan', 'Ahmet Arslan', 'Ahmet Hamdi Tanpınar', 'Ahmet Mithat Efendi', 'Alain de Botton', 'Albert Camus', 'Albert Einstein', 'Aldous Huxley', 'Aleksandr Sergeyeviç Puşkin', 'Alparslan Türkeş', 'André Gide', 'Andrey Tarkovski', 'Antoine de Saint-Exupéry', 'Anton Pavloviç Çehov', 'Aristoteles', 'Arthur Charles Clarke', 'Arthur Schopenhauer', 'Ataol Behramoğlu', 'Attila İlhan', 'Ayn Rand', 'Aziz Nesin', 'Bahriye Üçok', 'Baruch Spinoza', 'Bediüzzaman Said Nursi', 'Behçet Necatigil', 'Bernard Lewis', 'Bertolt Brecht', 'Bertrand Russell', 'Cahit Sıtkı Tarancı', 'Cahit Zarifoğlu', 'Can Dündar', 'Can Yücel', 'Caner Taslaman', 'Carl Gustav Jung', 'Carl Sagan', 'Cemal Süreya', 'Cengiz Aytmatov', 'Cesare Pavese', 'Charles Baudelaire', 'Charles Bukowski', 'Charles Darwin', 'Charles Dickens', 'Chuck Palahniuk', 'Claude Lévi-Strauss', 'Dücane Cündioğlu', 'Dan Brown', 'Dante Alighieri', 'Doğan Cüceloğlu', 'Doğu Perinçek', 'Douglas Adams', 'Ece Ayhan', 'Ece Temelkuran', 'Edgar Allan Poe', 'Edip Cansever', 'Elif Şafak', 'Emil Michel Cioran', 'Emile Zola', 'Emre Kongar', 'Erhan Afyoncu', 'Erich Fromm', 'Ernest Hemingway', 'Ernesto Che Guevara', 'Erol Mütercimler', 'Evliyâ Çelebi', 'Falih Rıfkı Atay', 'Farabi', 'Ferhan Şensoy', 'Fernando Pessoa', 'Fidel Castro', 'François Marie Arouet Voltaire', 'Franz Kafka', 'Friedrich Engels', 'Friedrich Wilhelm Nietzsche', 'Furuğ Ferruhzad', 'Fyodor Mihailoviç Dostoyevski', 'Gülten Dayıoğlu', 'Gabriel García Márquez', 'Gazi Mustafa Kemal Atatürk', 'Georg Wilhelm Friedrich Hegel', 'George Orwell', 'George R. R. Martin', 'Hüseyin Nihal Atsız', 'Halide Edib Adıvar', 'Halikarnas Balıkçısı', 'Halil İnalcık', 'Halil Cibran', 'Haruki Murakami', 'Hasan Ali Toptaş', 'Henry Miller', 'Homeros', 'Honoré de Balzac', 'Ian Fleming', 'Immanuel Kant', 'Isaac Asimov', 'Ivan Sergeyeviç Turgenyev', 'J. K. Rowling', 'Jack London', 'James Joyce', 'Jean Baudrillard', 'Jean Jacques Rousseau', 'Jean-Paul Sartre', 'Jiddu Krishnamurti', 'Johann Wolfgang von Goethe', 'John Ronald Reuel Tolkien', 'John Steinbeck', 'José Saramago', 'Jose Mauro de Vasconcelos', 'Josef Stalin', 'Jules Verne', 'Köroğlu', 'Küçük İskender', 'Karl Marks', 'Karl Raimund Popper', 'Kemal Tahir', 'Leonardo Da Vinci', 'Lev Davidoviç Troçki', 'Lev Nikolayeviç Tolstoy', 'Ludwig Wittgenstein', 'Mümin Sekman', 'Maksim Gorki', 'Marcel Proust', 'Marcus Aurelius Antoninus', 'Marcus Tullius Cicero', 'Mark Twain', 'Marquis de Sade', 'Martin Heidegger', 'Max Weber', 'Mehmet Fuad Köprülü', 'Melih Cevdet Anday', 'Mevlana Celaleddin Rumi', 'Michel Foucault', 'Michio Kaku', 'Milan Kundera', 'Mina Urgan', 'Moliére', 'Muazzez İlmiye Çığ', 'Murat Bardakçı', 'Murathan Mungan', 'Nazım Hikmet', 'Necib Mahfuz', 'Necip Fazıl Kısakürek', 'Nikolay Vasilyeviç Gogol', 'Noam Chomsky', 'Oğuz Atay', 'Orhan Kemal', 'Orhan Pamuk', 'Orhan Veli Kanık', 'Oruç Aruoba', 'Oscar Wilde', 'Pablo Neruda', 'Paulo Coelho', 'Peyami Safa', 'Pierre Loti', 'Platon', 'Pyotr Alekseyeviç Kropotkin', 'Rainer Maria Rilke', 'Reşat Nuri Güntekin', 'Rene Descartes', 'Richard Bach', 'Richard Dawkins', 'Richard P. Feynman', 'Rollo May', 'Sâdık Hidâyet', 'Søren Kierkegaard', 'Sabahattin Ali', 'Sabahattin Eyüboğlu', 'Sait Faik Abasıyanık', 'Samuel Beckett', 'Sigmund Freud', 'Simone de Beauvoir', 'Slavoj Žižek', 'Soner Yalçın', 'Stefan Zweig', 'Stephen King', 'Stephen W. Hawking', 'Sunay Akın', 'Sylvia Plath', 'Tezer Özlü', 'Theodor W. Adorno', 'Tomris Uyar', 'Turan Dursun', 'Turgut Özakman', 'Turgut Uyar', 'Uğur Mumcu', 'Umberto Eco', 'Ursula K. Le Guin', 'Victor Hugo', 'Virginia Woolf', 'Vladimir İlyiç Ulyanov Lenin', 'Wilhelm Reich', 'William Faulkner', 'William Shakespeare', 'Yaşar Kemal', 'Yakup Kadri Karaosmanoğlu', 'Yalçın Küçük', 'Yavuz Bahadıroğlu', 'Yusuf Atılgan', 'Yusuf Has Hacib', 'Yuval Noah Harari', 'Zülfü Livaneli', 'Ziya Gökalp' ) GROUP BY AUTHOR ) t ON t.author = a.author WHERE a.type = 'PDF' AND a.author != 'altKitap' AND a.author != 'Anonim' AND a.author != 'Anonymous' AND a.author != 'Belirtilmemiş' AND a.author != 'Bilinmiyor' AND a.author != 'Collective' AND a.author != 'esseGesse' AND a.author != 'Kolektif' AND a.author != 'Unknown' AND a.author != 'yazar belirtilmemiş' AND a.author IN ( 'Çetin Altan', 'Ömer Hayyam', 'Ömer Seyfettin', 'Özdemir Asaf', 'Ümit Yaşar Oğuzcan', 'Üstün Dökmen', 'İbn Sina', 'İbni Haldun', 'İhsan Oktay Anar', 'İlber Ortaylı', 'İmam Gazali', 'İskender Pala', 'İsmet Özel', 'İsmet İnönü', 'A. M. Celal Şengör', 'Abdülkadir Geylani', 'Abdullah Öcalan', 'Adolf Hitler', 'Agatha Christie', 'Ahmet Ümit', 'Ahmet Şerif İzgören', 'Ahmet Altan', 'Ahmet Arslan', 'Ahmet Hamdi Tanpınar', 'Ahmet Mithat Efendi', 'Alain de Botton', 'Albert Camus', 'Albert Einstein', 'Aldous Huxley', 'Aleksandr Sergeyeviç Puşkin', 'Alparslan Türkeş', 'André Gide', 'Andrey Tarkovski', 'Antoine de Saint-Exupéry', 'Anton Pavloviç Çehov', 'Aristoteles', 'Arthur Charles Clarke', 'Arthur Schopenhauer', 'Ataol Behramoğlu', 'Attila İlhan', 'Ayn Rand', 'Aziz Nesin', 'Bahriye Üçok', 'Baruch Spinoza', 'Bediüzzaman Said Nursi', 'Behçet Necatigil', 'Bernard Lewis', 'Bertolt Brecht', 'Bertrand Russell', 'Cahit Sıtkı Tarancı', 'Cahit Zarifoğlu', 'Can Dündar', 'Can Yücel', 'Caner Taslaman', 'Carl Gustav Jung', 'Carl Sagan', 'Cemal Süreya', 'Cengiz Aytmatov', 'Cesare Pavese', 'Charles Baudelaire', 'Charles Bukowski', 'Charles Darwin', 'Charles Dickens', 'Chuck Palahniuk', 'Claude Lévi-Strauss', 'Dücane Cündioğlu', 'Dan Brown', 'Dante Alighieri', 'Doğan Cüceloğlu', 'Doğu Perinçek', 'Douglas Adams', 'Ece Ayhan', 'Ece Temelkuran', 'Edgar Allan Poe', 'Edip Cansever', 'Elif Şafak', 'Emil Michel Cioran', 'Emile Zola', 'Emre Kongar', 'Erhan Afyoncu', 'Erich Fromm', 'Ernest Hemingway', 'Ernesto Che Guevara', 'Erol Mütercimler', 'Evliyâ Çelebi', 'Falih Rıfkı Atay', 'Farabi', 'Ferhan Şensoy', 'Fernando Pessoa', 'Fidel Castro', 'François Marie Arouet Voltaire', 'Franz Kafka', 'Friedrich Engels', 'Friedrich Wilhelm Nietzsche', 'Furuğ Ferruhzad', 'Fyodor Mihailoviç Dostoyevski', 'Gülten Dayıoğlu', 'Gabriel García Márquez', 'Gazi Mustafa Kemal Atatürk', 'Georg Wilhelm Friedrich Hegel', 'George Orwell', 'George R. R. Martin', 'Hüseyin Nihal Atsız', 'Halide Edib Adıvar', 'Halikarnas Balıkçısı', 'Halil İnalcık', 'Halil Cibran', 'Haruki Murakami', 'Hasan Ali Toptaş', 'Henry Miller', 'Homeros', 'Honoré de Balzac', 'Ian Fleming', 'Immanuel Kant', 'Isaac Asimov', 'Ivan Sergeyeviç Turgenyev', 'J. K. Rowling', 'Jack London', 'James Joyce', 'Jean Baudrillard', 'Jean Jacques Rousseau', 'Jean-Paul Sartre', 'Jiddu Krishnamurti', 'Johann Wolfgang von Goethe', 'John Ronald Reuel Tolkien', 'John Steinbeck', 'José Saramago', 'Jose Mauro de Vasconcelos', 'Josef Stalin', 'Jules Verne', 'Köroğlu', 'Küçük İskender', 'Karl Marks', 'Karl Raimund Popper', 'Kemal Tahir', 'Leonardo Da Vinci', 'Lev Davidoviç Troçki', 'Lev Nikolayeviç Tolstoy', 'Ludwig Wittgenstein', 'Mümin Sekman', 'Maksim Gorki', 'Marcel Proust', 'Marcus Aurelius Antoninus', 'Marcus Tullius Cicero', 'Mark Twain', 'Marquis de Sade', 'Martin Heidegger', 'Max Weber', 'Mehmet Fuad Köprülü', 'Melih Cevdet Anday', 'Mevlana Celaleddin Rumi', 'Michel Foucault', 'Michio Kaku', 'Milan Kundera', 'Mina Urgan', 'Moliére', 'Muazzez İlmiye Çığ', 'Murat Bardakçı', 'Murathan Mungan', 'Nazım Hikmet', 'Necib Mahfuz', 'Necip Fazıl Kısakürek', 'Nikolay Vasilyeviç Gogol', 'Noam Chomsky', 'Oğuz Atay', 'Orhan Kemal', 'Orhan Pamuk', 'Orhan Veli Kanık', 'Oruç Aruoba', 'Oscar Wilde', 'Pablo Neruda', 'Paulo Coelho', 'Peyami Safa', 'Pierre Loti', 'Platon', 'Pyotr Alekseyeviç Kropotkin', 'Rainer Maria Rilke', 'Reşat Nuri Güntekin', 'Rene Descartes', 'Richard Bach', 'Richard Dawkins', 'Richard P. Feynman', 'Rollo May', 'Sâdık Hidâyet', 'Søren Kierkegaard', 'Sabahattin Ali', 'Sabahattin Eyüboğlu', 'Sait Faik Abasıyanık', 'Samuel Beckett', 'Sigmund Freud', 'Simone de Beauvoir', 'Slavoj Žižek', 'Soner Yalçın', 'Stefan Zweig', 'Stephen King', 'Stephen W. Hawking', 'Sunay Akın', 'Sylvia Plath', 'Tezer Özlü', 'Theodor W. Adorno', 'Tomris Uyar', 'Turan Dursun', 'Turgut Özakman', 'Turgut Uyar', 'Uğur Mumcu', 'Umberto Eco', 'Ursula K. Le Guin', 'Victor Hugo', 'Virginia Woolf', 'Vladimir İlyiç Ulyanov Lenin', 'Wilhelm Reich', 'William Faulkner', 'William Shakespeare', 'Yaşar Kemal', 'Yakup Kadri Karaosmanoğlu', 'Yalçın Küçük', 'Yavuz Bahadıroğlu', 'Yusuf Atılgan', 'Yusuf Has Hacib', 'Yuval Noah Harari', 'Zülfü Livaneli', 'Ziya Gökalp' ) ORDER BY t.COUNT DESC, a.AUTHOR ASC, a.filesize DESC, a.publisher ASC"
        print("__________\n")
        print("Query started: " + query)
        
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        books = list()

        for book in result:
            books.append(self.set_book(book))
        
        end_time = datetime.now()
        print('Query finished, duration: {}'.format(end_time - start_time))
        print("__________\n")

        return books

    def update_book(self, book: Book) -> bool:
        query = "UPDATE "+self.table_name+" SET "

        query = query + "name = %s, ekitap_id = %s, "
        query = query + "parsed_name = %s, year = %s, "
        query = query + "author = %s, categories = %s, "
        query = query + "publisher = %s, language = %s, "
        query = query + "pagesize = %s, filesize = %s, "
        query = query + "download_url = %s, type = %s "

        query = query + "WHERE id = %s"
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (
                book.name, book.ekitap_id,
                book.parsed_name, book.year,
                book.author, book.categories,
                book.publisher, book.language,
                book.pagesize, book.filesize,
                book.download_url, book.type,
                book.id
            ))
            
            self.conn.commit()
            
            return True
        except:
            return False