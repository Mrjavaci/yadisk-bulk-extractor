from mega import Mega
import mysql.connector
import os
import concurrent.futures
import logging
import threading
import time

conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="scrapy"
)

def thread_function(url, path, filename):
    try:
        m.download_url(url, path, filename)
    except:
        pass

threads = list()

mega = Mega()
m = mega.login()

cursor = conn.cursor()
cursor.execute("SELECT 	*  FROM 	ekitapsite  WHERE 	author IN ( 		'Çetin Altan', 		'Ömer Hayyam', 		'Ömer Seyfettin', 		'Özdemir Asaf', 		'Ümit Yaşar Oğuzcan', 		'Üstün Dökmen', 		'İbn Sina', 		'İbni Haldun', 		'İhsan Oktay Anar', 		'İlber Ortaylı', 		'İmam Gazali', 		'İskender Pala', 		'İsmet Özel', 		'İsmet İnönü', 		'A. M. Celal Şengör', 		'Abdülkadir Geylani', 		'Abdullah Öcalan', 		'Adolf Hitler', 		'Agatha Christie', 		'Ahmet Ümit', 		'Ahmet Şerif İzgören', 		'Ahmet Altan', 		'Ahmet Arslan', 		'Ahmet Hamdi Tanpınar', 		'Ahmet Mithat Efendi', 		'Alain de Botton', 		'Albert Camus', 		'Albert Einstein', 		'Aldous Huxley', 		'Aleksandr Sergeyeviç Puşkin', 		'Alparslan Türkeş', 		'André Gide', 		'Andrey Tarkovski', 		'Antoine de Saint-Exupéry', 		'Anton Pavloviç Çehov', 		'Aristoteles', 		'Arthur Charles Clarke', 		'Arthur Schopenhauer', 		'Ataol Behramoğlu', 		'Attila İlhan', 		'Ayn Rand', 		'Aziz Nesin', 		'Bahriye Üçok', 		'Baruch Spinoza', 		'Bediüzzaman Said Nursi', 		'Behçet Necatigil', 		'Bernard Lewis', 		'Bertolt Brecht', 		'Bertrand Russell', 		'Cahit Sıtkı Tarancı', 		'Cahit Zarifoğlu', 		'Can Dündar', 		'Can Yücel', 		'Caner Taslaman', 		'Carl Gustav Jung', 		'Carl Sagan', 		'Cemal Süreya', 		'Cengiz Aytmatov', 		'Cesare Pavese', 		'Charles Baudelaire', 		'Charles Bukowski', 		'Charles Darwin', 		'Charles Dickens', 		'Chuck Palahniuk', 		'Claude Lévi-Strauss', 		'Dücane Cündioğlu', 		'Dan Brown', 		'Dante Alighieri', 		'Doğan Cüceloğlu', 		'Doğu Perinçek', 		'Douglas Adams', 		'Ece Ayhan', 		'Ece Temelkuran', 		'Edgar Allan Poe', 		'Edip Cansever', 		'Elif Şafak', 		'Emil Michel Cioran', 		'Emile Zola', 		'Emre Kongar', 		'Erhan Afyoncu', 		'Erich Fromm', 		'Ernest Hemingway', 		'Ernesto Che Guevara', 		'Erol Mütercimler', 		'Evliyâ Çelebi', 		'Falih Rıfkı Atay', 		'Farabi', 		'Ferhan Şensoy', 		'Fernando Pessoa', 		'Fidel Castro', 		'François Marie Arouet Voltaire', 		'Franz Kafka', 		'Friedrich Engels', 		'Friedrich Wilhelm Nietzsche', 		'Furuğ Ferruhzad', 		'Fyodor Mihailoviç Dostoyevski', 		'Gülten Dayıoğlu', 		'Gabriel García Márquez', 		'Gazi Mustafa Kemal Atatürk', 		'Georg Wilhelm Friedrich Hegel', 		'George Orwell', 		'George R. R. Martin', 		'Hüseyin Nihal Atsız', 		'Halide Edib Adıvar', 		'Halikarnas Balıkçısı', 		'Halil İnalcık', 		'Halil Cibran', 		'Haruki Murakami', 		'Hasan Ali Toptaş', 		'Henry Miller', 		'Homeros', 		'Honoré de Balzac', 		'Ian Fleming', 		'Immanuel Kant', 		'Isaac Asimov', 		'Ivan Sergeyeviç Turgenyev', 		'J. K. Rowling', 		'Jack London', 		'James Joyce', 		'Jean Baudrillard', 		'Jean Jacques Rousseau', 		'Jean-Paul Sartre', 		'Jiddu Krishnamurti', 		'Johann Wolfgang von Goethe', 		'John Ronald Reuel Tolkien', 		'John Steinbeck', 		'José Saramago', 		'Jose Mauro de Vasconcelos', 		'Josef Stalin', 		'Jules Verne', 		'Köroğlu', 		'Küçük İskender', 		'Karl Marks', 		'Karl Raimund Popper', 		'Kemal Tahir', 		'Leonardo Da Vinci', 		'Lev Davidoviç Troçki', 		'Lev Nikolayeviç Tolstoy', 		'Ludwig Wittgenstein', 		'Mümin Sekman', 		'Maksim Gorki', 		'Marcel Proust', 		'Marcus Aurelius Antoninus', 		'Marcus Tullius Cicero', 		'Mark Twain', 		'Marquis de Sade', 		'Martin Heidegger', 		'Max Weber', 		'Mehmet Fuad Köprülü', 		'Melih Cevdet Anday', 		'Mevlana Celaleddin Rumi', 		'Michel Foucault', 		'Michio Kaku', 		'Milan Kundera', 		'Mina Urgan', 		'Moliére', 		'Muazzez İlmiye Çığ', 		'Murat Bardakçı', 		'Murathan Mungan', 		'Nazım Hikmet', 		'Necib Mahfuz', 		'Necip Fazıl Kısakürek', 		'Nikolay Vasilyeviç Gogol', 		'Noam Chomsky', 		'Oğuz Atay', 		'Orhan Kemal', 		'Orhan Pamuk', 		'Orhan Veli Kanık', 		'Oruç Aruoba', 		'Oscar Wilde', 		'Pablo Neruda', 		'Paulo Coelho', 		'Peyami Safa', 		'Pierre Loti', 		'Platon', 		'Pyotr Alekseyeviç Kropotkin', 		'Rainer Maria Rilke', 		'Reşat Nuri Güntekin', 		'Rene Descartes', 		'Richard Bach', 		'Richard Dawkins', 		'Richard P. Feynman', 		'Rollo May', 		'Sâdık Hidâyet', 		'Søren Kierkegaard', 		'Sabahattin Ali', 		'Sabahattin Eyüboğlu', 		'Sait Faik Abasıyanık', 		'Samuel Beckett', 		'Sigmund Freud', 		'Simone de Beauvoir', 		'Slavoj Žižek', 		'Soner Yalçın', 		'Stefan Zweig', 		'Stephen King', 		'Stephen W. Hawking', 		'Sunay Akın', 		'Sylvia Plath', 		'Tezer Özlü', 		'Theodor W. Adorno', 		'Tomris Uyar', 		'Turan Dursun', 		'Turgut Özakman', 		'Turgut Uyar', 		'Uğur Mumcu', 		'Umberto Eco', 		'Ursula K. Le Guin', 		'Victor Hugo', 		'Virginia Woolf', 		'Vladimir İlyiç Ulyanov Lenin', 		'Wilhelm Reich', 		'William Faulkner', 		'William Shakespeare', 		'Yaşar Kemal', 		'Yakup Kadri Karaosmanoğlu', 		'Yalçın Küçük', 		'Yavuz Bahadıroğlu', 		'Yusuf Atılgan', 		'Yusuf Has Hacib', 		'Yuval Noah Harari', 		'Zülfü Livaneli', 	'Ziya Gökalp'  	) ORDER BY id desc")
rows = cursor.fetchall()

for row in rows:
    path = "Z:\E-Books" + "\\" + row[2]

    if not os.path.exists(path):
        os.makedirs(path)

    if not os.path.exists(path + "\\" + row[2] + " - " + row[1] + "." + row[10].lower()):
        try:
            m.download_url(row[9], path, row[2] + " - " + row[1] + "." + row[10].lower())
            #threads.append(threading.Thread(target=thread_function, args=(row[9], path, row[2] + " - " + row[1] + "." + row[10].lower(),)))
            #
            #if len(threads) == 10:
            #    for thread in threads:
            #        thread.start()
            #    threads = threads[10:]
            #    time.sleep(5)
        except:
            pass