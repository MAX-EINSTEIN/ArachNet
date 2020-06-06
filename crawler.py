import os, sys, sqlite3, ssl, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

class crawler():
    """Crawls over all or specified number of internal links on Website

    Functionality:
        Provides useful functions for crawling all internal links on a site
    """

    def __init__(self, url):
        self.starturl = url
        self.webs = list()
        self.connection = sqlite3.connect('db.sqlite')
        self.cursor = self.connection.cursor()
        self._init_database()

    def crawl(self):
        self._resume_or_reset()
        self._get_current_webs()

        many = 0
        while True:
            if many < 1:
                n = input('How many pages: ')
                if(len(n) < 1) : break
                many = int(n)
            many = many - 1
        
            self.cursor.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
            try:
                row = self.cursor.fetchone()
                from_id = row[0]
                url = row[1]
            except:
                print('No unretrieved HTML pages found')
                many = 0
                break

            print(from_id, url, end=' ')

            # If we are retrieving this page, there should be no links from it
            self.cursor.execute('DELETE from Links WHERE from_id=?', (from_id, ) )
            try:
                document = urlopen(url, context=self._get_ssl_context())

                html = document.read()

                if document.getcode() != 200:
                    print("Error on page {} : ".format(url), document.getcode())
                    self.cursor.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )
                
                if document.info().get_content_type() != 'text/html':
                    print("Ignore non text/html page")
                    self.cursor.execute('DELETE FROM Pages WHERE url=?', ( url, ) )
                    self.connection.commit()
                    continue

                print('(' + str(len(html)) + ')')

                soup = BeautifulSoup(html, "html.parser")
            except KeyboardInterrupt:
                print('')
                print('Program interrupted by user...')
                break
            except Exception as exp:
                print("Unable to retrieve or parse page")
                self.cursor.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
                self.connection.commit()
                print("Error: ", exp)
                continue

            self.cursor.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
            self.cursor.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ) )
            self.connection.commit()

            # Retrieve all of the anchor tags
            tags = soup('a')
            count = 0
            for tag in tags:
                href = tag.get('href', None)
                if ( href is None ) : continue
                # Resolve relative references like href="/contact"
                up = urlparse(href)
                if ( len(up.scheme) < 1 ) :
                    href = urljoin(url, href)
                ipos = href.find('#')
                if ( ipos > 1 ) : href = href[:ipos]
                if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
                if ( href.endswith('/') ) : href = href[:-1]
                if ( len(href) < 1 ) : continue

                # Check if the URL is in any of the webs
                found = False
                for web in self.webs:
                    if ( href.startswith(web) ) :
                        found = True
                        break
                if not found : continue

                self.cursor.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) )
                count = count + 1
                self.connection.commit()

                self.cursor.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
                try:
                    row = self.cursor.fetchone()
                    to_id = row[0]
                except:
                    print('Could not retrieve id')
                    continue
                self.cursor.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( from_id, to_id ) )

        self.connection.close()

    def _get_ssl_context(self):
        """Returns a SSL context ignoring certificate errors

        Returns:
            A list of words contained in the provided document
        """
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx


    def _init_database(self):
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS Pages
            (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
            error INTEGER, old_rank REAL, new_rank REAL);

            CREATE TABLE IF NOT EXISTS Links
            (from_id INTEGER, to_id INTEGER);

            CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE);
        ''')


    def _resume_or_reset(self):
        self.cursor.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
        row = self.cursor.fetchone()
        if row is not None:
            response = input('Continue crawling {} (Y/n): '.format(row[1]))
            if response.upper() != 'Y':
                print('Removing data for {} ...'.format(row[1]))
                os.remove('db.sqlite')
                sys.exit(0)
        else:
            if ( len(self.starturl) < 1 ) : self.starturl = 'https://en.wikipedia.org/'
            if ( self.starturl.endswith('/') ) : self.starturl = self.starturl[:-1]
            web = self.starturl
            if ( self.starturl.endswith('.htm') or self.starturl.endswith('.html') ) :
                pos = self.starturl.rfind('/')
                web = self.starturl[:pos]

            if ( len(web) > 1 ) :
                self.cursor.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
                self.cursor.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( self.starturl, ) )
                self.connection.commit()


    def _get_current_webs(self):
        self.cursor.execute('SELECT url FROM Webs')
        print('Websites to crawl:')
        rows = self.cursor.fetchall()
        for row in rows:
            address = str(row[0])
            self.webs.append(address)
            print('[^]', address)


if __name__ == "__main__":
    spider = crawler("https://learnopengl.com/")
    spider.crawl()