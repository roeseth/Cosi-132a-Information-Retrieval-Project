import re
from bs4 import BeautifulSoup


class imdbParser:
    baseUrl = "http://www.imdb.com/"
    titleUrl = baseUrl + "title/"
    searchUrl = baseUrl + "find?s=tt&ttype=ft&q="
    title = None
    soup = None
    info = {}

    def __init__(self, title, html):
        self.title = title
        if not html == None:
            self.soup = BeautifulSoup(html, 'lxml')

    def has_match(self):
        return self.soup != None

    def get_director(self):
        try:
            self.info['director'] = str(self.soup.find('div', {'class': 'credit_summary_item'}).a.string)
        except Exception:
            self.info['director'] = ''
        return self.info['director']

    def get_runtime(self):
        try:
            self.info['runtime'] = re.sub(r'\smin', '', str(self.soup.find('h4', string = 'Runtime:').parent.time.string))
        except Exception:
            self.info['runtime'] = ''
        return self.info['runtime']

    def get_language(self):
        try:
            self.info['language'] = str(self.soup.find('h4', string = 'Language:').parent.a.string)
        except Exception:
            self.info['language'] = ''
        return self.info['language']

    def get_country(self):
        country = [];
        try:
            for tag in self.soup.find('h4', string = 'Country:').parent.find_all('a'):
                country.append(str(tag.string))
            self.info['country'] = country
        except Exception:
            self.info['country'] = []
        return self.info['country']

    def get_cast_list(self):
        cast_list = []
        try:
            for tr in self.soup.find('table', {'class': 'cast_list'}).find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) > 2:
                    cast_list.append(re.sub(r'[\t\r\n]', '', "".join(tds[1].find_all(text = True))).strip())
            self.info['cast list'] = cast_list
        except Exception:
            self.info['cast list'] = []
        return self.info['cast list']
