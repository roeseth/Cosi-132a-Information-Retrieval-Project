import re
import requests
import aiohttp
from bs4 import BeautifulSoup


class imdbParser:
    baseUrl = "http://www.imdb.com/"
    titleUrl = baseUrl + "title/"
    searchUrl = baseUrl + "find?s=tt&ttype=ft&q="
    title = None
    id = ''
    info = {}
    soup = None;

    def __init__(self, title):
        self.title = title
        self.id = self.get_id_from_title(title)
        self.soup = self.scrap_site(self.titleUrl + self.id + "/")

    def has_match(self):
        return self.id != ''

    def get_director(self):
        try:
            self.info['director'] = self.soup.find('div', {'class': 'credit_summary_item'}).a.string
        except Exception:
            self.info['director'] = ''
        return self.info['director']

    def get_runtime(self):
        try:
            self.info['runtime'] = re.sub(r'\smin', '', self.soup.find('h4', string = 'Runtime:').parent.time.string)
        except Exception:
            self.info['runtime'] = ''
        return self.info['runtime']

    def get_language(self):
        try:
            self.info['language'] = self.soup.find('h4', string = 'Language:').parent.a.string
        except Exception:
            self.info['language'] = ''
        return self.info['language']

    def get_country(self):
        country = [];
        try:
            for tag in self.soup.find('h4', string = 'Country:').parent.find_all('a'):
                country.append(tag.string)
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

    def get_id_from_title(self, title):
        try:
            soup = self.scrap_site(self.searchUrl + title)
            movie = soup.find('td', {'class': 'result_text'}).a
            return movie['href'].split('/')[2]
        except Exception:
            print("Cant get data extracted")
            return ""

    @staticmethod
    def scrap_site(url):
        try:
            resp = requests.get(url)
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            print("Problem with the network connection")
