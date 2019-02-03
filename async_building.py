import aiohttp
import asyncio
import time
from bs4 import BeautifulSoup
from urllib.request import urljoin
import re
import multiprocessing as mp
import wikipediaapi
import wptools
import re
import json
from imdbparser import imdbParser
import requests

fetched = set()
unfetched = set()
base_url = "http://www.imdb.com/"
title_url = base_url + "title/"
search_url = base_url + "find?s=tt&ttype=ft&q="
json_data = {}


def crawl(p):
    title = p.title
    entry_name_no_film = re.sub(r' \(film\)| \(2018 film\)', '', title)
    id_html = scrap_site(search_url + entry_name_no_film)
    soup = BeautifulSoup(id_html, "lxml")
    movie = soup.find('td', {'class': 'result_text'}).a
    id = movie['href'].split('/')[2]

    html = scrap_site(title_url + id + "/")
    return p, html

    # r = await session.get(url)
    # html = await r.text()
    # await asyncio.sleep(0.1)  # slightly delay for downloading
    # return html


def scrap_site(url):
    try:
        resp = requests.get(url)
        html = resp.text()
        return html
    except Exception:
        print("Problem with the network connection")
        return None


async def main(loop):
    pool = mp.Pool(2)  # slightly affected
    async with aiohttp.ClientSession() as session:
        count = 1

        while len(unfetched) != 0:
            # tasks = [loop.create_task(crawl(p, session)) for p in unfetched]
            # done, pending = await asyncio.wait(tasks)
            # htmls = [f.result() for f in done]

            crawl_jobs = [pool.apply_async(crawl, args = (p,)) for p in unfetched]
            htmls = [j.get() for j in crawl_jobs]

            parse_jobs = [pool.apply_async(parse_entry, args = (p, html,)) for p, html in htmls]
            results = [j.get() for j in parse_jobs]

            fetched.update(unfetched)
            unfetched.clear()
            for info in results:
                json_data[count] = info
                print(info)
                count += 1


# def parse(html):
#     soup = BeautifulSoup(html, 'lxml')
#     urls = soup.find_all('a', {"href": re.compile('^/.+?/$')})
#     title = soup.find('h1').get_text().strip()
#     page_urls = set([urljoin(base_url, url['href']) for url in urls])
#     url = soup.find('meta', {'property': "og:url"})['content']
#     return title, page_urls, url

def parse_entry(p, html):
    imdb = imdbParser(p.title, html)
    info = {}
    if imdb.has_match():
        info['title'] = imdb.title
        info['director'] = imdb.get_director()
        info['starring'] = imdb.get_cast_list()
        info['running time'] = imdb.get_runtime()
        info['country'] = imdb.get_country()
        info['language'] = imdb.get_language()
        info['time'] = ''
        info['location'] = ''
    else:
        p2 = wptools.page(p.title)
        p2.get_parse()
        box = p2.data['infobox']
        if box == None: box = {}
        info['title'] = imdb.title
        info['director'] = parse_director(box['director']) if 'director' in box else ''
        info['starring'] = parse_sublist(box['starring']) if 'starring' in box else []
        info['running time'] = parse_minutes(box['runtime']) if 'runtime' in box else ''
        info['country'] = parse_sublist(box['country']) if 'country' in box else []
        info['language'] = box['language'] if 'language' in box else ''
        info['time'] = ''
        info['location'] = ''

    info['text'] = p.text
    cats = []
    for k in p.categories.keys():
        if k.startswith('Use'): continue
        cats.append(k[9:])
    info['categories'] = cats

    return info


def parse_sublist(str):
    return re.findall(r'\w{1,}\.?\s\w{1,}\.?\s\w{1,}\.?|\w{1,}\.?\s\w{1,}\.?|\w{4,}',
                      re.sub(r'Unbullted list|Plainlist|plainlist', '', str))


def parse_director(str):
    return re.split('<br>|\|',
                    str.replace('[[', '').replace(']]', '').replace('{{', '').replace(
                        '}}', ''))[0]


def parse_minutes(str):
    return int(re.match(r'\d+', str).group())


if __name__ == "__main__":
    t1 = time.time()

    wiki = wikipediaapi.Wikipedia('en')
    cat = wiki.page("Category:2018 films")
    unfetched = [wiki.page(p) for p in cat.categorymembers]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()

    # with open('data.json', 'w') as f:
    #     json.dump(json_data, f)

    print("Async total time: ", time.time() - t1)
