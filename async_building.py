import aiohttp
import asyncio
import time
from bs4 import BeautifulSoup
import multiprocessing as mp
import wikipediaapi
import wptools
import re
import json
from async_imdb_parser import imdbParser
import sys
import logging

logging.basicConfig(filename='logger.log', level=logging.ERROR)
#sys.setrecursionlimit(10000)
base_url = "http://www.imdb.com/"
title_url = base_url + "title/"
search_url = base_url + "find?s=tt&ttype=ft&q="


async def crawl_id(p, session):
    print("crawling id: " + p.title)
    entry_name_no_film = re.sub(r'\s\(.*film\)', '', p.title)
    print(p.title + ':' + entry_name_no_film)
    html = await scrap_site(search_url + entry_name_no_film, session)
    print('returned id html')
    return p, html


def parse_id(p, html):
    print("parsing id: " + p.title)
    try:
        soup = BeautifulSoup(html, 'lxml')
        movie = soup.find('td', {'class': 'result_text'}).a
        id = movie['href'].split('/')[2]
        return p, id
    except Exception:
        logging.error("parsing id ERROR: " + p.title)
        return p, ''


async def crawl_entry(p, id, session):
    print("crawling entry: " + p.title)
    html = None
    if id != '':
        html = await scrap_site(title_url + id + "/", session)
        print('returned id html')
    return p, html


async def scrap_site(url, session):
    print("scraping url: " + url)
    try:
        resp = await session.get(url)
        html = await resp.text()
        print("scraping url done: " + url)
        return html
    except Exception:
        logging.error("scraping url ERROR: " + url)
        return None


def parse_entry(p, html):
    print("parsing entry: " + p.title)
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


async def main(loop, list, json_data):
    pool = mp.Pool(2)  # slightly affected

    results_id = set()
    unfetched = list
    fetched = set()
    async with aiohttp.ClientSession() as session:

        while len(unfetched) != 0:
            tasks = [loop.create_task(crawl_id(p, session,)) for p in unfetched]
            done, pending = await asyncio.wait(tasks, timeout = 10)
            htmls = [d.result() for d in done]

            parse_id_jobs = [pool.apply_async(parse_id, args = (p, html,)) for p, html in htmls]
            results_id = [j.get() for j in parse_id_jobs]

            fetched.update(unfetched)
            unfetched.clear()
    print('finished fetching ids')
    unfetched = results_id
    fetched.clear()
    async with aiohttp.ClientSession() as session:
        count = 1
        while len(unfetched) != 0:

            tasks = [loop.create_task(crawl_entry(p, id, session)) for p, id in unfetched]
            done, pending = await asyncio.wait(tasks)
            htmls = [d.result() for d in done]

            parse_jobs = [pool.apply_async(parse_entry, args = (p, html,)) for p, html in htmls]
            results = [j.get() for j in parse_jobs]

            fetched.update(unfetched)
            unfetched.clear()
            for info in results:
                json_data[count] = info
                print(info)
                count += 1


if __name__ == "__main__":
    t1 = time.time()

    wiki = wikipediaapi.Wikipedia('en')
    cat = wiki.page("Category:2018 films")
    cat_pages = [wiki.page(p) for p in cat.categorymembers]
    json_data = {}
    list = set()
    for p in cat.categorymembers:
        page = wiki.page(p)
        if not page.title.startswith('Category:'):
            list.add(page)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, list, json_data))
    loop.close()

    # with open('data.json', 'w') as f:
    #     json.dump(json_data, f)

    print("Async total time: ", time.time() - t1)
