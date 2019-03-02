import aiohttp
import asyncio
import time
from bs4 import BeautifulSoup
import multiprocessing as mp
import wikipediaapi
import re
import json
import logging
from wpparser import wpParser
from imdbparser import imdbParser
import sys

# Log error message to file
logging.basicConfig(filename = 'logger_building_corpus.log', filemode = 'w', level = logging.ERROR)
# IMDB constant
BASE_URL = "http://www.imdb.com/"
TITLE_URL = BASE_URL + "title/"
SEARCH_URL = BASE_URL + "find?s=tt&ttype=ft&q="

MP_CORE = 6  # Core no. for multiprocessing
TIME_OUT = 10  # for async aiohttp task time out
WP_ONLY = False  # for Mode using Wikipedia only
LIMIT = False


def de_film(str):
    """ To get a cleaner film title

    :param str: the original title string
    :return: film title without " (2018 film)" or likewise
    """

    return re.sub(r'\s\(.*[fF]ilm\)', '', str)


async def crawl_id(p, session):
    """ Asynchronous method to get the HTML document of the search page of the movie on IMDB

    :param p: the tuple (wiki.page, index)
    :param session: asynchronous aiohttp client session
    :return: the tuple intact and the html document
    """

    title_no_film = de_film(p[0].title)
    html = await scrap_site(SEARCH_URL + title_no_film, session)
    # For console debug
    # print('(STAGE 1) Crawling id: ' + p[0].title + '...DONE')
    return p, html


def parse_id(p, html):
    """ To get IMDB id from the search page HTML document

    :param p: the tuple (wiki.page, index)
    :param html: the HTML document of the search page
    :return: the tuple p intact and IMDB id, if not found, return ''
    """

    try:
        soup = BeautifulSoup(html, 'lxml')
        movie = soup.find('td', {'class': 'result_text'}).a
        id = movie['href'].split('/')[2]
        # For console debug
        # print('(STAGE 2) Parsing id: ' + p[0].title + '...DONE')
        return p, id
    except Exception:
        logging.error('(STAGE 2) Parsing ID ERROR: ' + p[0].title)
        return p, ''


async def crawl_entry(p, id, session):
    """ Asynchronous method to get the movie info page

    :param p: the tuple (wiki.page, index)
    :param id: IMDB id
    :param session: asynchronous aiohttp client session
    :return: the tuple p intact and the HTML document of the movie
    """

    html = None
    # If can find movie on IMDB
    if id != '':
        html = await scrap_site(TITLE_URL + id + "/", session)
    # print('(STAGE 3) Crawling entry: ' + p[0].title + '...DONE')
    return p, html


def parse_entry(p, html):
    """ To parse the information needed from IMDB

    It will try bs4 IMDB page first for all available information. Typically, IMDB has more completed
    info than Wikipedia. If a movie does not exist on IMDB, it will use wptools to extract information
    from Wikipedia

    :param p: the tuple (wiki.page, index)
    :param html: HTML document of the movie
    :return: dictionary containing all info parsed
    """

    title_no_film = de_film(p[0].title)
    # Initialize imdbParser object
    info = {'title': title_no_film}
    if not WP_ONLY:
        imdb = imdbParser(title_no_film, html)
        if imdb.has_match():
            info['director'] = imdb.get_director()
            info['starring'] = imdb.get_cast_list()
            info['running time'] = imdb.get_runtime()
            info['country'] = imdb.get_country()
            info['language'] = imdb.get_language()
        else:
            parse_entry_wp(p, info)
    else:
        # Initialize wpParser object
        parse_entry_wp(p, info)

    # Try to find ### or 1### or 2### in full text and treat as story time
    year_search = ''
    if re.match(r'[12]?\d{3}', p[0].text):
        year_search = re.match(r'[12]?\d{3}', p[0].text).group()
    info['time'] = str(year_search)
    # Assume the film producing location is where the story happens
    info['location'] = info['country'][0] if info['country'] else ''
    # Fill out text section and categories section
    info['text'] = p[0].text
    cats = []
    for k in p[0].categories.keys():
        if not k.startswith('Use'):
            cats.append(k[9:])
    info['categories'] = cats
    # For console debug
    # print('(STAGE 4) Parsing entry: ' + p[0].title + '...DONE')
    return info, p[1]


def parse_entry_wp(p, info):
    # parse from wikipedia
    wp = wpParser(p[0].title)
    info['director'] = wp.info['director']
    info['starring'] = wp.info['starring']
    info['running time'] = wp.info['running time']
    info['country'] = wp.info['country']
    info['language'] = wp.info['language']
    return


async def scrap_site(url, session):
    """ Asynchronous method to use aiohttp session to retrieve http responses

    :param url: the url for the request
    :param session: the asynchronous aiohttp client session
    :return: the html document received, or None if exception
    """

    try:
        resp = await session.get(url)
        html = await resp.text()
        # print('...Scraping URL: ' + url + '...DONE')
        return html
    except Exception:
        logging.error("Scraping URL ERROR: " + url)
        return None


async def main(loop, list, json_data):
    """ Main method to handle all asynchronous tasks

    :param loop: asyncio event loop
    :param list: the fetching list of movies (list of tuples (wiki.page, index))
    :param json_data: the dictionary to store all movie entries
    :return:
    """
    # Initialize a pool of processes
    pool = mp.Pool(MP_CORE)

    # Open a new asynchronous aiohttp session
    async with aiohttp.ClientSession() as session:
        # Dealing with all crawl_id() tasks asynchronously
        parse_jobs = []
        if WP_ONLY:
            # Dealing with all parse_entry() tasks using asynchronous multiprocessing
            print('(STAGE 1/1) Parsing entries...')
            html = None
            parse_jobs = [pool.apply_async(parse_entry, args = (p, html)) for p in list]
        else:
            print('(STAGE 1/4) Crawling ids...', end = '')
            tasks = {loop.create_task(crawl_id(p, session)): p for p in list}
            pending = set(tasks.keys())
            htmls = []
            while pending:
                done, pending = await asyncio.wait(pending, timeout = TIME_OUT)
                htmls.extend([d.result() for d in done])
                new_pending = set()
                for t in pending:
                    new_pending.add(loop.create_task(crawl_id(tasks[t], session)))
                    t.cancel()
                pending = new_pending
            print('DONE')

            # Dealing with all parse_id() tasks using asynchronous multiprocessing
            print('(STAGE 2/4) Parsing ids...', end = '')
            parse_id_jobs = [pool.apply_async(parse_id, args = (p, html)) for p, html in htmls]
            results_id = [j.get() for j in parse_id_jobs]
            print('DONE')

            # Dealing with all crawl_entry() tasks asynchronously
            print('(STAGE 3/4) Crawling entries...', end = '')
            tasks = {loop.create_task(crawl_entry(p, id, session)): p for p, id in results_id}
            done, pending = await asyncio.wait(tasks)
            htmls = [d.result() for d in done]
            print('DONE')

            # Dealing with all parse_entry() tasks using asynchronous multiprocessing
            print('(STAGE 4/4) Parsing entries...')
            parse_jobs = [pool.apply_async(parse_entry, args = (p, html)) for p, html in htmls]
        # Storing all results and count the number
        count = 0
        for j in parse_jobs:
            info, ind = j.get()
            json_data[ind] = info
            count += 1
            print('...parsed and storing entry: ' + str(count) + '/' + str(len(list)))
        print('DONE\n\n' + str(count) + '/' + str(len(list)) + ' entries stored')


if __name__ == "__main__":
    i = 1;
    while i < len(sys.argv):
        if sys.argv[i] == '-p':
            MP_CORE = int(sys.argv[i + 1])
            i += 1
        elif sys.argv[i] == '-t':
            TIME_OUT = int(sys.argv[i + 1])
            i += 1
        elif sys.argv[i] == '-m':
            WP_ONLY = bool(sys.argv[i + 1])
            i += 1
        elif sys.argv[i] == '-l':
            LIMIT = int(sys.argv[i + 1])
            i += 1
        i += 1
    print('Number of Processes=' + str(MP_CORE) + '   Time Out=' + str(TIME_OUT) + 's')
    if LIMIT:
        print('Limit=' + str(LIMIT) + ' entries   ', end = '')
    else:
        print('No limit on crawling items', end = '')
    if WP_ONLY:
        print('Working in Wikipedia only mode')
    else:
        print('Working in hybrid mode\n')
    t1 = time.time()
    # Gathering all entries under 'Category:2018 films'
    wiki = wikipediaapi.Wikipedia('en')
    cat = wiki.page("Category:2018 films")
    cat_pages = [wiki.page(p) for p in cat.categorymembers]

    json_data = {}  # For storing Dicts
    list = set()  # Tuple (wiki.page, index) list
    index = 1
    for p in cat.categorymembers:
        page = wiki.page(p)
        if not page.title.startswith('Category:'):  # Eliminate category pages
            list.add((page, index))
            index += 1
            if index == LIMIT + 1:
                break

    # Initialize asynchronous task
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, list, json_data))
    loop.close()

    # Make dumped JSONs sorted by key
    json_sorted = {}
    for i in range(len(json_data)):
        json_sorted[i + 1] = json_data[i + 1]
    # Dump the Dict to JSON
    with open('data.json', 'w') as f:
        json.dump(json_sorted, f, indent=4)

    print('Finished writing JSON. JSON corpus built in total time: ' + str(time.time() - t1) + 's')
