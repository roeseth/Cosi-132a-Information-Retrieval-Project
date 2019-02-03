import wikipediaapi
import wptools
import re
import json
from imdbparser import imdbParser
import timeit
import asyncio


async def parse_entry(p):
    entry_name_no_film = re.sub(r' \(film\)| \(2018 film\)', '', p.title)
    imdb = imdbParser(entry_name_no_film)
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
        info['title'] = entry_name_no_film
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


async def job(p, i, json_data):
    if p.title.startswith('Category:'): return
    info = await parse_entry(p)
    json_data[i] = info
    print(info)


start = timeit.default_timer()
wiki = wikipediaapi.Wikipedia('en')
cat = wiki.page("Category:2018 films")
cat_pages = [wiki.page(p) for p in cat.categorymembers]

json_data = {}
loop = asyncio.get_event_loop()
tasks = [
    job(p, i, json_data)
    for i, p in enumerate(cat_pages)
]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()

# with open('data.json', 'w') as f:
#     json.dump(json_data, f)

stop = timeit.default_timer()
print('Time: ', stop - start)
