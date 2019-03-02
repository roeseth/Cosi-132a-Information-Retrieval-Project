import shelve
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
import json


def index(id):
    """ A method that tokenizes and stems each word in the data field and constructed the inverted index

    :param id: the doc_id
    :return: none
    """
    ps = PorterStemmer()
    tokens = word_tokenize(data[id]['Title'][0]) + word_tokenize(data[id]['Text'])
    for token in tokens:
        if token not in stop_words:
            term = ps.stem(token)
            if term not in cache_index:
                cache_index[term] = [int(id)]
            elif cache_index[term][-1] != int(id):
                cache_index[term].append(int(id))



if __name__ == "__main__":
    import time

    s = time.perf_counter()

    idx = shelve.open('corpus_index.dat', flag = 'n', writeback = False)
    sw = shelve.open('stop_words.dat', flag = 'n', writeback = False)
    with open('films_corpus.json', 'r', encoding = 'UTF-8') as f:
        data = json.load(f)
    sw['stop_words'] = stopwords.words('english')
    stop_words = sw['stop_words']

    cache_index = {}
    for id in data:
        index(id)

    for term in cache_index:
        idx[term] = cache_index[term]

    idx.close()
    sw.close()
    f.close()

    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
