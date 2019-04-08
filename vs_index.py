import shelve
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from collections import defaultdict
import json
import math
import time


def index():
    """ A method that generate the index of all document from data and store in cache_index

    :param: none
    :return: none
    """

    ps = PorterStemmer()

    # save all tokenized and stemmed terms for each document
    doc_terms = defaultdict(list)

    # process each document
    for id in data:

        # tokenize all content
        tokens = word_tokenize(data[id]['Title'][0]) + word_tokenize(data[id]['Text'])

        # stem all tokens and count term frequency
        terms = set()
        for token in tokens:
            if token not in stop_words:
                term = ps.stem(token)
                terms.add(term)
                tf[id][term] += 1

        doc_terms[id] = terms

        for t in terms:
            # calculate the log-frequency weighting
            wf[id][t] = 1 + math.log10(tf[id][t]) if tf[id][t] > 0 else 0

            # count the document frequency in this doc, store in idf temporarily
            idf[t] += 1

    # calculate the inverse document frequency
    N = len(data)
    for t in idf:
        idf[t] = math.log10(N / idf[t])

    # calculate the tf-idf weighting and normalizing
    for id in data:
        sum = 0

        # calculate weight and sum of w^2
        for t in doc_terms[id]:
            w[id][t] = wf[id][t] * idf[t]
            sum += math.pow(w[id][t], 2)

        # calculate len of the vector
        length = math.sqrt(sum)

        # normalize the vector
        for t in doc_terms[id]:
            w[id][t] /= length

if __name__ == "__main__":

    print('Building weight vector...')

    # setup timer
    s = time.perf_counter()

    # open files
    vs_w = shelve.open('corpus_vs_weight.dat', flag = 'n', writeback = False)
    vs_idf = shelve.open('corpus_vs_idf.dat', flag = 'n', writeback = False)
    sw = shelve.open('stop_words.dat', flag = 'n', writeback = False)
    with open('films_corpus.json', 'r', encoding = 'UTF-8') as f:
        data = json.load(f)

    # get and store the stopwords list
    sw['stop_words'] = stopwords.words('english')
    stop_words = sw['stop_words']

    # the term frequency tf_(t,d)
    tf = defaultdict(lambda: defaultdict(int))

    # the log-frequency weighting wf_(t,d) using sublinear tf scaling
    wf = defaultdict(lambda: defaultdict(float))

    # the inverse document frequency idf_t
    idf = defaultdict(float)

    # the tf-idf weighting
    w = defaultdict(lambda: defaultdict(float))

    # build weight index
    index()

    # save the cache index into disk
    for id in w:
        vs_w[id] = w[id]

    for t in idf:
        vs_idf[t] = idf[t]

    vs_w.close()
    vs_idf.close()
    sw.close()
    f.close()

    # print total processing time
    elapsed = time.perf_counter() - s
    print('Building process finished')
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
