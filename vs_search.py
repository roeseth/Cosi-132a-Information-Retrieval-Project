"""
boolean_search.py
author: 

Students: Modify this file to include functions that implement 
Boolean search, snippet generation, and doc_data presentation.
"""
import math
import heapq as hq
from collections import defaultdict


def disjunctive(query, idx):
    """Return a list of movie ids that match the query.
    intersect one by one from the shortest posting list

    :param query:
    :param idx:
    :return:
    """

    if len(query) == 0:
        return []
    ordered = {}
    for e in query:
        ordered[e] = len(idx[e])
    ordered = sorted(ordered.items(), key = lambda d: d[1])
    results = idx[ordered[0][0]]
    i = 1
    while i < len(ordered):
        results = merge(results, idx[ordered[i][0]])
        i += 1
    return results


def merge(p1, p2):
    """ doing intersection of posting list p1 and p2

    :param p1:
    :param p2:
    :return:
    """
    posts = []
    i = 0
    j = 0
    while i < len(p1) and j < len(p2):
        if p1[i] == p2[j]:
            posts.append(p1[i])
            i += 1
            j += 1
        elif p1[i] < p2[j]:
            posts.append(p1[i])
            i += 1
        else:
            posts.append(p2[j])
            j += 1
    while i < len(p1):
        posts.append(p1[i])
        i += 1
    while j < len(p2):
        posts.append(p2[j])
        j += 1
    return posts


def search(query, w, idf, idx):
    """Return a list of movie ids that match the query."""
    if len(query) == 0:
        return []

    scores = defaultdict(float)

    docs = disjunctive(query, idx)

    # all tf in q is 1
    for t in query:
        # the log-frequency weighting using sublinear tf scaling
        wf_q = 1 + math.log10(1)
        # the tf-idf weighting, without normalization
        w_q = wf_q * idf[t]

        for d in docs:
            d = str(d)
            scores[d] += w[d][t] * w_q

    # heap sort the result
    results = hq.nlargest(len(scores), scores.items(), key = lambda s: s[1])

    return results


def get_movie_data(doc_id, data):
    """
    Return data fields for a movie.
    Your code should use the doc_id as the key to access the shelf entry for the movie doc_data.
    You can decide which fields to display, but include at least title and text.
    """
    doc_id = str(doc_id)
    movie_object = {"title": data[doc_id]['Title'][0],
                    "director": data[doc_id]['Director'][0],
                    "location": data[doc_id]['Location'][0] if len(data[doc_id]['Location']) > 0 else data[doc_id][
                        'Location'],
                    "text": data[doc_id]['Text']
                    }
    return movie_object


def get_movie_snippet(doc, data):
    """
    Return a snippet for the results page.
    Needs to include a title and a short description.
    Your snippet does not have to include any query terms, but you may want to think about implementing
    that feature. Consider the effect of normalization of index terms (e.g., stemming), which will affect
    the ease of matching query terms to words in the text.
    """
    doc_id = doc[0]
    return doc_id, data[doc_id]['Title'][0], data[doc_id]['Text'][:300] + '......', doc[1]


def get_missing_terms(doc_id, terms, w):
    """
    Return a snippet for the results page.
    Needs to include a title and a short description.
    Your snippet does not have to include any query terms, but you may want to think about implementing
    that feature. Consider the effect of normalization of index terms (e.g., stemming), which will affect
    the ease of matching query terms to words in the text.
    """
    doc_set = set(w[doc_id])
    matched = terms & doc_set
    missing = list(terms - matched)

    matched = list(matched)
    matched.sort()

    missing = list(missing)
    missing.sort()

    return missing, matched
