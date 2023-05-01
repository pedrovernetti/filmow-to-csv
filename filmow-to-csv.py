#!/usr/bin/env python3

###############################################################################
##                                                                           ##
##                               filmow-to-csv                               ##
##                                                                           ##
##      Saves lists and users' watchlists, watched entries and favorites     ##
##                        from Filmow(TM) as .csv files                      ##
##                                                                           ##
##                                MIT License                                ##
##                Copyright (C) 2023 Pedro Vernetti Gonçalves                ##
##                                                                           ##
##  Permission is hereby granted, free of charge, to any person obtaining a  ##
## copy of this software and associated documentation files (the "Software"),##
## to deal in the Software without restriction, including without limitation ##
##  the rights to use, copy, modify, merge, publish, distribute, sublicense, ##
##   and/or sell copies of the Software, and to permit persons to whom the   ##
##    Software is furnished to do so, subject to the following conditions:   ##
##  The above copyright notice and this permission notice shall be included  ##
##          in all copies or substantial portions of the Software.           ##
##  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  ##
##        OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF         ##
##   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  ##
##    IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY   ##
## CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT ##
## OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR  ##
##                 THE USE OR OTHER DEALINGS IN THE SOFTWARE.                ##
##                                                                           ##
###############################################################################



from sys import argv, stdout, stderr
from os import getcwd, path
from math import ceil
from threading import Thread, Lock
from time import sleep
import re, requests

# dependencies
from bs4 import BeautifulSoup
from bs4.element import Tag as bs4ElementTag
from pandas import DataFrame

# headers used for requests
headers = { r'User-Agent':r'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0',
            r'Upgrade-Insecure-Requests': r'1',
            r'DNT': r'1',
            r'Accept': r'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            r'Accept-Language': r'en-US,en;q=0.5',
            r'Accept-Encoding': r'gzip, deflate' }



# basic IO functions

def printUnbuffered( e ):
    stdout.write(e)
    stdout.flush()

def help():
    print("""
  Options:

  -S         only collect series entries (tv programs included)
  -l         only collect feature-length film entries
  -s         only collect short film entries
  -f         only collect film (both short and feature-length) entries

  -w         only collect user's watchlist
  -W         only collect user's watched entries
  -F         only collect user's favorite entries

  -y         try \033[3mnot as hard\033[0m to fetch 'Year' metadata from entries

  -t N       download up to N entries in parallel (default: 15)

  -h         display this help and exit\n""")



# utility functions

def deduplicatedList( l ):
    knownValues = set()
    result = []
    for value in l:
        if (value in knownValues): continue
        knownValues.add(value)
        result.append(value)
    return result

def isSeasonURL( entryURL ):
    if (not entryURL): return False
    nameParts = entryURL[:-1].rsplit(r'/', maxsplit=1)[-1].split(r'-')
    if (len(nameParts) < 2): return False
    return (nameParts[-2] == r'temporada')

def truncatedURL( url, maxSize ):
    if (len(url) < maxSize): return url.strip().rjust(maxSize)
    else: return (r'...' + url[-(maxSize-3):])

urlToDatesURL_ = re.compile(r'^.*t([0-9]+)/$')

def datesPageURL( url ):
    return urlToDatesURL_.sub(r'https://filmow.com/estreias-do-filme/\1/', url)



# page-prefetching functions

_activePrefetchers = 0
_activePrefetchersLock = Lock()
_prefetchedPages = {}
_prefetchingLock = Lock()

def prefetchPages( urls ):
    _activePrefetchersLock.acquire()
    global _activePrefetchers
    _activePrefetchers += 1
    _activePrefetchersLock.release()
    failures = 0
    for url in urls:
        for i in range(0, 4):
            try: page = requests.get(url, timeout=5, headers=headers)
            except: page = None
            else: break
            sleep(5)
        failures += not page
        if (failures >= 3):
            sleep(10)
            failures = 1
        _prefetchingLock.acquire()
        _prefetchedPages[url] = page.text if page else r''
        _prefetchingLock.release()
    _activePrefetchersLock.acquire()
    _activePrefetchers -= 1
    _activePrefetchersLock.release()

def prefetchedPage( url ):
    if ((not _activePrefetchers) and (url not in _prefetchedPages)):
        try: page = requests.get(url, timeout=20, headers=headers)
        except: page = None
        return (page.text if page else r'')
    while (url not in _prefetchedPages): pass
    return _prefetchedPages[url]

def prefetch( urls, workers ):
    for i in range(0, workers):
        chunk = [urls[j] for j in range(i, len(urls), workers)]
        prefetcher = Thread(target=prefetchPages, args=(chunk,), daemon=True)
        prefetcher.start()



# specific metadata scraping functions

def listEntryCount( listPage ):
    stats = listPage.find_all(r'div', class_=r'list_stats')
    if (not stats): return 0
    stats = stats[0].find_all(r'p')
    if ((not stats) or (len(stats) < 2)): return 0
    stats = stats[1].find_all(r'span', class_=None)
    if (not stats): return 0
    stats = [span.text.strip() for span in stats if span]
    if ((not stats) or (not stats[0])): return 0
    return int(stats[0])

def listPageCount( listPage ):
    entryCount = listEntryCount(listPage)
    entriesPerPage = len([url for url in listPage.find_all(r'a') if url.get(r'data-movie-pk')])
    return ceil(entryCount / entriesPerPage)

def libPageCount( libPage ):
    pageLinks = libPage.select(r'a[href*=\?pagina\=]')
    if (not pageLinks): return 1
    pageLinks = [int(a.get(r'href').rsplit(r'=', maxsplit=1)[-1]) for a in pageLinks if a]
    return (sorted(pageLinks)[-1])

def originalTitle( entryPage ):
    title = entryPage.find_all(r'h2', class_=r'movie-original-title')
    if (not title):
        title = entryPage.find_all(r'div', class_=r'movie-other-titles')
        if (not title): return r''
        for item in title[0].find_all(r'li'):
            if (item.em.text == "Estados Unidos da América"): return item.strong.text.strip()
        return r''
    return title[0].text.strip()

def blankMetadata( a ):
    return r''

def yearFromDatesPage( entryURL ):
    page = prefetchedPage(datesPageURL(entryURL))
    if (not page): return r''
    page = BeautifulSoup(page, r'lxml')
    dates = page.find_all(r'tr')
    if (not dates): return r''
    for date in dates:
        details = date.find_all(r'td')
        if (len(details) < 3): continue
        if (details[0] and (details[0].text == r'Mundial') and details[1]):
            return details[1].text.strip()[-4:]
    return r''

def year( entryPage ):
    date = entryPage.find_all(r'div', class_=r'item release_date')
    if ((not date) or (not date[0].text.strip().startswith(r'Estreia Mundial'))): return r''
    date = date[0].find_all(r'div')
    if ((not date) or (not date[0].text)): return r''
    return re.sub(r'.*([0-9]{4})$', r'\1', re.sub(r'\W', r'', date[0].text))

def directors( entryPage ):
    names = entryPage.find_all(r'div', class_=r'directors')
    if (not names): return r''
    names = names[0].find_all(r'strong')
    if (not names): return r''
    if (names[0].text.strip().casefold() == r'dirigido por:'): names = names[1:]
    names = deduplicatedList([name for name in names if (type(name) == bs4ElementTag)])
    if (not names): return r''
    names = [re.sub(r' \([IVXL]+\)', r'', name.text) for name in names]
    if (len(names) == 1): return names[0].strip()
    return r', '.join([name.strip() for name in names])

def runtime( entryPage ):
    minutes = entryPage.find_all(r'span', class_=r'running_time')
    if (not minutes): return r''
    return re.sub(r'^\W*([0-9]+).*$', r'\1', minutes[0].text.strip())

def comment( entryPage ):
    return r''

def titleType( entryPage, runtime ):
    elements = [e.text.strip() for e in entryPage.find_all(r'li', class_=r'active') if e]
    elements = [e.casefold() for e in elements if ('\n' not in e)]
    if (elements):
        if (elements[0] == r'séries'): return r'series'
        if (elements[0] == r'tv'): return r'series'
        if (elements[0] == r'filmes'):
            if (runtime and (int(runtime) < 45)): return r'short'
            return r'film'
    return r''



# scraping loop functions

def collect( partialEntries, outputFile, titleTypes, total, currentStart=0 ):
    total = str(total)
    printUnbuffered("Reading entry " + (r'0/' + total).ljust(66))
    entries = { r'Title':[],   r'Year':[],    r'Directors':[], r'Rating':[],
                r'Runtime':[], r'Review':[], r'Title Type':[], r'URL':[] }
    for i in range(0, len(partialEntries)):
        url, rating, titleType_ = partialEntries[i]
        if (titleType_ and (titleType_[0] not in titleTypes)): continue
        entryPage = prefetchedPage(url)
        if (entryPage == r''):
            stderr.write("\r\033[2;31m" + ("Couldn't access " + url).ljust(80) + "\033[0;0m\n")
            printUnbuffered("Reading entry " + (str(i) + r'/' + total).ljust(66))
            continue
        printUnbuffered("\rReading entry " + (str(currentStart + i + 1) + r'/' + total).ljust(11))
        printUnbuffered("\033[2m" + truncatedURL(url, 55) + "\033[0m")
        entryPage = BeautifulSoup(entryPage, r'lxml')
        runtime_ = runtime(entryPage)
        if (not titleType_): titleType_ = titleType(entryPage, runtime_)
        if (titleType_ and (titleType_[0] not in titleTypes)): continue
        entries[r'Title'].append(originalTitle(entryPage))
        if (not entries[r'Title'][-1]):
            stderr.write("\r\033[2;33m" + ("Couldn't fetch title from " + url).ljust(80) + "\033[0;0m\n")
            printUnbuffered("Reading entry " + (str(i) + r'/' + total).ljust(66))
        year_ = year(entryPage)
        entries[r'Year'].append(year_ if (year_) else yearFromDatesPage(url))
        entries[r'Directors'].append(directors(entryPage))
        entries[r'Rating'].append(rating)
        entries[r'Runtime'].append(runtime_)
        entries[r'Review'].append(comment(entryPage))
        entries[r'Title Type'].append(titleType_)
        entries[r'URL'].append(url)
    printUnbuffered("\r" + ("Finished reading " + total + " entries").ljust(80) + "\n")
    printUnbuffered("Generating csv table...\n")
    bulk = DataFrame(data=entries)
    bulk.to_csv(outputFile, sep=r',', index=False)
    printUnbuffered("Table saved to '" + outputFile + "'\n")

def userRatings( page ):
    ratings = []
    for rating in [div for div in page.find_all(r'div', class_=r'user-rating')]:
        rating = rating.find_all(r'div', class_=r'average')
        if (not rating): ratings.append(r'')
        else: ratings.append(re.sub(r'.*?([0-9]+)(\.[0-9]+)%;?$', r'\1', rating[0][r'style']))
    return [(str(round(((int(rating) / 100) * 5), 1)) if rating else r'') for rating in ratings]

def entries( baseURL, t, nPrefetcherThreads, isList=False ):
    printUnbuffered("Collecting entries from " + baseURL + "\n")
    t = r'series' if (t[0] in r'st') else (r'short' if (t[0] == r'c') else (r'film' if (t[0] == r'f') else r''))
    try:
        page = requests.get(baseURL, timeout=15, headers=headers)
    except:
        stderr.write("\033[31mCouldn't reach " + baseURL + "\033[0m\n")
        return []
    page = BeautifulSoup(page.text, r'lxml')
    totalPages = listPageCount(page) if (isList) else libPageCount(page)
    if (totalPages > 1):
        urls = [(baseURL + r'?pagina=' + str(i)) for i in range(1, (totalPages + 1))]
        prefetch(urls, min(totalPages, nPrefetcherThreads))
    urls, ratings = ([], [])
    for i in range(1, 10000):
        page = prefetchedPage(baseURL + r'?pagina=' + str(i))
        if (not page): break
        page = BeautifulSoup(page, r'lxml')
        foundURLs = [url.get(r'href') for url in page.find_all(r'a') if url.get(r'data-movie-pk')]
        if ((not foundURLs) or (urls and (foundURLs[-1] == urls[-1]))): break
        urls += deduplicatedList(foundURLs)
        ratings += userRatings(page)
    urls = [re.sub(r'^/', r'https://filmow.com/', url) for url in urls]
    if (ratings): return [(urls[i], ratings[i], t) for i in range(0, len(urls))]
    else: return [(url, r'', t) for url in urls]



# target parsing functions

def parseTarget( target ):
    result = (None, None, None)
    url = re.sub(r'^(https?://)?(www\.)?filmow', r'https://filmow', target.strip())
    if (re.match(r'^https://filmow\.com/listas/[^/\s]+/?$', url)):
        name = re.sub(r'^https://filmow\.com/listas/([^/\s]+)/?$', r'\1', url)
        result = (False, re.sub(r'([^/])$', r'\1/', url), name)
    elif (re.match(r'^https://filmow\.com/usuario/[^/\s]+(/.*)?$', url)):
        baseURL = re.sub(r'\.com/usuario/([^/]+)(/.*)?$', r'.com/usuario/\1/', url)
        name = re.sub(r'^https://filmow\.com/usuario/([^/\s]+)/$', r'\1', baseURL)
        result = (True, baseURL, name)
    elif (re.match(r'^[a-zA-Z0-9_-]+-l[0-9]+$', url)):
        result = (False, (r'https://filmow.com/listas/' + url.lower() + r'/'), url.lower())
    elif (re.match(r'^[a-zA-Z0-9_-]+$', url)):
        result = (True, (r'https://filmow.com/usuario/' + url.lower() + r'/'), url.lower())
    else:
        stderr.write("Invalid username or URL: " + url + "\n")
        return result
    try: test = requests.head(result[1], timeout=15, headers=headers, allow_redirects=True)
    except: test = None
    if (not test):
        stderr.write("\033[31mCouldn't reach " + result[1] + "\033[0m\n")
    elif ((test.status_code < 200) or (test.status_code > 203)):
        stderr.write("\033[31mGot a " + str(test.status_code) + " response from " + result[1] + "\033[0m\n")
    return result



# command line interface

if __name__ == r'__main__':
    # default values
    outputPath = getcwd()
    titleTypes = [r'filmes/', r'curtas/', r'series/', r'tv/']
    userCollections = {r'W', r'w', r'F'}
    nPrefetcherThreads = 15
    tryFetchingYearFromDatesPages = True

    # command line parsing
    if ((len(argv) == 1) or (argv[1] == r'-h') or (argv[1] == r'--help')):
        selfName = re.split(r'[\\/]', argv[0])[-1]
        stderr.write("Usage: " + selfName + " [OPTIONS] [OUTPUT_DIR] USER|USER_URL|LIST_URL\n")
        if (len(argv) > 1): help()
        exit(0)
    elif (len(argv) >= 3):
        if (path.isdir(argv[-2])):
            outputPath = argv[-2]
        elif (not argv[-2].startswith(r'-')):
            stderr.write("Invalid output directory: " + argv[-1] + "\n")
            exit(1)
        nextOptionIsN = False
        for option in argv[1:-1]:
            if (nextOptionIsN):
                if (not re.match(r'^[0-9]{1,2}$', option)):
                    stderr.write("Invalid number of threads: " + option + "\n")
                    exit(2)
                nPrefetcherThreads = int(option)
                nextOptionIsN = False
                continue
            if   (option == r'-S'): titleTypes = [r'series/', r'tv/']
            elif (option == r'-l'): titleTypes = [r'filmes/']
            elif (option == r'-s'): titleTypes = [r'curtas/']
            elif (option == r'-f'): titleTypes = [r'filmes/', r'curtas/']
            elif (re.match(r'^-[wWF]', option)): userCollections = [option[1]]
            elif (option == r'-y'): tryFetchingYearFromDatesPages = False
            elif (option == r'-t'): nextOptionIsN = True
            elif (option != outputPath): stderr.write("Invalid option: " + option + "\n")

    # target parsing
    if (path.isfile(argv[-1])):
        isUserNotList, baseURL = (None, argv[-1])
        name = path.splitext(path.split(argv[-1])[-1])[0]
    else:
        isUserNotList, baseURL, name = parseTarget(argv[-1])
    if (baseURL is None):
        stderr.write(name)
        exit(3)
    if (len(titleTypes) < 4): name += r'_' + titleTypes[0][:-1]
    name += r'.csv'

    # collecting entries
    watched, watchlist, favorites, listEntries = ([], [], [], [])
    if (isUserNotList is None):
        try:
            listEntries = open(baseURL).read().splitlines()
        except:
            stderr.write("Couldn't read '" + baseURL + "'\n")
            exit(3)
        listEntries = [(e, r'', r'') for e in listEntries if re.match(r'^https?://(www\.)?filmow', e)]
    elif (isUserNotList):
        for titleType in titleTypes:
            if (r'W' in userCollections):
                watched += entries((baseURL + titleType + r'ja-vi/'), titleType, nPrefetcherThreads)
            if (r'w' in userCollections):
                watchlist += entries((baseURL + titleType + r'quero-ver/'), titleType, nPrefetcherThreads)
            if (r'F' in userCollections):
                favorites += entries((baseURL + titleType + r'favoritos/'), titleType, nPrefetcherThreads)
    else:
        listEntries = deduplicatedList(entries(baseURL, r'?', nPrefetcherThreads, True))
        if (r'series/' not in titleTypes):
            listEntries = [(url, r, t) for url, r, t in listEntries if (not isSeasonURL(url))]
        else:
            listEntries = [(url, r, (r'series' if (isSeasonURL(url)) else t)) for url, r, t in listEntries]

    # enqueueing pages for parallel prefetching
    urls = ([url for url, _, _ in watched] + [url for url, _, _ in watchlist] +
            [url for url, _, _ in favorites] + [url for url, _, _ in listEntries])
    prefetch(urls, nPrefetcherThreads)
    if (not tryFetchingYearFromDatesPages): globals()[r'yearFromDatesPage'] = blankMetadata
    else: prefetch([datesPageURL(url) for url in urls], (1 + round(nPrefetcherThreads * 0.15)))

    # scraping entries' metadata and generating the .csv files
    titleTypes = set([t[0] for t in titleTypes])
    if (listEntries): collect(listEntries, path.join(outputPath, (r'list_'      + name)), titleTypes, len(urls))
    if (watched):     collect(watched,     path.join(outputPath, (r'watched_'   + name)), titleTypes, len(urls))
    if (watchlist):   collect(watchlist,   path.join(outputPath, (r'watchlist_' + name)), titleTypes, len(urls), len(watched))
    alreadyCollected = len(watched) + len(watchlist)
    if (favorites):   collect(favorites,   path.join(outputPath, (r'favorites_' + name)), titleTypes, len(urls), alreadyCollected)

    # success
    printUnbuffered("Done!\n")
