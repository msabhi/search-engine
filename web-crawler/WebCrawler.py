__author__ = 'Abhi'

##############################################################################
#
#            WEB CRAWLER TO COLLECT DOCUMENTS TO BE INDEXED
#
#
##############################################################################


from bs4 import BeautifulSoup
import urlparse
import httplib
from urlparse import urljoin
import robotparser
import requests
import re
import time
from collections import OrderedDict
from requests.packages import urllib3
import socket
import pickle
import operator
import sgmllib, string
import os.path


# Crawled documents are partitioned into seperate files, each file having only 1000 documents
# this way of paritioning, helps in indexing better
OUTPUT_FILE = '/Users/Abhi/Documents/InformationRetrieval/hw3_collection/collection_'
LEVEL_MARKER = 'D'
socket.setdefaulttimeout(1)
rp = robotparser.RobotFileParser()

# define the default ports for different protocols
default_port = {'http': '80',
                'https': '443',
                'gopher': '70',
                'news': '119',
                'snews': '563',
                'nntp': '119',
                'snntp': '563',
                'ftp': '21',
                'telnet': '23',
                'prospero': '191',
                'None': 'None'}

# check if the link is crawlable per the domain defined robots.txt
def doesRobotsTextAllow(url):
    up = urlparse.urlparse(url)
    rp.set_url(up.scheme+"://"+up.hostname+"/robots.txt")
    rp.read()
    return rp.can_fetch("*", up.path)


# return type of data refered to by the URL
def isFileTextOrHtml(url):
    try:
        content = requests.head(url, allow_redirects=True, timeout=1)
    except:
        return False
    contentType = content.headers['Content-Type'].split(";")[0].strip()
    return re.match(r"text/html",contentType)

# determine if the URL is healthy and crawlable
def canBeParsed(url):
    return (not(url is None) and re.match(r"^http",url) and doesRobotsTextAllow(url)) #and isItHtmlOrText(url))

# if a url is a relative path, form the absolute path
def getAbsolutePath(parentUrl, relativeUrl):
    if(re.match(r"^#", relativeUrl)):
        return relativeUrl
    else:
        return urljoin(parentUrl, relativeUrl)


# form the canonical path
def getCanonicalPath(url):
    newCannonicalLink = urlparse.urlparse(url)
    try:
        if(str(newCannonicalLink.port) in default_port.values()):
            portName = ""
        else:
            portName = ":" + str(newCannonicalLink.port)
    except:
        portName = ""
    newCannonicalLinkPath = re.sub(r'/{2}','/',newCannonicalLink.path)
    return newCannonicalLink.scheme.lower() + "://" + newCannonicalLink.hostname.lower() + portName + newCannonicalLinkPath


def iterMap(m):
    l=[]
    for key in m.iterkeys():
        l.append((key, m[key]))
    return l

# main algorithm which use BFS strategy to crawl and maintain a level to
# crawl for the next set of documents, once a url is visited, we avoid the url
# by maintaining its identity in a set

def doBFSSearch(orderedDict, avu, noOfDocs):
    fileIndex = 23
    cwrite = open(OUTPUT_FILE+str(fileIndex)+'.txt', 'w')
    alreadVisitedUrls = avu
    od=OrderedDict(orderedDict)
    nextLevelOd=OrderedDict()
    noOfDocuments=noOfDocs
    od[LEVEL_MARKER]='[]'
    tempVisitedLinks=set()  # avoid visiting the already visited links
    try:
        while(od.__sizeof__()>0):
            for key in od.keys():
                u = urlparse.urlparse(key)
                if noOfDocuments > 30000:
                    cwrite.close()
                    exit(0)
                reqURL = key

                # is next level in the BFS?, reinitialize the queue for new level
                if(key==LEVEL_MARKER):
                    del od[key]
                    od = OrderedDict(sorted(iterMap(nextLevelOd), key=operator.itemgetter(1), reverse=True))
                    od[LEVEL_MARKER] = []
                    nextLevelOd = OrderedDict()
                    tempVisitedLinks=set()
                    continue

                alreadVisitedUrls.add(key)
                time.sleep(1)

                # retry twice to handle worst case scenarios
                try:
                    r = requests.get(reqURL, timeout=1, stream=True)
                except:
                    try:
                        r = requests.get(reqURL, timeout=1, stream=True)
                    except:
                        print key
                        r.close()
                        del od[key]
                        continue

                # some have either "Content-Type" or "content-type", handle both
                try:
                    contentType = r.headers['Content-Type']
                except:
                    try:
                        contentType = r.headers['content-type']
                    except:
                        del od[key]
                        r.close()
                        continue

                # Our documents needs to follow a a format including a <DOC> tag
                if(re.match(r"text/html",contentType.split(";")[0].strip()) or re.match(r"text/plain",contentType.split(";")[0].strip())):

                    # we have reached the threshold, we have to write to a new file
                    if(noOfDocuments>0 and noOfDocuments%1000==0):
                        fileIndex += 1
                        cwrite.close()
                        cwrite = open(OUTPUT_FILE+str(fileIndex)+'.txt', 'w')

                    noOfDocuments += 1  # track how many documents for the current open file
                    cwrite.write('<DOC>\n')
                    cwrite.write('<INDEX-DOC-ID>'+ key.encode('utf-8') +'</INDEX-DOC-ID>\n')
                    try:
                        data = r.content
                    except:
                        try:
                            data = r.content
                        except:
                            del od[key]
                            r.close()
                            continue

                    r.close()
                    soup = BeautifulSoup(data)
                    line = ""
                    try:
                        cwrite.write('<INDEX-RAW-HTML>'+data+'</INDEX-RAW-HTML>\n')
                    except:
                        cwrite.write('<INDEX-RAW-HTML>'+""+'</INDEX-RAW-HTML>\n')
                    cwrite.write('<INDEX-IN-LINKS>\n')
                    try:
                        for link in od[key]:
                            cwrite.write(link.encode('utf-8')+"\n")
                    except:
                        cwrite.write("\n")
                        #print link
                    cwrite.write('</INDEX-IN-LINKS>\n')
                    del od[key]
                    cwrite.write('<INDEX-OUT-LINKS>\n')

                    # Include the logic to seaerch for all the links in the current html page
                    # and include it for parsing
                    for link in soup.find_all('a'):
                        newlink = link.get('href')
                        if newlink is None:
                            continue
                        newlink = getAbsolutePath(reqURL, newlink)
                        if(canBeParsed(newlink)):
                            try:
                                newlink = getCanonicalPath(newlink)
                            except:
                                continue
                            if(tempVisitedLinks.__contains__(newlink)): continue
                            tempVisitedLinks.add(newlink)
                            if (not alreadVisitedUrls.__contains__(newlink)):
                                cwrite.write(newlink.encode('utf-8')+"\n")
                                if(od.has_key(newlink)):
                                    od[newlink].append(key)
                                else:
                                    if(nextLevelOd.has_key(newlink)):
                                        nextLevelOd[newlink].append(key)
                                    else:
                                        nextLevelOd[newlink] = [key]
                    cwrite.write('</INDEX-OUT-LINKS>\n')
                    cwrite.write('</DOC>\n')
                    od = OrderedDict(sorted(iterMap(od), key=operator.itemgetter(1), reverse=True))
                else:
                    del od[key]
                    r.close()
                    continue
    except:
        pickle.dump(od, open( "od.p", "wb" ))
        pickle.dump(alreadVisitedUrls, open("alreadVisitedUrls.p", "wb"))
        noOfDocumentsWriter = open("config.txt", "w")
        noOfDocumentsWriter.write(str(noOfDocuments))
        noOfDocumentsWriter.close()
    cwrite.close()

def main():
    if(os.path.isfile("od.p")):
        orderedDict = pickle.load( open( "od.p", "rb" ) )
        avu = pickle.load(open("alreadVisitedUrls.p", "rb"))
        configReader = open("config.txt")
        noOfDocs = int(configReader.read())
    else:
        orderedDict={'http://www.mass.gov/anf/research-and-tech/research-state-and-local-history/massachusetts-political-figures.html':[],
                     'http://www.ontheissues.org/states/ma.htm':[],
                     'http://en.wikipedia.org/wiki/Politics_of_Massachusetts':[],
                     'http://en.wikipedia.org/wiki/Deval_Patrick':[],
                     'http://en.wikipedia.org/wiki/Governor_of_Massachusetts':[]}
        avu = set()
        noOfDocs = 0
    doBFSSearch(orderedDict, avu, noOfDocs)
