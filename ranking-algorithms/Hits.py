__author__ = 'Abhi'
#import ime
import time
import re
import numpy
import math
import operator
from collections import OrderedDict

tempAuth = {}
tempHub = {}
auth = {}
hub = {}
inlinksFile = '../../PyCharmResources/inlinks.txt'
outLinksFile = '../../PyCharmResources/outlinks.txt'
allLinksFile = '../../PyCharmResources/valid_inlinks.txt'

allUrls=set()
allUrlsCrawled = set()
inlinkMap = {}
outlinkMap = {}
authFile = open('/Users/Abhi/Desktop/authority.txt', "w")
hubFile = open('/Users/Abhi/Desktop/hubs.txt', "w")

d=50

def buildDataStructure():
    #READ Root set
    infReader = open(inlinksFile)
    olfReader = open(outLinksFile)
    allReader = open(allLinksFile)

    for line in allReader.readlines():
        keyValueList = line.split(" ")
        url = keyValueList[0]
        allUrlsCrawled.add(url)
    baseset = set()
    for line in infReader.readlines():
        keyValueList = line.split("|")
        allUrls.add(keyValueList[0])
        inlinkSet = set(keyValueList[1].split("#"))
        inlinkMap[keyValueList[0]] = []
        outlinkMap[keyValueList[0]] = []
        count=0
        for everyLink in inlinkSet:
            if everyLink not in allUrlsCrawled: continue
            count += 1
            if(count>d):
                break
            #allUrls.add(keyValueList[0])
            baseset.add(everyLink)
            allUrls.add(everyLink)
            inlinkMap[everyLink] = []
            inlinkMap[keyValueList[0]].append(everyLink)
            if everyLink in outlinkMap:
                outlinkMap[everyLink].append(keyValueList[0])
            else:
                outlinkMap[everyLink] = [keyValueList[0]]

    print outlinkMap.__len__()

    for line in olfReader.readlines():
        keyValueList = line.split("|")
        allUrls.add(keyValueList[0])
        outlinkSet = set(keyValueList[1].split("#"))
        outlinkMap[keyValueList[0]] = []
        for everyLink in outlinkSet:
            if everyLink not in allUrlsCrawled:
                continue
            baseset.add(everyLink)
            allUrls.add(everyLink)
            outlinkMap[everyLink] = []
            outlinkMap[keyValueList[0]].append(everyLink)
            if everyLink in inlinkMap:
                inlinkMap[everyLink].append(keyValueList[0])
            else:
                inlinkMap[everyLink] = [keyValueList[0]]

    infReader.close()
    olfReader.close()
    inlFile = open('../../PyCharmResources/IR_Files/inlinks_file.txt')
    print "BASESET SIZE " + str(baseset.__len__())

    ## EXPAND BASE SET
    for line in inlFile.readlines():
        columns = line.split(" ")
        if columns[0] in baseset:
            inlinkMap[columns[0]] + columns[1:]
            #print inlinkMap[columns[0]]

    inlFile.close()
    outFile = open('../../PyCharmResources/IR_Files/outlinks_file.txt')
    for line in outFile.readlines():
        columns = line.split(" ")
        if columns[0] in baseset:
            outlinkMap[columns[0]] + columns[1:]
    outFile.close()
    print allUrls.__len__()


perplexity = []

def calculate_perplexity(i):
    H = 0
    perplexity = 0
    for page in hub.keys():
        #print page + "=>" + str(PR[page])
        H += hub[page] * math.log(1/hub[page], 2)
    perplexity = 2**H
    print "PERPLEXITY " + str(i+1) + " " +  str(perplexity)
    return perplexity

def converged(i):
    change = 0
    count = 0
    perplexity.append(calculate_perplexity(i))
    if  i > 0:
        change = abs(perplexity[i] - perplexity[i-1])
        if change < 1 and count <= 4:
            count += 1
            return True
        else:
            return False
    else:
        return False



def calHubsAndAuth():
    global auth
    global hub
    index=0
    print "Calculating...."
    for url in allUrls:
            auth[url] = 1
    for url in allUrls:
            hub[url] = 1
    print auth.__len__()
    itr = 0
    count=0
    while not converged(itr):
        count=0
        for keys in hub:
            score = 0
            if outlinkMap[keys].__len__() == 0:
                count+=1
                continue
            for outlink in outlinkMap[keys]:
                score += auth[outlink]
            hub[keys] += score

        total = 0.0
        for value in hub:
            total += hub[value]
        for value in hub:
            hub[value] /= total

        for keys in auth:
            score = 0
            if inlinkMap[keys].__len__() == 0:
                count+=1
                continue
            for inlink in inlinkMap[keys]:
                score += hub[inlink]
            auth[keys] += score

        total = 0.0
        for value in auth:
            total += auth[value]
        for value in auth:
            auth[value] /= total

        itr += 1
        print "ITERATION " + str(itr) + str(count)
        #print count

def main():
    buildDataStructure()

    #adjMatrix = [[0 for i in range(allUrls.__len__())] for i in range(allUrls.__len__())]

    calHubsAndAuth()
    sortedHub = OrderedDict(sorted(hub.iteritems(), key=operator.itemgetter(1), reverse=True))
    sortedAuth = OrderedDict(sorted(auth.iteritems(), key=operator.itemgetter(1), reverse=True))

    for keys in sortedHub:
        string = str(keys) + "\n"
        hubFile.write(string)
    hubFile.close()

    for keys in sortedAuth:
        string = str(keys) + "\n"
        authFile.write(string)
    authFile.close()


main()