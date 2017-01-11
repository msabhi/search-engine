__author__ = 'Abhi'

import os
import math
import operator
from collections import OrderedDict

urlNodes = {}
outLinkMap = {}
noOutLinksList = []
inlinksFile = '../../PyCharmResources/wt2g_inlinks.txt'
#inlinksFile = '../../PyCharmResources/valid_inlinks.txt'

PR = {}
N = 0  # number of given URLS in the input file inlinks.txt
perplexity = []
tempPR = {}
def buildDataStructure():
    allUrs = set()
    global N
    inlinkFileReader = open(inlinksFile)
    for line in inlinkFileReader.readlines():
        line = line.strip().strip("\n")
        urls = line.split(" ")
        node = urls[0]
        urlNodes[node] = []
        allUrs.add(node)
        #print urls
        #if (urls.__len__()==1): urlNodes[node] = [0]
        for link in urls[1:]:
            #print "AM HERE TOO"
            allUrs.add(link)
            link = link.strip()
            urlNodes[node].append(link)
            if link in outLinkMap:
                outLinkMap[link] += 1
            else:
                outLinkMap[link] = 1
    N = urlNodes.__len__()
    print "LENGTH OF ALL " + str(allUrs.__len__())
    print N
    for key in urlNodes.keys():
        key = key.strip()
        PR[key] = 1.0/N                       #Initialize all the urls to default 1/N
        #print N
        if key not in outLinkMap:           #Grab all the links which don't have outlinks
            noOutLinksList.append(key)
    #print "PR" + str(PR["WT17-B19-101"])

def calculate_perplexity(i):
    H = 0
    perplexity = 0
    for page in PR.keys():
        #print page + "=>" + str(PR[page])
        H += PR[page] * float(math.log(1.0/float(PR[page]),2.0))
    perplexity = 2.0**float(H)
    print "PERPLEXITY " + str(i+1) + " " +  str(perplexity)
    return perplexity

def converged(i):
    change = 0
    count = 0
    perplexity.append(calculate_perplexity(i))
    if  i > 0:
        change = abs(perplexity[i] - perplexity[i-1])
        if change < 1 and count < 4:
            count += 1
            return True
        else:
            return False
    else:
        return False

def calculatePR():
    global N
    global  PR
    d = 0.85
    i=0
    while not converged(i):
        sinkPR=0
        tempPR = {}
        for everyNoOutLink in noOutLinksList:
            sinkPR += PR[everyNoOutLink]
        sinkPR = d * float(sinkPR)/N
        #print urlNodes.keys().__len__()
        print len(urlNodes.keys())
        for key in urlNodes.keys():
            tempPR[key] = float(1-d)/float(N)
            tempPR[key] += sinkPR
            #print key
            for inlinkKey in urlNodes[key]:
                tempPR[key] += float(d) * float(PR[inlinkKey])/float(outLinkMap[inlinkKey])
        i += 1
        PR = tempPR

def main():
    buildDataStructure()
    calculatePR()
    sortedPR = sorted(PR.iteritems(), key=operator.itemgetter(1), reverse=True)
    output = open("/Users/Abhi/Desktop/pagerank-2.txt","w")
    for key in sortedPR:
        string = str(key) + "\n"
        output.write(string)
    output.close()




main()





