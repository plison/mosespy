# -*- coding: utf-8 -*-

# =================================================================                                                                   
# Copyright (C) 2014-2017 Pierre Lison (plison@ifi.uio.no)
                                                                            
# Permission is hereby granted, free of charge, to any person 
# obtaining a copy of this software and associated documentation 
# files (the "Software"), to deal in the Software without restriction, 
# including without limitation the rights to use, copy, modify, merge, 
# publish, distribute, sublicense, and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, 
# subject to the following conditions:

# The above copyright notice and this permission notice shall be 
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# =================================================================  


"""Module for dividing aligned data into several parts, respectively 
corresponding to the training, tuning, development and testing set.

NB: many functions in this modules are directly tailored (i.e. hacked)
 for the processing of specific data sets, notably the OpenSubtitles
 corpus from the OPUS database.

"""
__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date:: 2014-08-25 08:30:46 #$"

import  math, sys, gzip, json
from mosespy.system import Path
import xml.etree.cElementTree as etree



class AlignedSubtitles(object):
    
    def __init__(self, corporaDict, sourceLang, targetLang):
        self.aligns = corporaDict
        self.sourceLang = sourceLang
        self.targetLang = targetLang
          

    def extractSubset(self, subsetkeys):
        subdico = reduce(lambda x, y: x.update({y[0]:y[1]}) or x,
                  map(None, subsetkeys, map(self.aligns.get, subsetkeys)), {})
        
        return AlignedSubtitles(subdico, self.sourceLang, self.targetLang)
    
    
    def addAlternatives(self):
        correlatedAligns = {}
        for fromdoc in self.aligns:
            print "Search correlated sources for " + fromdoc
            initAligns = self.aligns[fromdoc]
            correlatedAligns[fromdoc] = [(s,set([t])) for (s,t) in initAligns]
            for otherfromDoc in [x for x in self.aligns if x!=fromdoc]:                              
                otherAligns = self.aligns[otherfromDoc]
                for i in range(0, len(initAligns)):
                    initPair = initAligns[i]  
                    for k in range(i-int(10*math.log(i+1)), i+int(10*math.log(i+1))):
                        otherPair = otherAligns[k] if k < len(otherAligns) else None  
                        if otherPair and initPair[0] == otherPair[0]:
                            correlatedAligns[fromdoc][i][1].add(otherPair[1])
                            break                         
        
        self.aligns = correlatedAligns
        for d in self.aligns:
            self.aligns[d] = [(s,list(t)) for (s,t) in self.aligns[d]]
        
    
    def getNbAlternativeTranslations(self):
        nbTranslations = 1
        for a in self.aligns:
            for p in self.aligns[a]:
                if isinstance(p[1], list) and len(p[1]) > nbTranslations:
                    nbTranslations = len(p[1])
        print "number of alternative translations: " + str(nbTranslations)
        return nbTranslations
       
        
        
    def getBestAlignment(self):
        self.addAlternatives()
        sortedKeys = sorted(self.aligns, key=lambda x: 
                            max([len(y) for y in self.aligns[x]]))
        return self.extractSubset([sortedKeys[-1]]) 
    
    
    def addSubtitles(self, alignedSubtitles):
        self.aligns.update(alignedSubtitles.aligns)   
        
        
    def removeDirs(self, dirsToRemove):
        newAligns = {}
        for a in self.aligns:
            if a.getUp() not in dirsToRemove:
                newAligns[a] = self.aligns[a]
        self.aligns = newAligns
         

    def getInverse(self):
        invertedDict = {}
        for a in self.aligns:
            invertedDict[a] = [(t,s) for (s,t) in self.aligns[a]]
        return AlignedSubtitles(invertedDict, self.targetLang, self.sourceLang)


    def selectDirectories(self, nbDirs=5):
        if len(self.aligns) < 20:
            raise RuntimeError("not enough data to divide")
        print "Sorting data by number of duplicates"
        sources = sorted(self.aligns.keys(), key=lambda x : len(x.getUp().listdir()))
        testDirs = set()
        while len(testDirs) < nbDirs:
            sourceFile = sources.pop()
            testDirs.add(sourceFile.getUp())
        return list(testDirs)
 
  
    
    def extractBestAlignments(self, directories, inverse=False):
           
        alignedData = AlignedSubtitles({}, self.sourceLang, self.targetLang)
        
        for testDir in directories:
            alignsInDir = self.extractSubset([x for x in self.aligns if testDir in x])
            alignsInDir = alignsInDir.getInverse() if inverse else alignsInDir
            bestAlignment = alignsInDir.getBestAlignment()
            alignedData.addSubtitles(bestAlignment)

        return alignedData   
    
    
    def divideData(self, nbTuningFiles=3, nbDevFiles=5, nbTestFiles=5):
        
        training = AlignedSubtitles(self.aligns, self.sourceLang, self.targetLang)
        
        testDirs = training.selectDirectories(nbTestFiles)
        test = training.extractBestAlignments(testDirs, True)
        
        training.removeDirs(testDirs)
     
        devdirs = training.selectDirectories(nbDevFiles)
        dev = training.extractBestAlignments(devdirs, True)

        training.removeDirs(devdirs)

        tuneDirs = training.selectDirectories(nbTuningFiles)
        tune = training.extractBestAlignments(tuneDirs, False)

        training.removeDirs(tuneDirs)
        
        return training, tune, dev, test
    
    
    def generateMosesFiles(self, stem):
        nbTranslations = self.getNbAlternativeTranslations()
        
        srcFile = open(stem+"." + self.sourceLang, 'w')
        trgFile = open(stem + "." + self.targetLang, 'w')
        altFiles = ([open((trgFile.name + str(i)), 'w') for i in range(0, nbTranslations)]
                    if nbTranslations > 1 else [])
        
        for document in self.aligns:
            for pair in self.aligns[document]:
                srcFile.write(pair[0].encode("UTF-8"))
                trgLine = pair[1][0] if isinstance(pair[1], list) else pair[1]
                trgFile.write(trgLine.encode("UTF-8"))
                for i in range(0, len(altFiles)):
                    altLine = pair[1][i] if i < len(pair[1]) else ""
                    altFiles[i].write(altLine.encode("UTF-8"))
        
        srcFile.close()
        trgFile.close()
        for altFile in altFiles:
            altFile.close()
                
              

class XCESCorpus(AlignedSubtitles):
    
    def __init__(self, xcesFile):
        self.xcesFile = Path(xcesFile)
        print "Parsing file " + xcesFile
        tree = etree.parse(str(xcesFile))
        self.xmlRoot = tree.getroot()        
        for linkGrp in self.xmlRoot:
            if linkGrp.tag == 'linkGrp':
                sourceLang = linkGrp.attrib['fromDoc'].split("/")[0] 
                targetLang = linkGrp.attrib['toDoc'].split("/")[0] 
                break
             
        AlignedSubtitles.__init__(self, self.getAlignments(), sourceLang, targetLang)
        print "Finished parsing file " + xcesFile
        
                  
    def getAlignments(self):
        
        if Path(self.xcesFile+".json").exists():
            dump = json.loads((self.xcesFile+".json").read())
            newAligns = {}
            for a in dump:
                newAligns[Path(a)] = dump[a]
            return newAligns
        
        print "Extracting alignments"
        basePaths = [self.xcesFile.getUp() + "/OpenSubtitles2013/xml/", 
                     self.xcesFile.getUp() + "/OpenSubtitles2013/"]
        
        corporaDict = {}
        for linkGrp in self.xmlRoot:
            if linkGrp.tag == 'linkGrp':
                for basePath in basePaths:
                    fromdoc = Path(basePath + linkGrp.attrib['fromDoc'])
                    todoc =  Path(basePath + linkGrp.attrib['toDoc'])
                    if fromdoc.exists() and todoc.exists():
                        break
                if not fromdoc.exists():
                    raise RuntimeError("could not find " + fromdoc)
                if not todoc.exists():
                    raise RuntimeError("could not find " + todoc)
                
                fromLines = getLines(fromdoc)
                toLines = getLines(todoc)
                alignmentList = []
                for link in linkGrp:
                    if link.tag == 'link':
                        split = link.attrib["xtargets"].split(";")
                        sourceLines = [int(i) for i in split[0].strip().split(" ") if len(i)>0]
                        targetLines = [int(i) for i in split[1].strip().split(" ") if len(i)>0]                 
                        if len(sourceLines) == 0 or len(targetLines)==0:
                            continue       
                        sourceLine = " ".join([fromLines[s-1].strip() for s in sourceLines])
                        targetLine = " ".join([toLines[s-1].strip() for s in targetLines])
                        alignmentList.append((sourceLine, targetLine))
                              
                if len(alignmentList) < len(linkGrp)/2:
                    print "Skipping bad alignment files %s -> %s"%(fromdoc, todoc)
                else:
                    corporaDict[fromdoc] = alignmentList
        
        dump = json.dumps(corporaDict)
        Path(self.xcesFile + ".json").write(dump)
            
        return corporaDict


def getLines(gzipDoc):
    text = gzip.open(gzipDoc, 'r').read()
    root = etree.fromstring(text)
    lines = []
    for s in root:
        if s.tag == "s":
            wordList = []
            for w in s:
                if w.tag == "w" and w.text:
                    wordList.append(w.text.strip())
                elif w.tag == "w":
                    print "empty word for " + str(gzipDoc)
            lines.append(" ".join(wordList))
    return lines
    
   
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: opensubtitles_cleanup.py [path to XCESFile]"
        
    else:  
        xcesFile = sys.argv[1]
        corpus = XCESCorpus(xcesFile)
        train, tune, dev, test = corpus.divideData()
        
        stem = xcesFile.replace(".xml", "")
        
        train.generateMosesFiles(stem + ".train")
        tune.generateMosesFiles(stem + ".tune")
        dev.generateMosesFiles(stem + ".dev")
        test.generateMosesFiles(stem + ".test")


