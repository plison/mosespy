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

import  math, sys, gzip, json, re, codecs, os
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
            correlatedAligns[fromdoc] = [(s,set([t] if t else [])) for (s,t) in initAligns]
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
        return self        
    
    def getNbAlternativeTranslations(self):
        nbTranslations = 1
        for a in self.aligns:
            for p in self.aligns[a]:
                if isinstance(p[1], list) and len(p[1]) > nbTranslations:
                    nbTranslations = len(p[1])
        return nbTranslations
       

    
    def addSubtitles(self, alignedSubtitles):
        self.aligns.update(alignedSubtitles.aligns)   
        
        
    def removeDirs(self, dirsToRemove):
        newAligns = {}
        for a in self.aligns:
            if a.getUp() not in dirsToRemove:
                newAligns[a] = self.aligns[a]
        self.aligns = newAligns
        
        
    def getDirs(self):
        dirs = set()
        for a in self.aligns:
            dirs.add(a.getUp())
        return list(dirs)
         

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

  
    def getInverse(self):
        invertedDict = {}
        flatten = lambda x : x[0] if isinstance(x,list) else x
        for a in self.aligns:
            invertedDict[a] = [(flatten(t),s) for (s,t) in self.aligns[a]]
        return AlignedSubtitles(invertedDict, self.targetLang, self.sourceLang)


    
    def extractBestAlignments(self, directories, addAlternatives=False):
        
        alignedData = AlignedSubtitles({}, self.sourceLang, self.targetLang)
        
        for testDir in directories:
            alignsInDir = self.extractSubset([x for x in self.aligns if testDir in x])
            if addAlternatives:
                alignsInDir.addAlternatives()
            alignedDocs= alignsInDir.aligns.keys()
            alignedDocs.sort(key=lambda x: max([len(y) for y in alignsInDir.aligns[x]]))
            bestAlignment = alignsInDir.extractSubset([alignedDocs[-1]])
            alignedData.addSubtitles(bestAlignment)

        return alignedData   
    

    
    def generateMosesFiles(self, stem):
        nbTranslations = self.getNbAlternativeTranslations()
        print ("Generating bitexts %s.%s -> %s.%s (number of translations: %i)"
               %(stem, self.sourceLang, stem, self.targetLang, nbTranslations))
        
        srcFile = codecs.open(stem+"." + self.sourceLang, 'w', "utf-8")
        trgFile = codecs.open(stem + "." + self.targetLang, 'w', "utf-8")
        altFiles = []
        if nbTranslations > 1:      
            altFiles = [codecs.open((trgFile.name + str(i)), 'w', "utf-8") 
                        for i in range(0, nbTranslations)]
        
        # Sorted by year, then number
        
        prefix = os.path.commonprefix(self.aligns.keys())
        alignKeys = [x.replace(prefix, "") for x in list(self.aligns.keys())]
        alignKeys.sort(key=lambda x : opushash(x))              
                           
        for document in alignKeys:
            document = prefix + document
            for pair in self.aligns[document]:
                if pair[0] and pair[1]:
                    srcFile.write(normalise(pair[0]))
                    trgFile.write(normalise(pair[1]))
                    for i in range(0, len(altFiles)):
                        altLine = pair[1][i] if i < len(pair[1]) else ""
                        altFiles[i].write(normalise(altLine))
        
        srcFile.close()
        trgFile.close()
        for altFile in altFiles:
            altFile.close()

            

     
    def divideData(self, nbTuningFiles=3, nbDevFiles=5, nbTestFiles=5):
        
        trainingData = AlignedSubtitles(self.aligns, self.sourceLang, self.targetLang)
        
        print "Extracting test data"
        testDirs = trainingData.selectDirectories(nbTestFiles)
        testData = trainingData.extractBestAlignments(testDirs, True)        
        trainingData.removeDirs(testDirs)
     
        print "Extracting development data"
        devdirs = trainingData.selectDirectories(nbDevFiles)
        devData = trainingData.extractBestAlignments(devdirs, True)
        trainingData.removeDirs(devdirs)

        print "Extracting tuning data"
        tuneDirs = trainingData.selectDirectories(nbTuningFiles)
        tuneData = trainingData.extractBestAlignments(tuneDirs, False)
        trainingData.removeDirs(tuneDirs)
        
        return trainingData, tuneData, devData, testData
        
       

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
        print "Source lang: %s, target lang: %s"%(sourceLang, targetLang)
        
                  
    def getAlignments(self):
        
        if Path(self.xcesFile+".json").exists():
            dump = json.loads((self.xcesFile+".json").read())
            newAligns = {}
            for a in dump:
                newAligns[Path(a)] = dump[a]
            return newAligns
        
        print "Extracting alignments"
        corporaDict = {}
        for l in range(0, len(self.xmlRoot)):
            linkGrp = self.xmlRoot[l]
            if linkGrp.tag == 'linkGrp':
                fromdoc = self.expandPath(linkGrp.attrib['fromDoc'])
                todoc =  self.expandPath(linkGrp.attrib['toDoc'])
                
                fromLines = self.getLines(fromdoc)
                toLines = self.getLines(todoc)
                alignmentList = []
                for link in linkGrp:
                    if link.tag == 'link':
                        split = link.attrib["xtargets"].split(";")
                        sourceLines = [int(i) for i in split[0].strip().split(" ") if len(i)>0]
                        targetLines = [int(i) for i in split[1].strip().split(" ") if len(i)>0]                 
                        if (len(sourceLines) == 0 or len(targetLines)==0 
                            or len(sourceLines) >2 or len(targetLines) > 2):
                            continue       
                        sourceLine = " ".join([fromLines[s-1].strip() for s in sourceLines])
                        targetLine = " ".join([toLines[s-1].strip() for s in targetLines])
                        if sourceLine and targetLine:
                            alignmentList.append((sourceLine, targetLine))
                
                if len(alignmentList) < (3*len(linkGrp)/4):
                    print "Skipping badly aligned file %s -> %s"%(fromdoc, todoc)
                else:
                    corporaDict[fromdoc] = alignmentList
            if not (l % (len(self.xmlRoot)/min(100,len(self.xmlRoot)))):
                print "... %s %% of alignments extracted"%((l*100/len(self.xmlRoot)))
        
        print "Percentage of discarded pairs: %i %%"%(len(corporaDict)*100/len(self.xmlRoot))
        dump = json.dumps(corporaDict)
        Path(self.xcesFile + ".json").write(dump)
            
        return corporaDict

 
    def expandPath(self, doc):               
        for basePath in ["/OpenSubtitles2013/xml/","/OpenSubtitles2013/"]:
            docPath = Path(self.xcesFile.getUp() + basePath + doc)
            if docPath.exists():
                break
        if not docPath.exists():
            raise RuntimeError("could not find " + docPath)
        return docPath
        
    def getLines(self, gzipDoc):
        
        text = gzip.open(gzipDoc, 'r').read()
        root = etree.fromstring(text)
        lines = []
        for s in root:
            if s.tag == 's':
                line = getLine(s)
                lines.append(line)
        return lines

def getLine(xmlChunk):
    wordList = []
    for w in xmlChunk:
        if w.tag == 'w' and w.text != None:
            wordList.append(w.text.strip())
        else:
            wordList.append(getLine(w))                    
    return " ".join(wordList)


def opushash(path):
    year = int(path.split("/")[0])
    number = path.split("/")[1]
    result = year*1000000000
    for i in range(0, min(6,len(number))):
        result += ord(number[i])*(100000/math.pow(10,i))
    return result
        
        
    
def normalise(line):
    if isinstance(line, list):
        return normalise(line[0])
    else:
        line = line.strip()
        line = re.sub(r"\s+", " ", line)
        line = re.sub(r"[\x00-\x1f\x7f\n]", " ", line)
        line = re.sub(r"\<(s|unk|\/s|\s*and\s*|)\>", "", line)
        line = re.sub(r"\[\s*and\s*\]", "", line)
        line = unicode(line).translate({ord(u"\u201c"):ord('"'), ord(u"\u201d"):ord('"'),
                               ord(u"\u201e"):ord('"'), ord(u"\u201f"):ord('"'),
                               ord(u"\u2013"):ord('-')})
        line = re.sub(r"\|", "_", line)
        return (line + "\n")
                
           
              
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: opensubtitles_cleanup.py [path to XCESFile]"
        
    else:  
        xcesFile = sys.argv[1]
        corpus = XCESCorpus(xcesFile)
        baseStem = Path(xcesFile.replace(".xml", ""))
            
        train, tune, dev, test = corpus.divideData()
        
        for inDir in baseStem.getUp().listdir():
            if any([(baseStem + "." + f) in inDir for f in ["train","tune","dev","test"]]):
                inDir.remove()
        
        train.generateMosesFiles(baseStem + ".train")
        tune.generateMosesFiles(baseStem + ".tune")
        dev.generateMosesFiles(baseStem + ".dev")
        test.generateMosesFiles(baseStem+ ".test")
        
        devInv = dev.getInverse().addAlternatives()
        devInv.generateMosesFiles(baseStem + ".dev")
        
        testInv = test.getInverse().addAlternatives()
        testInv.generateMosesFiles(baseStem + ".test")


