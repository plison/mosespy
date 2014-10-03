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

from io import BytesIO
import  math, sys, re, os, collections, tarfile, gzip
from mosespy.system import Path
import xml.etree.cElementTree as etree



class AlignedSubtitles(object):
    
    def __init__(self, bitext, sourceLang, targetLang):
        self.bitext = bitext
        self.sourceLang = sourceLang
        self.targetLang = targetLang
          

    def extractSubset(self, subsetkeys):
        subdico = reduce(lambda x, y: x.update({y[0]:y[1]}) or x,
                  map(None, subsetkeys, map(self.bitext.get, subsetkeys)), {})
        
        return AlignedSubtitles(subdico, self.sourceLang, self.targetLang)
    
    
    def addAlternatives(self):
        correlatedAligns = {}
        for fromdoc in self.bitext:
            print "Search correlated sources for " + fromdoc
            initAligns = self.bitext[fromdoc]
            correlatedAligns[fromdoc] = [(s,set([t] if t else [])) for (s,t) in initAligns]
            for otherfromDoc in [x for x in self.bitext if x!=fromdoc]:                              
                otherAligns = self.bitext[otherfromDoc]
                for i in range(0, len(initAligns)):
                    initPair = initAligns[i]  
                    for k in range(i-int(10*math.log(i+1)), i+int(10*math.log(i+1))):
                        otherPair = otherAligns[k] if k < len(otherAligns) else None  
                        if otherPair and initPair[0] == otherPair[0]:
                            correlatedAligns[fromdoc][i][1].add(otherPair[1])
                            break                         
        
        self.bitext = correlatedAligns
        for d in self.bitext:
            self.bitext[d] = [(s,list(t)) for (s,t) in self.bitext[d]]
        return self        
    
    
    def getNbAlternativeTranslations(self):
        nbTranslations = 1
        for a in self.bitext:
            for p in self.bitext[a]:
                if isinstance(p[1], list) and len(p[1]) > nbTranslations:
                    nbTranslations = len(p[1])
        return nbTranslations
       

    def addSubtitles(self, alignedSubtitles):
        self.bitext.update(alignedSubtitles.bitext)   
        
        
    def removeDirs(self, dirsToRemove):
        newAligns = {}
        for a in self.bitext:
            if a.getUp() not in dirsToRemove:
                newAligns[a] = self.bitext[a]
        self.bitext = newAligns
        
        
    def getDirs(self):
        dirs = set()
        for a in self.bitext:
            dirs.add(a.getUp())
        return list(dirs)
  
  
    def getInverse(self):
        invertedDict = {}
        flatten = lambda x : x[0] if isinstance(x,list) else x
        for a in self.bitext:
            invertedDict[a] = [(flatten(t),s) for (s,t) in self.bitext[a]]
        return AlignedSubtitles(invertedDict, self.targetLang, self.sourceLang)


    
    def extractData(self, nbDirs, addAlternatives=False):
        
        alignedData = AlignedSubtitles({}, self.sourceLang, self.targetLang)
        
        if len(self.bitext) < 20:
            raise RuntimeError("not enough data to divide")
        
        print "Sorting data by number of duplicates"
        nbEntries = collections.defaultdict(int)
        for a in self.bitext.keys():
            nbEntries[a.getUp()] += 1
        directories = sorted(nbEntries.keys(), key=lambda x : nbEntries[x])
        
        while len(alignedData.bitext) < nbDirs:
            testDir = directories.pop()
            print "Extracting best alignments for " + testDir
            alignsInDir = self.extractSubset([x for x in self.bitext if testDir in x])
            if addAlternatives:
                alignsInDir.addAlternatives()
            alignedDocs= alignsInDir.bitext.keys()
            alignedDocs.sort(key=lambda x: max([len(y) for y in alignsInDir.bitext[x]]))
            bestAlignment = alignsInDir.extractSubset([alignedDocs[-1]])
            alignedData.addSubtitles(bestAlignment)

            self.removeDirs([testDir])

        return alignedData   
    

    
    def generateMosesFiles(self, stem):
        nbTranslations = self.getNbAlternativeTranslations()
        print ("Generating bitexts %s.%s -> %s.%s (number of translations: %i)"
               %(stem, self.sourceLang, stem, self.targetLang, nbTranslations))
        
        srcFile = open(stem+"." + self.sourceLang, 'w')
        trgFile = open(stem + "." + self.targetLang, 'w')
        altFiles = []
        if nbTranslations > 1:      
            altFiles = [open((trgFile.name + str(i)), 'w')
                        for i in range(0, nbTranslations)]
        
        # Sorted by year, then number
        
        prefix = os.path.commonprefix(self.bitext.keys())
        alignKeys = [x.replace(prefix, "") for x in list(self.bitext.keys())]
        alignKeys.sort(key=lambda x : opushash(x))              
                           
        for document in alignKeys:
            document = prefix + document
            for pair in self.bitext[document]:
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

            
     
    def divideData(self, nbTuningFiles=2, nbDevFiles=5, nbTestFiles=5):
        
        trainingData = AlignedSubtitles(self.bitext, self.sourceLang, self.targetLang)
        
        print "Extracting test data"
        testData = trainingData.extractData(nbTestFiles, True)        
     
        print "Extracting development data"
        devData = trainingData.extractData(nbDevFiles, True)        

        print "Extracting tuning data"
        tuneData = trainingData.extractData(nbTuningFiles, False)        
        
        return trainingData, tuneData, devData, testData
        


class XCESCorpus(AlignedSubtitles):
    
    def __init__(self, xcesFile):
        self.xcesFile = Path(xcesFile)
        print "Parsing file " + xcesFile
        tree = etree.parse(str(xcesFile))
        self.xmlRoot = tree.getroot()        
        for linkGrp in self.xmlRoot:
            if linkGrp.tag == 'linkGrp':
                self.sourceLang = linkGrp.attrib['fromDoc'].split("/")[0] 
                self.targetLang = linkGrp.attrib['toDoc'].split("/")[0] 
                break     
           
        print "Opening zipped files in same directory..."
        self.subtitles = self.loadTarFiles()
                    
        print "Source lang: %s, target lang: %s"%(self.sourceLang, self.targetLang)
        bitext = self.getBitext()
        AlignedSubtitles.__init__(self, bitext, self.sourceLang, self.targetLang)
        print "Finished parsing file " + xcesFile
        
        self.rezipTarFiles()
     
                    
    def loadTarFiles(self):
        subtitles = {}
        for fileInDir in self.xcesFile.getUp().listdir():
            filePath = self.xcesFile.getUp() + "/" + fileInDir
            
            if filePath.endswith(".tar.gz"):
                print "Decompressing file " + filePath               
                zipped = gzip.open(filePath, 'rb')
                unzipped = open(filePath.replace(".gz", ""), 'wb')
                unzipped.write(zipped.read())
                zipped.close()
                unzipped.close()
                filePath = unzipped.name
                
            if not filePath.endswith(".tar"):
                continue
            
            tarFile = tarfile.open(filePath, 'r') 
            lang = re.search(r"OpenSubtitles201(2|3/xml)/(\w+)",
                             tarFile.getnames()[0]).group(2)
            if not lang == self.sourceLang and not lang == self.targetLang:
                continue
            
            for tari in tarFile:
                if not tari.issym():
                    if subtitles.has_key(tari.name):
                        print "Problem: two occurrences of " + tari.name
                    subtitles[tari.name] = tarFile,tari.offset_data, tari.size
            print "Finished processing file " + fileInDir
            tarFile.close()
        return subtitles
    
    
    def rezipTarFiles(self):
        for fileInDir in self.xcesFile.getUp().listdir():
            if fileInDir.endswith(".tar"):
                f_in = open(self.xcesFile.getUp() + "/" + fileInDir, 'rb')
                f_out = gzip.open(self.xcesFile.getUp() + "/" + fileInDir + ".gz", 'wb')
                f_out.writelines(f_in)
                f_out.close()
                f_in.close()
        print "Tar files rezipped"
        
                                           
    def getBitext(self):
        
        print "Extracting alignments"
        bitext = {}
        for l in range(0, len(self.xmlRoot)):
            linkGrp = self.xmlRoot[l]
            if linkGrp.tag == 'linkGrp':
                todoc = linkGrp.attrib['fromDoc']
                fromLines = self.extractLines(todoc)
                toLines =  self.extractLines(linkGrp.attrib['toDoc'])
                           
                alignmentList = []
                for link in linkGrp:
                    if link.tag == 'link':
                        split = link.attrib["xtargets"].split(";")
                        sourceLines = [int(i) for i in split[0].strip().split(" ") if len(i)>0]
                        targetLines = [int(i) for i in split[1].strip().split(" ") if len(i)>0]                 
                        if (len(sourceLines) == 0 or len(targetLines)==0 
                            or len(sourceLines) >2 or len(targetLines) > 2):
                            continue    
                        try:   
                            sourceLine = " ".join([fromLines[s-1].strip() for s in sourceLines])
                            targetLine = " ".join([toLines[s-1].strip() for s in targetLines])
                        except IndexError:
                            print "error when processing file %i, %s and %s"%(todoc,str(sourceLines), str(targetLines))
                            continue
                        if sourceLine and targetLine:
                            alignmentList.append((sourceLine, targetLine))
                
                if len(alignmentList) > (2*len(linkGrp)/3):
                    bitext[todoc] = alignmentList
                    
            if not (l % (len(self.xmlRoot)/min(100,len(self.xmlRoot)))):
                print ("%i aligned files already processed (%i %% of %i):"
                       %(l+1, (l*100/len(self.xmlRoot)), len(self.xmlRoot))
                       + " %i stored and %i discarded."%(len(bitext), (l+1)-len(bitext)))
          
        print "Percentage of discarded pairs: %i %%"%((len(self.xmlRoot)-len(bitext))
                                                      *100/len(self.xmlRoot))
        return bitext

 
    def extractLines(self, doc):   
        for expansion in ["OpenSubtitles2012/", "OpenSubtitles2013/xml/"]:
            if self.subtitles.has_key(expansion+doc):
                tarFile = self.subtitles[expansion+doc][0]
                offset, size = self.subtitles[expansion+doc][1:]
                tarFile.seek(offset,0)
                gzippedData = tarFile.read(size)
                zippedFile = gzip.GzipFile(fileobj=BytesIO(gzippedData))
                root = etree.parse(zippedFile).getroot()
                lines = []
                for s in root:
                    if s.tag == 's':
                        line = getLine(s)
                        lines.append(line)
                print "finished processing file " + doc
                return lines
        raise RuntimeError("could not find file " + doc)
    
     
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
    #    line = unicode(line).translate({ord(u"\u201c"):ord('"'), ord(u"\u201d"):ord('"'),
    #                           ord(u"\u201e"):ord('"'), ord(u"\u201f"):ord('"'),
    #                           ord(u"\u2013"):ord('-')})
        line = re.sub(r"\|", "_", line)
        return (line + "\n").encode('utf-8')
                
                

              
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


