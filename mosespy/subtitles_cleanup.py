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
import mosespy.system as system
import xml.etree.cElementTree as etree



class AlignedSubtitles(object):
    
    def __init__(self, bitext, sourceLang, targetLang):
        self.bitext = bitext
        self.sourceLang = sourceLang
        self.targetLang = targetLang
          

    def extractSubset(self, subsetkeys):
        subdico = reduce(lambda x, y: x.update({y[0]:y[1]}) or x,
                  map(None, subsetkeys, map(self.bitext.get, subsetkeys)), {})
        print "size of subdico with keys %s : %i"%(str(subsetkeys), len(subdico))
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
        
 #       if len(self.bitext) < 20:
 #           raise RuntimeError("not enough data to divide")
        
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
    """Representation of an XCES file that contains aligned documents.
    
    """
    
    def __init__(self, xcesFile, rezipFiles=False):
        """Creates a new XCESCorpus object from the file cxesFile.
        
        Args:
            xcesFile (str): path to the xcesFile
            rezipFiles (bool): whether to rezip the tar files after
                the extraction of the aligned documents.
        
        """
        self.xcesFile = Path(xcesFile)
        print "Parsing file " + xcesFile
        tree = etree.parse(str(xcesFile))
        self.xmlRoot = tree.getroot()        
        for linkGrp in self.xmlRoot:
            if linkGrp.tag == 'linkGrp':
                self.sourceLang = linkGrp.attrib['fromDoc'].split("/")[0] 
                self.targetLang = linkGrp.attrib['toDoc'].split("/")[0] 
                break     
           
        self.subtitles = self._loadTarFiles()
                    
        print "Source lang: %s, target lang: %s"%(self.sourceLang, self.targetLang)
        bitext = self.getBitext()
        AlignedSubtitles.__init__(self, bitext, self.sourceLang, self.targetLang)
        print "Finished parsing file " + xcesFile
        
        if rezipFiles:
            self._rezipTarFiles()
     
                    
    def _loadTarFiles(self):
        """Loads the tar files that correspond to the corpus files for the
        XCES alignments.  The files can be in .tar or .tar.gz format (in which
        case they are uncompressed).  A list of subtitle documents (with the
        detailed location in each tar file) is generated from these files.
        
        """
        subtitles = {}
        rootDir = self.xcesFile.getUp() + "/"
        tarPaths = [Path(rootDir + f) for f in rootDir.listdir() 
                    if (f.endswith(".tar") or f.endswith(".tar.gz"))]
        
        print "Opening tarred files in same directory..."
        for tarPath in tarPaths:            
            match = tarPath.searchMatch(r"OpenSubtitles201(2|3/xml)/(\w+)")
            if not match or (match.group(2) != self.sourceLang 
                             and match.group(2) != self.targetLang):
                continue 
                 
            if tarPath.endswith(".tar.gz"):
                print "Decompressing file " + tarPath               
                zipped = gzip.open(tarPath, 'rb')
                unzipped = open(tarPath.replace(".gz", ""), 'wb')
                unzipped.write(zipped.read())
                zipped.close()
                unzipped.close()
                tarPath.remove()
                tarPath = unzipped.name
            
            tarFile = tarfile.open(tarPath, 'r')          
            for tari in tarFile:
                if not tari.issym():
                    if subtitles.has_key(tari.name):
                        print "Problem: two occurrences of " + tari.name
                    subtitles[tari.name] = tarPath,tari.offset_data, tari.size
            print "Finished processing file " + tarPath
            tarFile.close()
        return subtitles
  
                                           
    def getBitext(self):
        """Extracts the bitext from the XCES corpus.  The bitext is a set of aligned
        documents, each document being composed of a list of aligned pairs
        (sourceLine, targetLine).
        
        In order to work, the corresponding corpus files (in .tar or .tar.gz format) 
        must be present in the same directory as the XCES file.
        
        The method prunes the following alignments: (1) empty or seriously unbalanced
        aligned pairs (2) documents for which the resulting alignment list is less than 
        two third of the original alignments in the XCES file (which often indicates
        that the two subtitles refer to different sources).
        
        """       
        print "Extracting alignments"
        bitext = {}
        for l in range(0, len(self.xmlRoot)):
            linkGrp = self.xmlRoot[l]
            if linkGrp.tag == 'linkGrp':
                todoc = Path(linkGrp.attrib['fromDoc'])
                
                #Extracting the source and target lines
                fromLines = self._extractLines(todoc)
                toLines =  self._extractLines(linkGrp.attrib['toDoc'])
                           
                alignmentList = []
                for link in linkGrp:
                    if link.tag == 'link':
                        split = link.attrib["xtargets"].split(";")
                        srcLineIndices = [int(i) for i in split[0].strip().split(" ") if len(i)>0]
                        trgLineIndices = [int(i) for i in split[1].strip().split(" ") if len(i)>0]
                                      
                        # Pruning out empty or seriously unbalanced alignments   
                        if (len(srcLineIndices) == 0 or len(trgLineIndices)==0 
                            or len(srcLineIndices) >2 or len(trgLineIndices) > 2):
                            continue    
                        try:   
                            sourceLine = " ".join([fromLines[j-1].strip() for j in srcLineIndices])
                            targetLine = " ".join([toLines[j-1].strip() for j in trgLineIndices])
                        except IndexError:
                            print "alignment error with file %s"%(todoc)
                            continue
                        
                        if sourceLine and targetLine:
                            alignmentList.append((sourceLine, targetLine))
                
                # If the resulting list of alignments is less than two thirds of the
                # original number of alignments, discard the document
                if len(alignmentList) > (2*len(linkGrp)/3):
                    bitext[todoc] = alignmentList
                    
            if not (l % (len(self.xmlRoot)/min(100,len(self.xmlRoot)))):
                print ("%i aligned files already processed (%i %% of %i):"
                       %(l+1, (l*100/len(self.xmlRoot)), len(self.xmlRoot))
                       + " %i stored and %i discarded."%(len(bitext), (l+1)-len(bitext)))
          
        print ("Percentage of discarded pairs: %i %%"
               %((len(self.xmlRoot)-len(bitext))*100/len(self.xmlRoot)))
        return bitext

 
    def _extractLines(self, doc): 
        """Extracts the list of lines from the document.  The list of 
        subtitle documents must already be generated  in self.subtitles
        
        """
        for expansion in ["OpenSubtitles2013/xml/", "OpenSubtitles2012/"]:
            if self.subtitles.has_key(expansion+doc):
                tarFile = open(self.subtitles[expansion+doc][0])
                offset, size = self.subtitles[expansion+doc][1:]
                tarFile.seek(offset,0)
                gzippedData = tarFile.read(size)
                zippedFile = gzip.GzipFile(fileobj=BytesIO(gzippedData))
                root = etree.parse(zippedFile).getroot()
                lines = {}
                for s in root:
                    if s.tag == 's':
                        lineId = int(s.attrib["id"])
                        lines[lineId] = getLine(s)
                tarFile.close()
                linesList = []
                for i in range(1, max(lines.keys())+1):
                    if lines.has_key(i):
                        linesList.append(lines[i])
                    else:
                        print "Missing line number %i in %s"%(i,doc)
                        linesList.append("")
                return linesList
                    
        raise RuntimeError("could not find file " + doc)
    
    def _rezipTarFiles(self):
        """Rezips the tar files."""
        
        unzippedFiles = set()
        for (tarPath,_,_) in self.subtitles.values():
            unzippedFiles.add(tarPath)
        print "Rezipping the tar files: %s"%(str(unzippedFiles))
            
        for tarPath in unzippedFiles:
            f_in = open(tarPath, 'rb')
            f_out = gzip.open(tarPath + ".gz", 'wb')
            f_out.writelines(f_in)
            f_out.close()
            f_in.close()
            tarPath.remove()
        print "Tar files rezipped"
        
             
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


