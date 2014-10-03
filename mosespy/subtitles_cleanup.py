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
import  math, sys, re, collections, tarfile, gzip, codecs
from mosespy.system import Path
import xml.etree.cElementTree as etree



class AlignedSubtitles(object):
    """Representation of a set of aligned subtitles, which can be divided into
    subsets (i.e. training, tuning, development and test sets), and converted
    into Moses bitext format. 
    
    """
    
    def __init__(self, bitext, sourceLang, targetLang):
        """Creates a new set of aligned subtitles with the bitext content
        as well as the source and target language codes.
        
        """
        self.bitext = bitext
        self.sourceLang = sourceLang
        self.targetLang = targetLang
    
      
    def getInverse(self):
        """Reverts the direction of the alignment (source becomes target and
        vice versa).
        
        """
        invertedDict = {}
        flatten = lambda x : x[0] if isinstance(x,list) else x
        for a in self.bitext:
            invertedDict[a] = [(flatten(t),s) for (s,t) in self.bitext[a]]
        return AlignedSubtitles(invertedDict, self.targetLang, self.sourceLang)

      
    def divideData(self, nbTuningFiles=2, nbDevFiles=5, nbTestFiles=5):
        """Divides the aligned documents into four subsets that respectively
        corresponds to the training, tuning, development and test sets. 
        
        For the development and test sets, alternative translations (in the same
        directory as the documents initially selected) are extracted.
        
        """
        trainingData = AlignedSubtitles(self.bitext, self.sourceLang, self.targetLang)
        
        print "Extracting test data"
        extractedData = trainingData._extractData(nbTestFiles)
        testData = MultiAlignedSubtitles(extractedData, self.sourceLang, self.targetLang)      
     
        print "Extracting development data"
        extractedData = trainingData._extractData(nbDevFiles)
        devData = MultiAlignedSubtitles(extractedData, self.sourceLang, self.targetLang)      

        print "Extracting tuning data"
        extractedData = trainingData._extractData(nbTuningFiles)
        tuneData = AlignedSubtitles(extractedData, self.sourceLang, self.targetLang)      
        
        return trainingData, tuneData, devData, testData
        
   
    def findUnknownWords(self):
        srcUnk =  collections.defaultdict(int)
        trgUnk =  collections.defaultdict(int)
        trgDic = Dictionary(self.targetLang)
        srcDic = Dictionary(self.sourceLang)
        for doc in self.bitext:
            bitextdoc = self.bitext[doc]
            for i in range(0, len(bitextdoc)):
                sourceLine = bitextdoc[i][0]
                targetLine = bitextdoc[i][1]
                for w in sourceLine.split():
                    if w[0].isalpha() and (w[0].islower() or w not in targetLine):  
                        w = w.lower()                                         
                        isWord = srcDic.isWord(w)
                        if not isWord:
                            srcUnk[w]  += 1
                for w in targetLine.split():
                    if w[0].isalpha() and (w[0].islower() or w not in sourceLine): 
                        w = w.lower()                     
                        isWord = trgDic.isWord(w)
                        if not isWord:
                            trgUnk[w]  += 1
                if not (i % (len(bitextdoc)/min(100,len(bitextdoc)))):
                    print ("%i lines already spell-checked (%i %% of %i):"
                           %(i, (i*100/len(bitextdoc)), len(bitextdoc)))
                        
        srcUnkList = sorted(srcUnk.keys(), key=lambda x :srcUnk[x], reverse=True)
        trgUnkList = sorted(trgUnk.keys(), key=lambda x :trgUnk[x], reverse=True)
        return srcUnkList, trgUnkList
            
        
    def generateMosesFiles(self, stem):
        """Generates the moses files from the aligned documents. The 
        generated files will be stem.{sourceLang} and stem.{targetLang}.
        
        """
        print ("Generating bitexts %s.%s -> %s.%s"
               %(stem, self.sourceLang, stem, self.targetLang))
        
        srcFile = open(stem+"." + self.sourceLang, 'w')
        trgFile = open(stem + "." + self.targetLang, 'w')
             
        for document in sorted(self.bitext.keys(),key=lambda x: docOrder(x)):
            for pair in self.bitext[document]:
                if pair[0] and pair[1]:
                    srcFile.write(normalise(pair[0]))
                    trgFile.write(normalise(pair[1]))
        
        srcFile.close()
        trgFile.close()
            
    
    def _extractData(self, nbDirs):
        """Extracts aligned documents from a number of directories (the directories
        with the largest number of documents being selected first) and returns
        the aligned subtitles for these documents.
        
        """     
        if len(self.bitext) < 20:
            raise RuntimeError("not enough data to divide")
        
        extractedBitext = {}
        print "Sorting data by number of duplicates"
        nbEntries = collections.defaultdict(int)
        for a in self.bitext.keys():
            nbEntries[a.getUp()] += 1
        directories = sorted(list(nbEntries.keys()), key=lambda x : nbEntries[x])
        
        def extraction(subkeys):
            return reduce(lambda x, y: x.update({y[0]:y[1]}) or x, 
                          map(None, subkeys, map(self.bitext.get, subkeys)), {})
      
        while len(extractedBitext) < nbDirs and len(directories)>0:
            testDir = directories.pop()
            print "Extracting best alignments for " + testDir
            subset = extraction([x for x in self.bitext if testDir in x])
            alignedDocs= subset.keys()
            alignedDocs.sort(key=lambda x: max([len(y) for y in subset[x]]))
            bestAlignment = extraction([alignedDocs[-1]])
            extractedBitext.update(bestAlignment)
            self._removeDirs([testDir])

        return extractedBitext   
    
  
    def _removeDirs(self, dirsToRemove):
        """Removes a set of directories from the aligned subtitles"""
        newAligns = {}
        for a in self.bitext:
            if a.getUp() not in dirsToRemove:
                newAligns[a] = self.bitext[a]
        self.bitext = newAligns
 


class Dictionary():
    
    def __init__(self, lang):
        dicFile = Path(__file__).getUp() + "/data/" + lang + ".dic"
        if not dicFile.exists():
            raise RuntimeError("Dictionary " + dicFile + " cannot be found")
        self.words = set()
        with codecs.open(dicFile, encoding='utf-16') as dico:
            for l in dico:
                if not l.startswith("%%") and not l.startswith("#"):
                    self.words.add(l.strip().encode("utf-8"))
        print "Total number of words in dictionary: %i"%(len(self.words))
    
    def isWord(self, word):
        return word in self.words or re.sub(r"['-]","",word) in self.words


class MosesAlignment(AlignedSubtitles):
    
    def __init__(self, stem, sourceLang, targetLang):
        bitext = {}
        sourceLines = Path(stem + "." + sourceLang).readlines() 
        targetLines = Path(stem + "." + targetLang).readlines() 
        bitext[stem] = []
        for i in range(0, len(sourceLines)):
            pair = (sourceLines[i].strip(), targetLines[i].strip())
            bitext[stem].append(pair)
        AlignedSubtitles.__init__(self, bitext, sourceLang, targetLang)
        print "finished creating moses alignment"


class MultiAlignedSubtitles(AlignedSubtitles):
    
    def __init__(self, bitext, sourceLang, targetLang):
        multiBitext = self._getMultiBitext(bitext)
        AlignedSubtitles.__init__(self, multiBitext, sourceLang, targetLang)
    
        
    def _getMultiBitext(self, bitext):
        correlatedAligns = {}
        for fromdoc in bitext:
            print "Search correlated sources for " + fromdoc
            initAligns = bitext[fromdoc]
            correlatedAligns[fromdoc] = [(s,set([t] if t else [])) for (s,t) in initAligns]
            
            # Loop on the other documents
            for otherfromDoc in [x for x in bitext if x!=fromdoc]:                              
                otherAligns = bitext[otherfromDoc]
                for i in range(0, len(initAligns)):
                    initPair = initAligns[i]  
                    
                    # We search for an identical source sentence in a window of 10*log(i+1)
                    for k in range(i-int(10*math.log(i+1)), i+int(10*math.log(i+1))):
                        otherPair = otherAligns[k] if k < len(otherAligns) else None
                        
                        # If initPair and otherPair are identical, we take their translations
                        # to be alternatives of one another  
                        if otherPair and initPair[0] == otherPair[0]:
                            correlatedAligns[fromdoc][i][1].add(otherPair[1])
                            break                         
        
        for d in correlatedAligns:
            bitext[d] = [(s,list(t)) for (s,t) in correlatedAligns[d]]
        return bitext
       
 
    def getInverse(self):
        basicInv = AlignedSubtitles.getInverse(self)
        return MultiAlignedSubtitles(basicInv.bitext, basicInv.sourceLang, 
                                     basicInv.targetLang)


        
    def getNbAlternativeTranslations(self):
        """Returns the maximum number of alternative translations for the 
        aligned subtitles.
        
        """
        nbTranslations = 1
        for a in self.bitext:
            for p in self.bitext[a]:
                if isinstance(p[1], list) and len(p[1]) > nbTranslations:
                    nbTranslations = len(p[1])
        return nbTranslations


    
    def generateMosesFiles(self, stem):
        """Generates the moses files from the aligned documents. The 
        generated files will be stem.{sourceLang} and stem.{targetLang}.
        
        """
        
        AlignedSubtitles.generateMosesFiles(self, stem)
        
        nbTranslations = self.getNbAlternativeTranslations()
        print "Generating %i alternative translations"%(nbTranslations)
  
        altFiles = []
        if nbTranslations > 1:      
            altFiles = [open((stem + "." + self.targetLang + str(i)), 'w')
                        for i in range(0, nbTranslations)]
        
   
        for document in sorted(self.bitext.keys(),key=lambda x: docOrder(x)):
            for pair in self.bitext[document]:
                if pair[0] and pair[1]:
                    for i in range(0, len(altFiles)):
                        altLine = pair[1][i] if i < len(pair[1]) else ""
                        altFiles[i].write(normalise(altLine))
        
        for altFile in altFiles:
            altFile.close()


        
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


def docOrder(path):
    year = int(path.split("/")[1])
    number = path.split("/")[2]
    result = year*1000000000
    for i in range(0, min(6,len(number))):
        result += ord(number[i])*(100000/math.pow(10,i))
    return result
   
def logList(aList, logFile):
    with open(logFile, 'w') as logFile:
        logFile.write("\n".join(aList))
    
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
        
        devInv = dev.getInverse()
        devInv.generateMosesFiles(baseStem + ".dev")
        
        testInv = test.getInverse()
        testInv.generateMosesFiles(baseStem + ".test")


