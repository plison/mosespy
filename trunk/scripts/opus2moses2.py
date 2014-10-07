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


"""Scripts to generate Moses-style bitexts from OPUS alignments (in XCES format), 
and divide the data into training, tuning, development and test sets.  To run the 
script, simply use:
    opus2moses2.py XCES_file [-s unigram_file_for_source] [-t unigram_file_for_target]

To extract the aligned sentence pairs, the corpus files for each language must be present 
in the same directory as the XCES_file (in .tar or .tar.gz format). The script will 
automatically open the relevant tar files and extract their content.

The result of the script will be the following collection of files (written in the
same directory as the XCES_file):
 - {XCES_file}.train.{source lang} and {XCES_file}.train.{target lang} correspond
    to the training set.
- {XCES_file}.tune.{source lang} and {XCES_file}.tune.{target lang} correspond
    to the tuning set.
- {XCES_file}.dev.{source lang} and {XCES_file}.dev.{target lang} correspond
    to the development set.  If alternative translations are available, 
    they are generated in new files with suffixes 0,1,2,...
- {XCES_file}.test.{source lang} and {XCES_file}.test.{target lang} correspond
    to the held-out test set.  If alternative translations are available, 
    they are generated in new files with suffixes 0,1,2,...    

The script prunes low-quality alignments from the bitexts, and can also perform
spell-checking to correct OCR errors. For this spellcheck, a file containing the 
unigrams for the corresponding language must be provided (each line containing
the word, a space, and a frequency number, as in the Google unigrams).

"""
__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date:: 2014-08-25 08:30:46 #$"

from io import BytesIO
import  os, math, sys, re, collections, tarfile, gzip
import codecs, random, unicodedata, string, time
from Queue import Queue
from threading import Thread
import xml.etree.cElementTree as etree


class AlignedDocs(object):
    """Representation of a set of aligned documents.
        
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
        return AlignedDocs(invertedDict, self.targetLang, self.sourceLang)
    
    
    def splitData(self):
        """Splits the AlignedDoc into two AlignedDoc objects, each AlignedDoc 
        containing half the documents.
        
        """
        docIds = list(self.bitext.keys())
        random.shuffle(docIds)
        part1 = extraction(self.bitext, docIds[0:len(docIds)/2])
        part2 = extraction(self.bitext, docIds[len(docIds)/2:])
        return (AlignedDocs(part1, self.sourceLang, self.targetLang),
                AlignedDocs(part2, self.sourceLang, self.targetLang))
        
              
      
    def divideData(self, nbTuningFiles=2, nbTestFiles=20):
        """Divides the aligned documents into three subsets that respectively
        corresponds to the training, tuning and test sets. 
        
        For the test set, alternative translations (in the same directory as 
        the documents initially selected) are extracted.
        
        """
        trainingData = AlignedDocs(self.bitext, self.sourceLang, self.targetLang)
        
        print "Extracting test data"
        testData = trainingData.extractData(nbTestFiles)        
        testData = MultiAlignedDocs(testData)      
     
        print "Extracting tuning data"
        tuneData = trainingData.extractData(nbTuningFiles)
        
        return trainingData, tuneData, testData
        
   
    def spellcheck(self, srcDic, trgDic, correct=True, dumpCorrections=True):

        totalNbLines = sum([len(self.bitext[d]) for d in self.bitext])
        counter = 0
        for doc in self.bitext:
            bitextdoc = self.bitext[doc]
            for i in range(0, len(bitextdoc)):
                srcLine = bitextdoc[i][0]
                trgLine = bitextdoc[i][1]
                
                # If the two aligned sentences are identical, skip the corrections
                # (for e.g. cases where the subtitles are song lyrics)
                if strip(srcLine) == strip(trgLine):
                    continue
                
                newSrcWords = []
                newTrgWords = []
                for w in srcLine.split():
                    
                    #If the word does not start with a letter or can also
                    # be found in the aligned sentence, we skip corrections
                    if not w[0].isalpha() or w in trgLine or not srcDic:
                        newSrcWords.append(w)
                    else:
                        corrected = srcDic.spellcheck(w)
                        newSrcWords.append(corrected if correct else w)
                for w in trgLine.split():
                    if not w[0].isalpha() or w in srcLine or not trgDic:
                        newTrgWords.append(w)
                    else:
                        corrected = trgDic.spellcheck(w)
                        newSrcWords.append(corrected if correct else w)
                        
                bitextdoc[i] = (" ".join(newSrcWords),
                                " ".join(newTrgWords))
                
                counter += 1
                if not (counter % (totalNbLines/min(100,totalNbLines))):
                    print ("%i lines already spell-checked (%i %% of %i):"
                           %(counter, (counter*100/totalNbLines), totalNbLines))
          
        if srcDic:
            print "Number of corrections in source: %i"%(srcDic.getNbCorrections())
            if dumpCorrections:
                srcDic.dumpCorrections()
        if trgDic:
            print "Number of corrections in target: %i"%(trgDic.getNbCorrections())
            if dumpCorrections:
                trgDic.dumpCorrections()


                       
            
        
    def generateMosesFiles(self, stem):
        """Generates the moses files from the aligned documents. The 
        generated files will be stem.{sourceLang} and stem.{targetLang}.
        
        """
        print ("Generating bitexts %s.%s -> %s.%s"
               %(stem, self.sourceLang, stem, self.targetLang))
        
        srcFile = open(stem+"." + self.sourceLang, 'w')
        trgFile = open(stem + "." + self.targetLang, 'w')
             
        for document in sorted(self.bitext.keys()):
            for pair in self.bitext[document]:
                if pair[0] and pair[1]:
                    srcFile.write(pair[0])
                    trgFile.write(pair[1][0] if isinstance(pair[1],list) else pair[1])
        
        srcFile.close()
        trgFile.close()
            
    
    def extractData(self, nbDirs):
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
            nbEntries[os.path.dirname(a)] += 1
        directories = sorted(list(nbEntries.keys()), key=lambda x : nbEntries[x])
              
        while len(extractedBitext) < nbDirs and len(directories)>0:
            testDir = directories.pop()
            print "Extracting best alignments for " + testDir
            subset = extraction(self.bitext, [x for x in self.bitext if testDir in x])
            alignedDocs= subset.keys()
            alignedDocs.sort(key=lambda x: max([len(y) for y in subset[x]]))
            bestAlignment = extraction(self.bitext, [alignedDocs[-1]])
            extractedBitext.update(bestAlignment)
            self._removeDirs([testDir])

        return AlignedDocs(extractedBitext, self.sourceLang, self.targetLang)   
    
  
    def _removeDirs(self, dirsToRemove):
        """Removes a set of directories from the aligned subtitles"""
        newAligns = {}
        for a in self.bitext:
            if os.path.dirname(a) not in dirsToRemove:
                newAligns[a] = self.bitext[a]
        self.bitext = newAligns
 

def extraction(fullDic, subkeys):
    """Extracts a new dictionary that only contains the provided keys"""
    return reduce(lambda x, y: x.update({y[0]:y[1]}) or x, 
        map(None, subkeys, map(fullDic.get, subkeys)), {})
        


class MosesAlignment(AlignedDocs):
    """Representation of a Moses-style bitext."""
    
    def __init__(self, stem, sourceLang, targetLang):
        bitext = {}
        with open(stem + "." + sourceLang, 'r') as sourceFile:
            sourceLines = sourceFile.readlines()
        with open(stem + "." + targetLang, 'r') as targetFile:
            targetLines = targetFile.readlines()
        bitext[stem] = []
        for i in range(0, len(sourceLines)):
            pair = (sourceLines[i].strip(), targetLines[i].strip())
            bitext[stem].append(pair)
        AlignedDocs.__init__(self, bitext, sourceLang, targetLang)
        print "finished creating moses alignment"


class MultiAlignedDocs(AlignedDocs):
    """Representation of a 'multi-alignment', where each source sentence
    can be aligned to multiple alternative translations for the target.
    
    Such multi-alignments are useful to provide alternative translations
    for the calculation of BLEU scores.
    
    """
    
    def __init__(self, docs):
        """Creates a new multi-alignment."""
        bitext = self._getMultiBitext(docs.bitext)
        AlignedDocs.__init__(self, bitext, docs.sourceLang, docs.targetLang)
    
        
    def _getMultiBitext(self, bitext):
        """Extracts the alternative translations from the set of aligned documents."""
        
        if len(bitext)== 0:
            return bitext
        elif isinstance(bitext[bitext.keys()[0]][0][1], list):
            return bitext

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
        """Inverts the bitext (source becomes target and vice versa)."""
        
        basicInv = AlignedDocs.getInverse(self)
        return MultiAlignedDocs(basicInv)

   
    def splitData(self):
        """Splits the bitext in two parts of equal size."""
        
        split1, split2 = AlignedDocs.splitData(self)
        return MultiAlignedDocs(split1), MultiAlignedDocs(split2)
        
        
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
        generated files will be stem.{sourceLang} and stem.{targetLang},
        with  suffixes 0, 1, 2,... when alternative translations
        are available.
        
        """
        
        AlignedDocs.generateMosesFiles(self, stem)
        
        nbTranslations = self.getNbAlternativeTranslations()
        print "Generating %i alternative translations"%(nbTranslations)
  
        altFiles = []
        if nbTranslations > 1:      
            altFiles = [open((stem + "." + self.targetLang + str(i)), 'w')
                        for i in range(0, nbTranslations)]
        
   
        for document in sorted(self.bitext.keys()):
            for pair in self.bitext[document]:
                if pair[0] and pair[1]:
                    for i in range(0, len(altFiles)):
                        altLine = pair[1][i] if i < len(pair[1]) else ""
                        altFiles[i].write(altLine)
        
        for altFile in altFiles:
            altFile.close()


        
class XCESCorpus(AlignedDocs):
    """Representation of an XCES file that contains aligned documents.
    
    """
    
    def __init__(self, xcesFile, rezipFiles=False):
        """Creates a new XCESCorpus object from the file cxesFile.
        
        Args:
            xcesFile (str): path to the xcesFile
            rezipFiles (bool): whether to rezip the tar files after
                the extraction of the aligned documents.
        
        """
        self.xcesFile = xcesFile
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
        AlignedDocs.__init__(self, bitext, self.sourceLang, self.targetLang)
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
        tarPaths = self._getRelevantTarFiles()
        
        print "Opening tarred files in same directory..."
        for tarPath in tarPaths: 
            
            if tarPath.endswith(".tar.gz"):
                print "Decompressing file " + tarPath               
                zipped = gzip.open(tarPath, 'rb')
                unzipped = open(tarPath.replace(".gz", ""), 'wb')
                unzipped.write(zipped.read())
                zipped.close()
                unzipped.close()
                os.remove(tarPath)
                tarPath = unzipped.name
            
            tarFile = tarfile.open(tarPath, 'r')          
            for tari in tarFile:
                if not tari.issym():
                    tarkey = tari.name[max(tari.name.find("/"+self.sourceLang+"/"), 
                                           tari.name.find("/"+self.targetLang+"/"))+1:]
                    subtitles[tarkey] = tarPath,tari.offset_data, tari.size
            print "Finished processing file " + tarPath
            tarFile.close()
        return subtitles
  
                                       
    def getBitext(self, nbThreads = 16):
        """Extracts the bitext from the XCES corpus.  The bitext is a set of aligned
        documents, each document being composed of a list of aligned pairs
        (sourceLine, targetLine).
        
        In order to work, the corresponding corpus files (in .tar or .tar.gz format) 
        must be present in the same directory as the XCES file.
        
        The method prunes the following alignments: (1) empty or greatly unbalanced
        aligned pairs (2) documents for which the resulting alignment list is less than 
        two third of the original alignments in the XCES file (which often indicates
        that the two subtitles refer to different sources).
        
        """       
        print "Extracting alignments"
        bitext = {}
        queues = []
        
        linkGrps = [c for c in self.xmlRoot.getChildren() if c.tag == 'linkGrp']
        for linkGrp in linkGrps: 
            
            resultQueue = Queue()
            t = Thread(target=self._readGroup, args= ((linkGrp, resultQueue)))
            t.start()
            queues.append(resultQueue)
            
            while len(queues) == nbThreads:
                for finished in [q for q in queues if not q.empty()]:
                    bitext[linkGrp.attrib['fromDoc']] = finished.get()
                    queues.remove(finished)
                if len(finished) == 0:
                    time.sleep(0.1)
                               
            if not (linkGrp % (len(self.xmlRoot)/min(100,len(self.xmlRoot)))):
                nbReals = len([d for d in bitext.keys() if bitext[d]])
                print ("%i aligned files already processed (%i %% of %i):"
                       %(len(bitext), (len(bitext)*100/len(self.xmlRoot)), len(self.xmlRoot))
                       + " %i stored and %i discarded."%(nbReals, len(bitext)-nbReals))              
                           
        while len(queues) > 0:
            for finished in [q for q in queues if not q.empty()]:
                bitext[linkGrp.attrib['fromDoc']] = finished.get()
                queues.remove(finished)
            if len(finished) == 0:
                time.sleep(0.1)
            
          
        for d in list(bitext.keys()):
            if not bitext[d]:
                del bitext[d]
        print ("Percentage of discarded pairs: %i %%"
               %((len(self.xmlRoot)-len(bitext))*100/len(self.xmlRoot)))
        return bitext
    
    
    
    def _readGroup(self, linkGrp, resultQueue):

        #Extracting the source and target lines
        fromLines = self._extractLines(linkGrp.attrib["fromDoc"])
        toLines =  self._extractLines(linkGrp.attrib["toDoc"])
                   
        alignmentList = []
        for link in [l for l in linkGrp if l.tag=='link']:
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
                print "alignment error with file %s"%(linkGrp.attrib["fromDoc"])
                continue
            
            if sourceLine and targetLine:
                alignmentList.append((normalise(sourceLine), 
                                      normalise(targetLine)))
        
        # If the resulting list of alignments is less than two thirds of the
        # original number of alignments, discard the document
        if len(alignmentList) > (2*len(linkGrp)/3):
            resultQueue.put(alignmentList)
        else:
            resultQueue.put(None)
            
 
    def _extractLines(self, doc): 
        """Extracts the list of lines from the document.  The list of 
        subtitle documents must already be generated  in self.subtitles
        
        """
        
        if self.subtitles.has_key(doc):
            tarFile = open(self.subtitles[doc][0])
            offset, size = self.subtitles[doc][1:]
            tarFile.seek(offset,0)
            gzippedData = tarFile.read(size)
            zippedFile = gzip.GzipFile(fileobj=BytesIO(gzippedData))
            root = etree.parse(zippedFile).getroot()
            lines = {}
            for s in root:
                if s.tag == 's':
                    lineId = int(s.attrib["id"])
                    wordList = []
                    toProcess = s.getchildren()
                    while len(toProcess) > 0:
                        w = toProcess.pop()
                        if w.tag == 'w' and w.text != None:
                            wordList.append(w.text.strip())
                        else:
                            toProcess.extend(w.getchildren())      
                        lines[lineId] = " ".join(wordList)
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


    def _getRelevantTarFiles(self, maxNbLines=10):
        """Returns the tar files that are relevant for the bitext (i.e. that 
        contains some of the documents referred to in the XCES file).
        
        """
        rootDir = os.path.dirname(self.xcesFile) + "/"
        tarFiles = [rootDir + f for f in os.listdir(rootDir) 
                    if (f.endswith(".tar") or f.endswith(".tar.gz"))]
        tarFiles.sort()
        relevantTars = []
        for tarFile in tarFiles:
            f = gzip.open(tarFile) if tarFile.endswith(".gz") else open(tarFile)
            nbLines = 0
            for l in f:
                if re.search("/("+self.sourceLang+"|"+self.targetLang+")/", l):
                    relevantTars.append(tarFile)
                nbLines += 1
                if nbLines == maxNbLines:
                    break
        return relevantTars

     
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
            os.remove(tarPath)
        print "Tar files rezipped"
        
       
class Dictionary():
    """Representation of a dictionary containing a list of words for a given 
    language along with their unigram frequencies. The dictionary is used
    to perform spell-checking of the documents, and correct common errors
    (such as OCR errors and wrong accents).
    
    """
    def __init__(self, dicFile):
        """Creates a new dictionary from a given file.  Each line in the file 
        must contain a word followed by a space or tab and an integer 
        representing the frequency of the word.
        
        """
        if not os.path.exists(dicFile):
            raise RuntimeError("Unigrams file " + dicFile + " cannot be found")
        self.dicFile = dicFile
        self.words = collections.defaultdict(int)
        with codecs.open(dicFile, encoding='utf-8') as dico:
            for l in dico:
                if not l.startswith("%%") and not l.startswith("#"):
                    split = l.split()
                    word = split[0].strip().encode("utf-8")
                    frequency = int(split[1].strip())
                    self.words[word] = frequency
        
        self.corrections =  collections.defaultdict(int)
        print "Total number of words in dictionary: %i"%(len(self.words))
        
        self.no_accents = {}
        if re.search(r"[\xa8\xa9\xa0\xb9]", " ".join(self.words.keys()[0:100])):
            for w in self.words:
                stripped = strip(w)
                if (not self.no_accents.has_key(stripped) or 
                    self.words[w] > self.words[self.no_accents[stripped]]):
                    self.no_accents[stripped] = w
      
               
    def spellcheck(self, word):
        """Spell-check the word.  The method first checks if the word is in the
        dictionary.  If yes, the word is returned.  Else, the method search for
        a possible correction, and returns it.  If no correction could be found,
        the initial word is returned.  The word and its correction are recorded
        in the object self.corrections.
        
        """        
        isKnown = self.isWord(word)
        correction = self.correct(word) if not isKnown else word
        if not isKnown:
            self.corrections[(word,correction if correction!= word else "?")] += 1
        return word


    def isWord(self, word):
        """Returns true if the (lowercased) word can be found in the dictionary,
        and false otherwise.
        
        """
        wlow = word.decode("utf-8").lower().encode("utf-8")
        return wlow in self.words or re.sub(r"['-]","",wlow) in self.words
    

    def getNbCorrections(self):
        """Returns the number of corrections recorded so far by the dictionary.
        
        """
        return sum([self.corrections[(i,j)] for (i,j) in self.corrections if j!="?"])
    
    def dumpCorrections(self):
        """Dumps the corrections to a file named {dictionary file}.corrections."""
        with open(self.dicFile +".corrections") as dump:
            dump.write("\n".join(["%s -> %s"%(p1,p2) for (p1,p2) in self.corrections]))

            
    def getWords(self):
        """Returns the (word,frequency) pairs in the dictionary."""
        return self.words
    
  
    def getFrequency(self, word):
        """Returns the frequency of the word in the dictionary."""
        wlow = word.decode("utf-8").lower().encode("utf-8")
        if wlow in self.words:
            return self.words[wlow]
        elif re.sub(r"['-]","",wlow):
            return self.words[re.sub(r"['-]","",wlow)]
        else:
            return 0


    def correct(self, word):
        """Finds the best correction for the word, if one can be found.  The
        method tries to correct common OCR errors, wrong accents, and a few 
        other heuristics.
        
        """
        
        # OCR errors
        mappings = [("ii", "ll"), ("II", "ll"), ("l", "I"), ("i", "l"), ("I", "l"), ("l", "i")]
        
        replaces = []
        for m in mappings:
            matches = re.finditer(r"(?=%s)"%(m[0]), word)
            for match in matches:
                pos = match.start()
                replace = word[:pos] + m[1] + word[pos+len(m[0]):]
                if self.isWord(replace):
                    replaces.append(replace)
        if replaces:
            return max(replaces, key=self.getFrequency) 
  
        # Wrong accents
        if self.no_accents and not self.isWord(word):
            no_accent = strip(word)
            if self.no_accents.has_key(no_accent):
                return self.no_accents[no_accent]
        
        # correcting errors such as "entertainin" --> "entertaining"
        if word.endswith("in") and self.isWord(word + "g"):
            return word + "g"
                  
        return word
    
    
    
def normalise(line):
    """Normalises the string and its encoding."""
    
    line = line.strip()
    line = re.sub(r"\s+", " ", line)
    line = re.sub(r"[\x00-\x1f\x7f\n]", " ", line)
    line = re.sub(r"\<(s|unk|\/s|\s*and\s*|)\>", "", line)
    line = re.sub(r"\<(S|UNK|\/S)\>", "", line)
    line = re.sub(r"\[\s*and\s*\]", "", line)
    line = re.sub(r"\|", "_", line)
    return (line + "\n").encode('utf-8')
            
        
def strip(word):
    """Strips the word of accents and punctuation."""
    
    normalised = unicodedata.normalize('NFKD',word.decode("utf-8"))
    stripped = normalised.encode("ascii", "replace").lower()
    stripped= stripped.translate(string.maketrans("",""), string.punctuation)
    return stripped
   
   
                 
if __name__ == '__main__':
  
    if len(sys.argv) < 2:
        print ("Usage: opus2moses2.py  XCESFile " 
              + "[-s file_with_source_unigrams] [-t file_with_target_unigrams]")
        
    else:  
        
        # STEP 1: process XCES file
        xmlFile = sys.argv[1]
        corpus = XCESCorpus(xmlFile)
        baseStem = xmlFile.replace(".xml", "")
        
        # STEP 2: spell-check the bitext
        dic_source, dic_target = None, None
        for argi in range(2, len(sys.argv)):
            if sys.argv[argi]=="-s":
                dic_source =Dictionary(sys.argv[argi+1])
            elif sys.argv[argi] =="-t":
                dic_target =Dictionary(sys.argv[argi+1])
        corpus.spellcheck(dic_source, dic_target)

        # STEP 3: divide bitext into training, tuning, dev and test sets
        train, tune, devAndTest = corpus.divideData()
        dev, test = devAndTest.splitData()
        
        # STEP 4: remove existing files
        for inDir in os.listdir(os.path.dirname(baseStem)):
            if any([(baseStem+"."+part) in inDir for part in ["train","tune","dev","test"]]):
                os.remove(inDir)
        
        # STEP 5: generates Moses-files for each set
        train.generateMosesFiles(baseStem + ".train")
        tune.generateMosesFiles(baseStem + ".tune")
        dev.generateMosesFiles(baseStem + ".dev")
        test.generateMosesFiles(baseStem+ ".test")
        devInv = dev.getInverse()
        devInv.generateMosesFiles(baseStem + ".dev")
        testInv = test.getInverse()
        testInv.generateMosesFiles(baseStem + ".test")


