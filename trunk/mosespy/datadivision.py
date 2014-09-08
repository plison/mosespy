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

import sys, math, random, gzip, re, copy, uuid
import mosespy.slurm as slurm
from mosespy.corpus import AlignedCorpus, BasicCorpus
from mosespy.system import Path, ShellExecutor
import xml.etree.cElementTree as etree
    

def divideData(alignedStem, sourceLang, targetLang, nbTuning=1000, nbDev=3000, 
               nbTesting=3000, randomPick=True, duplicatesWindow=4):
    """Divides the aligned data into distinct parts. Since datasets (such 
    as subtitles corpora) often contain duplicates sentences, the method 
    seeks to avoid selecting sentences that can also be found in other 
    parts of the dataset.
    
    Args:
        alignedStem (str): stem for the aligned data
        sourceLang (str): source language code
        targetLang (str): target language code
        nbTuning (int): number of sentences to select for the tuning set
        nbDev (int): number of sentences for the development set
        nbTesting (int): number of sentences for the testing set
        randomPick (bool): whether to pick tuning, development and testing
            sentences randomly (if True), or at the end of the data set 
            (if False).
        duplicatesWindow (int): number of sentences to take into account
            when searching for duplicates in the data set.
    
    Returns:
        Four aligned corpora corresponding to the training, tuning,
        development and test data. 
    
    """
    corpus = AlignedCorpus(alignedStem, sourceLang, targetLang)
   
    if nbTuning + nbDev + nbTesting > corpus.countNbLines():
        raise RuntimeError("cannot divide such small amount of data")
    
    outputPath = corpus.getSourceCorpus().getUp()
    
    if randomPick:
        nbLines = corpus.countNbLines()
        toExclude = set()
        
        tuningIndices =_drawRandom(nbTuning, nbLines, exclusion=toExclude)
        toExclude = toExclude.union(tuningIndices)
        developIndices =_drawRandom(nbDev, nbLines, exclusion=toExclude)
        toExclude = toExclude.union(developIndices)
        testingIndices = _drawRandom(nbTesting,nbLines, exclusion=toExclude)
    else:
        nbLines = corpus.countNbLines()
        tuningIndices = range(0,nbLines)[-nbTuning-nbTesting-nbDev:-nbDev-nbTesting]
        developIndices = range(0,nbLines)[-nbDev-nbTesting:-nbTesting]
        testingIndices = range(0,nbLines)[-nbTesting:]

    sourceLines = corpus.getSourceCorpus().readlines()
    targetLines = corpus.getTargetCorpus().readlines()

    trainSourceLines = []
    tuneSourceLines = []
    devSourceLines = []       
    testSourceLines = []       
    print "Dividing source data..."
    for i in range(0, len(sourceLines)):
        sourceLine = sourceLines[i]
        if i in tuningIndices:
            tuneSourceLines.append(sourceLine)
        elif i in developIndices:
            devSourceLines.append(sourceLine)
        elif i in testingIndices:
            testSourceLines.append(sourceLine)
        else:
            trainSourceLines.append(sourceLine)
    
    trainTargetLines = []
    tuneTargetLines = []
    devTargetLines = []
    testTargetLines = []       
    print "Dividing target data..."
    for i in range(0, len(targetLines)):
        targetLine = targetLines[i]
        if i in tuningIndices:
            tuneTargetLines.append(targetLine)
        elif i in developIndices:
            devTargetLines.append(targetLine)
        elif i in testingIndices:
            testTargetLines.append(targetLine)
        else:
            trainTargetLines.append(targetLine)
    
    trainStem = outputPath + "/" + (corpus.stem + ".train").basename()
    (trainStem + "." + corpus.sourceLang).writelines(trainSourceLines) 
    (trainStem + "." + corpus.targetLang).writelines(trainTargetLines)
    trainCorpus = AlignedCorpus(trainStem, corpus.sourceLang, corpus.targetLang)
    
    
    tuneStem = outputPath + "/" + (corpus.stem + ".tune").basename()
    (tuneStem + "." + corpus.sourceLang).writelines(tuneSourceLines) 
    (tuneStem + "." + corpus.targetLang).writelines(tuneTargetLines)
    tuneCorpus = AlignedCorpus(tuneStem, corpus.sourceLang, corpus.targetLang)

    devStem = outputPath + "/" + (corpus.stem + ".dev").basename()
    (devStem + "." + corpus.sourceLang).writelines(devSourceLines) 
    (devStem + "." + corpus.targetLang).writelines(devTargetLines)
    devCorpus = AlignedCorpus(devStem, corpus.sourceLang, corpus.targetLang)

    testStem = outputPath + "/" + (corpus.stem + ".test").basename()
    (testStem + "." + corpus.sourceLang).writelines(testSourceLines) 
    (testStem + "." + corpus.targetLang).writelines(testTargetLines)
    testCorpus = AlignedCorpus(testStem, corpus.sourceLang, corpus.targetLang)

    return trainCorpus, tuneCorpus, devCorpus, testCorpus
    
    

def filterOutLines(fullCorpusFile, *toRemoveFiles):
    """Filters out sentences from the corpus represented by toRemoveFiles
    from the corpus in fullCorpusFile.  This method is used to prune 
    language model data from development and test sentences.
    
    """
    fullCorpus = BasicCorpus(fullCorpusFile)
    
    occurrences = {}
    histories = {}
    for toRemoveFile in toRemoveFiles:
        toRemoveCorpus= BasicCorpus(toRemoveFile)
        occurrences[toRemoveFile] = toRemoveCorpus.getOccurrences()
        histories[toRemoveFile] = toRemoveCorpus.getHistories()


    outputFile = fullCorpus.addFlag("filtered") 
    with open(outputFile, 'w', 1000000) as newLmFileD:                 
        inputLines = fullCorpus.readlines()
        skippedLines = []
        for i in range(2, len(inputLines)):
            l = inputLines[i].strip()
            toSkip = False
            for f in occurrences:
                if l in occurrences[f]:
                    for index in occurrences[f][l]:
                        if histories[f][index] == [iline.strip("\n") for 
                                                   iline in inputLines[i-2:i]]:
                            skippedLines.append(l)
                            toSkip = True
            if not toSkip:
                newLmFileD.write(l+"\n")                                

    print "Number of skipped lines: " + str(len(skippedLines))
    return outputFile




def _drawRandom(nbToDraw, maxValue, exclusion=None):
    """Draws random numbers from 0 to maxValue.
    
    Args:
        nbToDraw (int): number of numbers to draw
        maxValue (int): max value for the numbers to draw
        exclusion (set): numbers to exclude
        
    """
    numbers = set()     
    while len(numbers) < nbToDraw:
        choice = random.randrange(0, maxValue)
        if not exclusion or choice not in exclusion:
            numbers.add(choice)
    
    return numbers 




def divideXCESCorpus(xcesFile):
    xcesFile = Path(xcesFile)
    print "Parsing file " + xcesFile
    tree = etree.parse(str(xcesFile))
    root = tree.getroot()
    alignments = getAlignments(root, xcesFile.getUp() + "/OpenSubtitles2013/xml/")
    
    train, tune, dev, test = divideAlignedData(alignments)
    print "train:%i, tune:%i, dev:%i, test:%i"%(len(train),len(tune),len(dev), len(test))
     
    srcDevFile, trgDevFile = generateMosesFiles(extractDict(alignments, dev.keys()), 
                                                xcesFile.replace(".xml", ".dev"))
    srcTestFile, trgTestFile = generateMosesFiles(extractDict(alignments, test.keys()), 
                                                xcesFile.replace(".xml", ".test"))                                      
    
    generateMosesRefFiles(alignments, dev.keys(), trgDevFile)
    generateMosesRefFiles(alignments, test.keys(), trgTestFile)    

    inverseAlignments = {}
    invDev, invTest = [], []
    for a in alignments:
        align = alignments[a]
        inverseAlignments[align[0]] = (a, align[2], align[1])
        if a in dev:
            invDev.append(align[0])
        if a in test:
            invTest.append(align[0])                       
    generateMosesRefFiles(inverseAlignments, invDev, srcDevFile)
    generateMosesRefFiles(inverseAlignments,invTest, srcTestFile)
    
    generateMosesFiles(train, xcesFile.replace(".xml", ".train"))
    generateMosesFiles(tune, xcesFile.replace(".xml", ".tune"))
    print "Finished generating Moses files"
                    
    


def getAlignments(xmlRoot, basePath):
    print "Extracting alignments"
    corporaDict = {}
    for linkGrp in xmlRoot:
        if linkGrp.tag == 'linkGrp':
            fromdoc = Path(basePath + linkGrp.attrib['fromDoc'])
            todoc =  Path(basePath + linkGrp.attrib['toDoc'])
            if not fromdoc.exists():
                raise RuntimeError("could not find " + fromdoc)
            if not todoc.exists():
                raise RuntimeError("could not find " + todoc)
            sourceIndices = []
            targetIndices = []
            nb11Aligns = 0
            for link in linkGrp:
                if link.tag == 'link':
                    split = link.attrib["xtargets"].split(";")
                    if len(split) != 2:
                        raise RuntimeError("xtargets %s not separated by ;"
                                           %(link.attrib["xtargets"]))
                    sourceLines = [int(i) for i in split[0].strip().split(" ") if len(i)>0]
                    targetLines = [int(i) for i in split[1].strip().split(" ") if len(i)>0]
                    if len(sourceLines) == 1 and len(targetLines)==1:
                        nb11Aligns += 1
                    sourceIndices.append(sourceLines)
                    targetIndices.append(targetLines)
            if nb11Aligns < (2*len(sourceIndices)/5):
                print "Skipping alignment %s -> %s"%(fromdoc, todoc)
                print "(Percentage of 1:1 alignments: %i %%)"%((100*nb11Aligns)/len(sourceIndices))
                continue
            corporaDict[fromdoc] = (todoc, sourceIndices, targetIndices)

    return corporaDict


def divideAlignedData(fullAligns, nbTuning=2, nbDev=6, nbTesting=6):
    if len(fullAligns) < 20:
        raise RuntimeError("not enough data to divide")
    sources = sorted(fullAligns.keys(), 
                     key=lambda x : len(fullAligns[x][0].getUp().listdir()))
    
    aligns = copy.deepcopy(fullAligns)
    testAligns = {}
    for _ in range(0, nbTesting):
        selection = sources[-1]
        testAligns[selection] = aligns[selection]
        for a in aligns.keys():
            if selection.getUp() in a:
                del aligns[a]
                del sources[sources.index(a)]
    devAligns = {}
    for _ in range(0, nbDev):
        selection = sources[-1]
        devAligns[selection] = aligns[selection]
        for a in aligns.keys():
            if selection.getUp() in a:
                del aligns[a]
                del sources[sources.index(a)]
    
   
    trainAligns = extractDict(aligns, sources[:-nbTuning])
    tuneAligns = extractDict(aligns,sources[-nbTuning:])
    print "Tune keys: " + str(tuneAligns.keys())
    print "tune values: " + str([tuneAligns[al][0] for al in tuneAligns])
    return trainAligns, tuneAligns, devAligns, testAligns



def extractDict(dico, dkeys):
    return reduce(lambda x, y: x.update({y[0]:y[1]}) or x,
                  map(None, dkeys, map(dico.get, dkeys)), {})


def getCorrelatedTargets(fullAligns, testAligns):
                   
    corrTargetsForDoc = {}
    for fromdoc in testAligns:
        xcesfromdoc = str(uuid.uuid4())[0:5]
        srcFile, trgFile = generateMosesFiles({fromdoc:fullAligns[fromdoc]}, xcesfromdoc)
        with open(srcFile) as fromdocSrc:
            fromdocSrcLines = fromdocSrc.readlines()
        corrTargetsForDoc[fromdoc] = []
        for otherSource in fromdoc.getUp().listdir():
            otherSourcePath = fromdoc.getUp()+"/"+otherSource
            if fullAligns.has_key(otherSourcePath):
                corrTargets = []
                xcesotherSource = str(uuid.uuid4())[0:5]
                srcFile2, trgFile2 = generateMosesFiles({otherSourcePath:fullAligns
                                                         [otherSourcePath]}, xcesotherSource)
                with open(srcFile2) as otherSrc:
                    otherSrcLines = otherSrc.readlines()
                with open(trgFile2) as otherTrg:
                    otherTrgLines = otherTrg.readlines()
                              
                for i in range(0, len(fromdocSrcLines)):
                    srcLine = fromdocSrcLines[i]
                    foundTarget = None
                    for k in range(i-5, i+5):
                        otherLine = otherSrcLines[k] if k < len(otherSrcLines) else None
                        if srcLine == otherLine:
                            foundTarget = otherTrgLines[k]
                    corrTargets.append(foundTarget)
                               
                Path(xcesotherSource).remove()
                Path(srcFile2).remove()
                Path(trgFile2).remove() 
                
                if len([target for target in corrTargets if target!=""]) > 2*len(fromdocSrcLines)/3:
                    corrTargetsForDoc[fromdoc].append(corrTargets) 
                    print "Adding reference!"      
   
        Path(xcesfromdoc).remove()
        Path(srcFile).remove()
        Path(trgFile).remove()
        
    return corrTargetsForDoc
 
 
def generateMosesRefFiles(fullAligns, testKeys, referenceStem):
    
    corrTargetsForDoc = getCorrelatedTargets(fullAligns, testKeys)    
    
    alternativesPerLine = []
    for fromdoc in testKeys:
        for i in range(0, len(corrTargetsForDoc[fromdoc][0])):
            alternativesForLine = set()
            for corrTargets in corrTargetsForDoc[fromdoc]:
                corrTarget = corrTargets[i]
                if corrTarget:
                    alternativesForLine.add(corrTarget)
            alternativesPerLine.append(list(alternativesForLine))
                             
    
    nbReferences = max([len(line) for line in alternativesPerLine])
    print "max number of referernces: %i"%(nbReferences)
    for i in range(0, nbReferences):
        with open(referenceStem+str(i), 'w') as refe:
            for line in alternativesPerLine:
                if i < len(line):
                    refe.write(line[i])
                else:
                    refe.write("\n") 
                       
    
def generateMosesFiles(alignments, dataStem):
    xcesFile = Path(dataStem + ".xml")
    writeXCESFile(alignments, xcesFile)
    
    s = re.search(r"(.*)\-(.*?)\.", xcesFile.basename())
    if s:
        sourceLang, targetLang = s.group(1), s.group(2)
    else:
        sourceLang, targetLang = "src", "trg"
    sourceFile = Path(dataStem + "." + sourceLang)
    targetFile = Path(dataStem + "." + targetLang)
    
    script = "./uplug/tools/opus2moses.pl -f %s -e %s %s"%(targetFile, sourceFile, xcesFile)
    ShellExecutor(quiet=False).run(script)
    xcesFile.remove()
    return sourceFile, targetFile
    

header = """\
<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE cesAlign PUBLIC "-//CES//DTD XML cesAlign//EN" "">
<cesAlign version="1.0">
""" 

def writeXCESFile(aligns, xcesFile):
    
    print "Writing to file %s"%(xcesFile)
    with open(xcesFile, 'w') as xces:
        
        xces.write(header)
        for fromdoc in aligns:
            alignment = aligns[fromdoc]
            todoc = alignment[0] 
            linkGrp = """<linkGrp targType="s" fromDoc="%s" toDoc="%s">\n"""%(fromdoc, todoc)
            xces.write(linkGrp)
            for i in range(0, len(alignment[1])):
                sourceLines = [str(j) for j in alignment[1][i]]
                targetLines = [str(j) for j in alignment[2][i]]
                xtargets = " ".join(sourceLines) + ";" + " ".join(targetLines)
                line = """<link id="SL%i" xtargets="%s" />\n"""%(i, xtargets)
                xces.write(line)
            xces.write("</linkGrp>\n")
        
        xces.write("</cesAlign>\n")
        
   
def mergeAlignments(aligns):
    print "merging alignments"
    sizes = extractSizes(aligns.keys())
    samples = {}
    print "document samples extracted"
    
    newAligns = {}
    for fromdoc in aligns:  
        newAligns[fromdoc] = [aligns[fromdoc]]   
        for otherSource in fromdoc.getUp().listdir():
            otherSource = fromdoc.getUp() + "/" + otherSource
            if (otherSource != fromdoc and newAligns.has_key(otherSource)
                and (sizes[fromdoc] - sizes[otherSource]) < 2000
                and extractSamples(samples,fromdoc) == extractSamples(samples, otherSource)
                and len(aligns[fromdoc][1]) == len(aligns[otherSource][1])):
                print "YES! %s and %s"%(fromdoc,otherSource)                          
                newAligns[fromdoc] += newAligns[otherSource]
                del newAligns[otherSource]
    
    print "Nb. files with alternative translations: %i"%(len(aligns)-len(newAligns))
    return newAligns


def extractSamples(samples, fromdoc):
    if samples.has_key(fromdoc):
        return samples[fromdoc]
    
    docunzipped = gzip.open(fromdoc, 'r')
    root = etree.fromstring(docunzipped.read())
    first = getSentenceFromXML(root[0])
    size = len(root)
    oneThird = getSentenceFromXML(root[size/3])
    twoThird = getSentenceFromXML(root[2*size/3])
    last = getSentenceFromXML(root[size-1])
    docunzipped.close()
    result = (first,oneThird,twoThird,last)
    samples[fromdoc] = result
    return result


def getSentenceFromXML(xmlEntity):
    if xmlEntity.tag == 's':
        sentence = []
        for wid in xmlEntity:
            if wid.tag == 'w':
                sentence.append(wid.text if isinstance(wid.text, basestring) else "")
        return " ".join(sentence) 
    
     
def extractSizes(documents):
    sizes = {}
    for d in documents:
        try: 
            docunzipped = gzip.open(d, 'r')
            doctext = docunzipped.read()  
            docunzipped.close()   
            sizes[d] = len(doctext)
        except IOError:
            print "IOError for file " + str(d)
    return sizes        

      

