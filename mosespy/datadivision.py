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

"""
__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date:: 2014-08-25 08:30:46 #$"

import sys, math, random
import mosespy.slurm as slurm
from mosespy.corpus import AlignedCorpus, BasicCorpus
from mosespy.system import Path
import xml.etree.cElementTree as etree


def findAlignedCorpora(xcesFile):
    print "Parsing file " + str(xcesFile)
    tree = etree.parse(xcesFile)
    root = tree.getroot()
    corporaDict = {}
    for child in root:
        if child.tag == 'linkGrp':
            fromdoc = Path(child.attrib['fromDoc'])
            todoc = child.attrib['toDoc']
            if not corporaDict.has_key(fromdoc):
                corporaDict[fromdoc] = []
            corporaDict[fromdoc].append(todoc)
            for otherSource in fromdoc.getUp().listdir():
                if (otherSource != fromdoc and "1of1" in fromdoc and "1of1" in otherSource and 
                    math.fabs(fromdoc.getSize() - otherSource.getSize()) < 100 
                    and corporaDict.has_key(otherSource)):
                    print "YES! %s and %s"%(fromdoc,otherSource)
                    corporaDict[fromdoc] += corporaDict[otherSource]                 
                    
    totalLinks = 0.0
    for k in corporaDict:
        totalLinks += len(corporaDict[k])
    print "Average number of alignments for each source: %f"%(totalLinks/len(corporaDict))



def divideData(alignedStem, sourceLang, targetLang, nbTuning=1000, nbDev=3000, 
               nbTesting=3000, randomPick=True, duplicatesWindow=4):
    """Divides the aligned data into distinct parts. Since datasets (such 
    as subtitles corpora) often contain duplicates sentences, the method 
    seeks to avoid selecting sentences that can also be found in other 
    parts of the dataset.
    
    In addition, indices files are also generated to allow for 'backtracking'
    the selected sentences in the original corpus.
    
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
        toExclude = extractDuplicates(corpus.getSourceCorpus(), duplicatesWindow)
        
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
    (tuneStem + ".indices").writelines([corpus.stem+"\n"] + 
                                       [str(i)+"\n" for i in sorted(list(tuningIndices))])
    tuneCorpus = AlignedCorpus(tuneStem, corpus.sourceLang, corpus.targetLang)

    devStem = outputPath + "/" + (corpus.stem + ".dev").basename()
    (devStem + "." + corpus.sourceLang).writelines(devSourceLines) 
    (devStem + "." + corpus.targetLang).writelines(devTargetLines)
    (devStem + ".indices").writelines([corpus.stem+"\n"] + 
                                       [str(i)+"\n" for i in sorted(list(developIndices))])
    devCorpus = AlignedCorpus(devStem, corpus.sourceLang, corpus.targetLang)

    testStem = outputPath + "/" + (corpus.stem + ".test").basename()
    (testStem + "." + corpus.sourceLang).writelines(testSourceLines) 
    (testStem + "." + corpus.targetLang).writelines(testTargetLines)
    (testStem + ".indices").writelines([corpus.stem+"\n"] + 
                                       [str(i)+"\n" for i in sorted(list(testingIndices))])
    testCorpus = AlignedCorpus(testStem, corpus.sourceLang, corpus.targetLang)

    return trainCorpus, tuneCorpus, devCorpus, testCorpus
    
    
def extractDuplicates(corpusFile, window=4, nbSplits=1):
    """Extract the set of line numbers in the corpus that contain duplicate 
    sentences (i.e. that contain the same source and target sentences for
    both the current line and its local history).
    
    """
    corpus = BasicCorpus(corpusFile)
    outputPath = corpusFile.getUp()
    
    print "Start search for duplicates (%i splits)"%(nbSplits)
    sourceLines = corpus.readlines()
    nbLines = len(sourceLines)
    indices = range(0, nbLines)
    indices.sort(key=lambda x : sourceLines[x])
    
    step = len(indices)/nbSplits    
    indicesFiles = [Path(outputPath + "/ind"+str(i)) for i in range(0, nbSplits)]
    for i in range(0, nbSplits):
        indicesFiles[i].write(" ".join([str(j) for j in indices[i*step:i*step + step]]))
    
    args = [(corpus,indicesFile, window) for indicesFile in indicesFiles]
    
    outputs = slurm.SlurmExecutor().run_parallel_function(_printDuplicates, 
                                                          args, stdouts=True)
    duplicates = set()
    for output in outputs:
        duplicates = duplicates.union([int(d) for d in output.split()])
    print ("Duplicates found: " + str(len(duplicates)) 
           + " (" + str(len(duplicates)*100.0/nbLines) + " % of total)") 

    return duplicates


def filterOutLines(fullCorpusFile, toRemoveFile):
    """Filters out sentences from the corpus represented by toRemoveFile
    from the corpus in fullCorpusFile.  This method is used to prune 
    language model data from development and test sentences.
    
    """
    fullCorpus = BasicCorpus(fullCorpusFile)
    toRemoveCorpus = BasicCorpus(toRemoveFile)
    
    inputLines = fullCorpus.readlines()
    
    occurrences = toRemoveCorpus.getOccurrences()
    histories = toRemoveCorpus.getHistories()  

    outputFile = fullCorpus.addFlag("filtered") 
    with open(outputFile, 'w', 1000000) as newLmFileD:                 
        skippedLines = []
        for i in range(2, len(inputLines)):
            l = inputLines[i].strip()
            toSkip = False
            if l in occurrences:
                for index in occurrences[l]:
                    if histories[index] == [iline.strip("\n") for iline in inputLines[i-2:i]]:
                        skippedLines.append(l)
                        toSkip = True
            if not toSkip:
                newLmFileD.write(l+"\n")                                

    print "Number of skipped lines: " + str(len(skippedLines))
    return outputFile



def  _printDuplicates(sourceFile, indicesFile, window):
    """Process the indices in indicesFile in the source file, and 
    prints the duplicates in the standard output.  
    
    """
    sys.stderr.write("Starting local extraction of source duplicates...\n") 
    indicesFile = Path(indicesFile)
    indices = [int(val) for val in indicesFile.read().split()] 
    sourceLines = Path(sourceFile).readlines()
    duplicates = set() 
    for i in range(0, len(indices)):
        curIndex = indices[i]
        curWindow = sourceLines[curIndex:curIndex+window]
        for j in range(i+1, len(indices)-window):
            nextIndex = indices[j]
            nextWindow = sourceLines[nextIndex:nextIndex+window]
            if curWindow[0] != nextWindow[0]:
                break
            elif curWindow == nextWindow:
                duplicates.add(curIndex)
                duplicates.add(nextIndex)
                break
        if len(indices) > 100 and not (i % (len(indices)/100)):
            sys.stderr.write("Extraction of duplicates: " + 
                             str(math.ceil(i*10000/len(indices)) / 100) + " %\n")
    
    indicesFile.remove()
    print " ".join([str(d) for d in duplicates])

   

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

