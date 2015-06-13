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

import random
from mosespy.corpus import AlignedCorpus, BasicCorpus
    

def divideData(alignedStem, sourceLang, targetLang, nbTuning=1000, nbDev=3000, 
               nbTesting=3000, randomPick=True):
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

