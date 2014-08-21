# -*- coding: utf-8 -*-


__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"


import random
from mosespy.system import Path


class BasicCorpus(object):
         
    def __init__(self, corpusFile):
        
        self.corpusFile = Path(corpusFile)
        
        if not self.getCorpusFile().exists():
            raise RuntimeError(self.getCorpusFile() + " does not exist")    

        self.originCorpus = None
        self.originIndices = None
        
        indicesFile = (self.corpusFile.getStem() + ".indices")
        if indicesFile.exists():
            indLines = indicesFile.readlines()
            self.originCorpus = BasicCorpus(indLines[0].strip() + "." + self.corpusFile.getLang())
            self.originIndices = [int(i.strip()) for i in indLines[1:]]
      
       
    def getCorpusFile(self):
        return self.corpusFile 
         
    def getOccurrences(self):
        
        occurrences = {} 
        corpusLines = self.getCorpusFile().readlines()
        for i in range(0, len(corpusLines)):
            corpusLine = corpusLines[i].strip()
            if corpusLine not in occurrences:
                occurrences[corpusLine] = set()
            occurrences[corpusLine].add(i)
                
        return occurrences
   
    def getHistories(self, historyWindow=2):
        
        histories = {}
        corpusLines = self.getCorpusFile().readlines()
        if self.originCorpus:
            originLines = self.originCorpus.getCorpusFile().readlines()
        else:
            originLines = corpusLines
            
        originLines = [originLine.strip("\n") for originLine in originLines]
        
        for i in range(0, len(corpusLines)):
            origindex = self.originIndices[i] if self.originIndices else i
            histories[i] = originLines[max(0,origindex-historyWindow):max(0,origindex)]
 
        return histories


        
    def filterOutLines(self, contentToRemove, outputFile):
    
        inputLines = self.getCorpusFile().readlines()
        
        contentToRemove = BasicCorpus(contentToRemove)
        occurrences = contentToRemove.getOccurrences()
        histories = contentToRemove.getHistories()     

        with open(outputFile, 'w', 1000000) as newLmFileD:                 
            skippedLines = []
            for i in range(2, len(inputLines)):
                l = inputLines[i]
                toSkip = False
                if l in occurrences:
                    for index in occurrences[l]:
                        if histories[index] == inputLines[i-2:i]:
                            skippedLines.append(l)
                            toSkip = True
                if not toSkip:
                    newLmFileD.write(l)                                
    
        print "Number of skipped lines: " + str(len(skippedLines))
        return outputFile
    
    
    
    def splitData(self, outputDir, nbSplits):
    
        lines = self.getCorpusFile().readlines()
        extension = "." + Path(self.getCorpusFile()).getLang()
            
        totalLines = len(lines) 
        nbSplits = min(nbSplits, totalLines)
        filenames = []
        curSplit = 0
        filename = Path(outputDir + "/" + str(curSplit) + extension)
        filenames.append(filename)
        curFile = open(filename, 'w')
        nbLines = 0
        for l in lines:
            if nbLines >= (totalLines / nbSplits) and curSplit < nbSplits -1:
                nbLines = 0
                curFile.close()
                curSplit += 1
                filename = Path(outputDir + "/" + str(curSplit) + extension)
                curFile = open(filename, 'w')
                filenames.append(filename)
            curFile.write(l)
            nbLines += 1
        curFile.close()
        return filenames



class AlignedCorpus(object):
    
    def __init__(self, stem, sourceLang, targetLang):
        
        self.stem = Path(stem)
        self.sourceLang = sourceLang
        self.targetLang = targetLang
        self.sourceCorpus = BasicCorpus(self.stem + "." + sourceLang)
        self.targetCorpus = BasicCorpus(self.stem + "." + targetLang)
              
        nbLinesSource = self.getSourceFile().countNbLines()
        nbLinesTarget = self.getTargetFile().countNbLines()
        if nbLinesSource != nbLinesTarget:
            raise RuntimeError("Number of lines for source and target are different")

    
    def splitData(self, outputDir, nbSplits):
    
        sourceCorpus = BasicCorpus(self.getSourceFile())
        targetCorpus = BasicCorpus(self.getTargetFile())
        sourceFiles = sourceCorpus.splitData(outputDir, nbSplits)
        targetFiles = targetCorpus.splitData(outputDir, nbSplits)
        stems = [filename.getStem() for filename in sourceFiles]
        if stems != [filename.getStem() for filename in targetFiles]:
            raise RuntimeError("stems from split data in source and target are different")
        
        return stems
               
 
    def divideData(self, workPath, nbTuning=1000, nbTesting=3000, randomPick=True):

        workPath = Path(workPath)
        sourceLines = self.getSourceFile().readlines()
        targetLines = self.getTargetFile().readlines()
            
        if randomPick:
            tuningIndices =self. _drawRandomUnique(nbTuning)
            testingIndices = self._drawRandomUnique(nbTesting, exclusion=tuningIndices)
        else:
            tuningIndices = range(0,len(sourceLines))[-nbTuning-nbTesting:-nbTesting]
            testingIndices = range(0,len(sourceLines))[-nbTesting:]
        trainSourceLines = []
        tuneSourceLines = []
        testSourceLines = []       
        print "Dividing source data..."
        for i in range(0, len(sourceLines)):
            sourceLine = sourceLines[i]
            if i in tuningIndices:
                tuneSourceLines.append(sourceLine)
            elif i in testingIndices:
                testSourceLines.append(sourceLine)
            else:
                trainSourceLines.append(sourceLine)
        
        trainTargetLines = []
        tuneTargetLines = []
        testTargetLines = []       
        print "Dividing target data..."
        for i in range(0, len(targetLines)):
            targetLine = targetLines[i]
            if i in tuningIndices:
                tuneTargetLines.append(targetLine)
            elif i in testingIndices:
                testTargetLines.append(targetLine)
            else:
                trainTargetLines.append(targetLine)
         
        trainStem = workPath + "/" + (self.stem + ".train").basename()
        (trainStem + "." + self.sourceLang).writelines(trainSourceLines) 
        (trainStem + "." + self.targetLang).writelines(trainTargetLines)
        trainCorpus = AlignedCorpus(trainStem, self.sourceLang, self.targetLang)
        

        tuneStem = workPath + "/" + (self.stem + ".tune").basename()
        (tuneStem + "." + self.sourceLang).writelines(tuneSourceLines) 
        (tuneStem + "." + self.targetLang).writelines(tuneTargetLines)
        (tuneStem + ".indices").writelines([self.stem+"\n"] + [str(i)+"\n" for i in sorted(list(tuningIndices))])
        tuneCorpus = AlignedCorpus(tuneStem, self.sourceLang, self.targetLang)

        testStem = workPath + "/" + (self.stem + ".test").basename()
        (testStem + "." + self.sourceLang).writelines(testSourceLines) 
        (testStem + "." + self.targetLang).writelines(testTargetLines)
        (testStem + ".indices").writelines([self.stem+"\n"] + [str(i)+"\n" for i in sorted(list(testingIndices))])
        testCorpus = AlignedCorpus(testStem, self.sourceLang, self.targetLang)
  
        return trainCorpus, tuneCorpus, testCorpus
        


    def _drawRandomUnique(self, number, exclusion=None):
        sourceRaw = self.getSourceFile().read()
        targetRaw = self.getTargetFile().read()
        sourceLines = sourceRaw.split()
        targetLines = targetRaw.split()
        start = 2
        end = self.getSourceFile().countNbLines() -1
        numbers = set()
        while len(numbers) < number:
            choice = random.randrange(start, end)
            if not exclusion or choice not in exclusion:
                sourceWindow = sourceLines[choice-2] + sourceLines[choice-1] + sourceLines[choice] + sourceLines[choice+1]
                targetWindow = targetLines[choice-2] + targetLines[choice-1] + targetLines[choice] + targetLines[choice+1]
                print "Source count: " + sourceRaw.count(sourceWindow)
                print "Target count: " + targetRaw.count(targetWindow)
                numbers.add(choice)
        return numbers

  
    def getStem(self):
        return self.stem
    
    def getSourceFile(self):
        return self.sourceCorpus.getCorpusFile()
        
    def getTargetFile(self):
        return self.targetCorpus.getCorpusFile()
    
    def getSourceCorpus(self):
        return self.sourceCorpus

    def getTargetCorpus(self):
        return self.targetCorpus
    
    def getAlignments(self, addHistory=False): 
        
        sourceLines = self.getSourceFile().readlines()
        targetLines = self.getTargetFile().readlines()      
            
        alignments = []
        for i in range(0, len(sourceLines)):
            align = {"source": sourceLines[i].strip(), "target": targetLines[i].strip()}
            alignments.append(align)
            
        if addHistory:
            targetCorpus = BasicCorpus(self.getTargetFile())
            histories = targetCorpus.getHistories()
            for i in range(0, len(alignments)):
                align = alignments[i]
                if histories.has_key(i):
                    align["previoustarget"] = histories[i][-1] if len(histories[i]) > 0 else None
                 
        return alignments
            


class TranslatedCorpus(AlignedCorpus):
    
    def __init__(self, corpus, translationFile):
        if not isinstance(corpus, AlignedCorpus):
            raise RuntimeError("corpus must be an AlignedCorpus object")
        AlignedCorpus.__init__(self, corpus.getStem(), corpus.sourceLang, corpus.targetLang)
        
        translationFile = Path(translationFile)
        if not translationFile.exists():
            raise RuntimeError(translationFile + " does not exist")
        if translationFile.getLang() != self.targetLang:
            raise RuntimeError("language for reference and actual translations differ")
        elif translationFile.countNbLines() != self.getTargetFile().countNbLines():
            raise RuntimeError("reference and actual translation do not have the same number of lines")
        
        self.translationFile = translationFile
          
    def getTranslationFile(self):
        return self.translationFile
            
   
    def getAlignments(self, addHistory=False): 
        
        alignments = AlignedCorpus.getAlignments(self, addHistory)
        
        translationLines = self.translationFile.readlines()
        for i in range(0, len(alignments)):
            alignment = alignments[i]
            alignment["translation"] = translationLines[i].strip()
                
        return alignments

 
            
