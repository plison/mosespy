
import os, pathutils, random
from pathutils import Path


class AlignedCorpus():
    
    def __init__(self, alignedStem, sourceLang, targetLang):
        
        self.alignedStem = alignedStem
        self.sourceLang = sourceLang
        self.targetLang = targetLang
        
        if not self.getSourceFile().exists():
            raise RuntimeError(self.getSourceFile() + " does not exist")
        if not self.getTargetFile().exists():
            raise RuntimeError(self.getTargetFile() + " does not exist")
        
        nbLinesSource = self.getSourceFile().countNbLines()
        nbLinesTarget = self.getTargetFile().countNbLines()
        if nbLinesSource != nbLinesTarget:
            raise RuntimeError("Number of lines for source and target are different")

 
    def divideData(self, workPath, nbTuning=1000, nbTesting=3000):
         
        sourceLines = Path(self.alignedStem + "." + self.sourceLang).readlines()
        targetLines = Path(self.alignedStem + "." + self.sourceLang).readlines()
            
        tuningIndices = _drawRandom(2, len(sourceLines), nbTuning)
        testingIndices = _drawRandom(2, len(sourceLines), nbTesting, exclusion=tuningIndices)
        
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
         
        trainSource = self.getSourceFile().changePath(workPath).setInfix("train")
        trainSource.writelines(trainSourceLines) 
        trainTarget = self.getTargetFile.changePath(workPath).setInfix("train")
        trainTarget.writelines(trainTargetLines)
        trainCorpus = AlignedCorpus(self.alignedStem+".train", self.sourceLang, self.targetLang)

        tuneSource = self.getSourceFile.changePath(workPath).setInfix("tune")
        tuneSource.writelines(tuneSourceLines)
        tuneTarget = self.getTargetFile.changePath(workPath).setInfix("tune")
        tuneTarget.writelines(tuneTargetLines)
        tuneCorpus = AlignedCorpus(self.alignedStem+".tune", self.sourceLang, self.targetLang)

        testSource = self.getSourceFile.changePath(workPath).setInfix("test")
        testSource.writelines(testSourceLines)
        testTarget = self.getTargetFile.changePath(workPath).setInfix("test")
        testTarget.writelines(testTargetLines)
        testCorpus = AlignedCorpus(self.alignedStem+".test", self.sourceLang, self.targetLang)
        testCorpus.linkWithOriginalCorpus(self, testingIndices)
        
        return trainCorpus, tuneCorpus, testCorpus
        
    
    def linkWithOriginalCorpus(self, fullCorpus, lineIndices):
        self.origin = {"corpus":fullCorpus, "indices":lineIndices}
        
    def getAlignedStem(self):
        return self.alignedStem
    
    def getSourceFile(self):
        return Path(self.alignedStem + "." + self.sourceLang)
        
    def getTargetFile(self):
        return Path(self.alignedStem + "." + self.targetLang)
    
    
    def addActualTranslations(self, translationFile):
        
        if not translationFile.exists():
            raise RuntimeError(translationFile + " does not exist")
        if translationFile.getSuffix() != self.targetLang:
            raise RuntimeError("language for reference and actual translations differ")
        elif translationFile.countNbLines() != self.getTargetFile().countNbLines():
            raise RuntimeError("reference and actual translation do not have the same number of lines")
        
        self.translationFile = translationFile
        
    
    def filterLmData(self, lmFile, newLmFile):
        
        print "Filtering language model to remove sentences from test set..."
             
        if self.origin:
            targetLines = self.origin["corpus"].getTargetFile().readlines()
            
            testoccurrences = {}
            for i in range(0, len(targetLines)):
                l = targetLines[i]
                if i in self.origin["indices"]:
                    history = [targetLines[i-2], targetLines[i-1]]
                    if l not in testoccurrences:
                        testoccurrences[l] = [history]
                    else:
                        testoccurrences[l].append(history)
        else:
            testoccurrences = set().union(self.getTargetFile().readlines())
    
        lmLines = lmFile.readlines()
        
        with open(newLmFile, 'w', 1000000) as newLmFileD:                 
            prev2Line = None
            prevLine = None
            skippedLines = []
            for l in lmLines:
                toSkip = False
                if l in testoccurrences and isinstance(testoccurrences, dict):
                    for occurrence in testoccurrences[l]:
                        if prev2Line == occurrence[0] and prevLine == occurrence[1]:
                            skippedLines.append(l)
                            toSkip = True
                elif l in testoccurrences and isinstance(testoccurrences, set):
                    toSkip = True
                if not toSkip:
                    newLmFileD.write(l)                                
                prev2Line = prevLine
                prevLine = l
        
        print "Number of skipped lines in language model: " + str(len(skippedLines))
    
    
    def getAlignments(self, addHistory=False): 
        
        sourceLines = self.getSourceFile().readlines()
        targetLines = self.getTargetFile().readlines()      
            
        alignments = []
        for i in range(0, len(sourceLines)):
            align = {"source": sourceLines[i].strip(), "target": targetLines[i].strip()}
            alignments.append(align)
            
        if self.translationFile:
            translationLines = self.translationFile.readlines()
            for i in range(0, len(alignments)):
                align = alignments[i]
                align["translation"] = translationLines[i].strip()
        
        if addHistory and self.origin:
            origTargetLines = self.origin["corpus"].getTargetFile().readlines()
            for i in range(0, len(alignments)):
                testingIndex = self.origin["indices"][i]
                align["previoustarget"] = origTargetLines[testingIndex-1].strip()
        
        elif addHistory:
            for i in range(0, len(alignments)):
                align["previoustarget"] = targetLines[i-1].strip()
                
        return alignments
            
    

def splitData(inputFile, outputDir, nbSplits):

    if inputFile.exists():  
        extension = "." + Path(inputFile).getSuffix()
        lines = inputFile.readlines()
    else:
        raise RuntimeError("cannot split the content for data " + inputFile)
        
    totalLines = len(lines) 
    nbSplits = min(nbSplits, totalLines)
    print "Splitting " + str(totalLines)  + " with " + str(nbSplits)
    filenames = []
    curSplit = 0
    filename = outputDir + "/" + str(curSplit) + extension
    filenames.append(filename)
    curFile = open(filename, 'w')
    nbLines = 0
    for l in lines:
        curFile.write(l)
        nbLines += 1
        if nbLines >= (totalLines / nbSplits + 1):
            nbLines = 0
            curFile.close()
            curSplit += 1
            filename = outputDir + "/" + str(curSplit) + extension
            curFile = open(filename, 'w')
            filenames.append(filename)
    curFile.close()
    return filenames




def _drawRandom(start, end, number, exclusion=None):
    numbers = set()
    while len(numbers) < number:
        choice = random.randrange(start, end)
        if not exclusion or choice not in exclusion:
            numbers.add(choice)
    return numbers



 