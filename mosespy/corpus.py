
import random
from paths import Path

class AlignedCorpus(object):
    
    def __init__(self, stem, sourceLang, targetLang):
        
        self.stem = Path(stem)
        self.sourceLang = sourceLang
        self.targetLang = targetLang
        self.origin = None
        
        if not self.getSourceFile().exists():
            raise RuntimeError(self.getSourceFile() + " does not exist")
        if not self.getTargetFile().exists():
            raise RuntimeError(self.getTargetFile() + " does not exist")
        
        nbLinesSource = self.getSourceFile().countNbLines()
        nbLinesTarget = self.getTargetFile().countNbLines()
        if nbLinesSource != nbLinesTarget:
            raise RuntimeError("Number of lines for source and target are different")

 
    def divideData(self, workPath, nbTuning=1000, nbTesting=3000):
         
        sourceLines = (self.stem + "." + self.sourceLang).readlines()
        targetLines = (self.stem + "." + self.targetLang).readlines()
            
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
         
        trainStem = workPath + "/" + (self.stem + ".train").basename()
        (trainStem + "." + self.sourceLang).writelines(trainSourceLines) 
        (trainStem + "." + self.targetLang).writelines(trainTargetLines)
        trainCorpus = AlignedCorpus(trainStem, self.sourceLang, self.targetLang)

        tuneStem = workPath + "/" + (self.stem + ".tune").basename()
        (tuneStem + "." + self.sourceLang).writelines(tuneSourceLines) 
        (tuneStem + "." + self.targetLang).writelines(tuneTargetLines)
        tuneCorpus = AlignedCorpus(tuneStem, self.sourceLang, self.targetLang)

        testStem = workPath + "/" + (self.stem + ".test").basename()
        (testStem + "." + self.sourceLang).writelines(testSourceLines) 
        (testStem + "." + self.targetLang).writelines(testTargetLines)
        testCorpus = AlignedCorpus(testStem, self.sourceLang, self.targetLang)
        testCorpus.linkWithOriginalCorpus(self, testingIndices)
        
        return trainCorpus, tuneCorpus, testCorpus
        
    
    def linkWithOriginalCorpus(self, fullCorpus, lineIndices=None):
        if not isinstance(fullCorpus, AlignedCorpus):
            raise RuntimeError(fullCorpus + " must be an aligned corpus")
                
        elif not lineIndices:
            print "Linking test sentences to original corpus..."
            
            linesdict = {}
            sourceLines = self.getSourceFile().readlines()
            targetLines = self.getTargetFile().readlines()
            linesdict = dict.fromkeys(sourceLines, {})        
            for i in range(0, len(sourceLines)):
                sourceLine = sourceLines[i]
                targetLine = targetLines[i]
                print sourceLine + " --> " + targetLine
                linesdict[sourceLine][targetLine] = i
                
            print "number of keys in linesdict: " + str(len(linesdict))
            maxl = -100
            maxk = None
            for k in linesdict:
                if maxl < len(linesdict[k].keys()):
                    maxl = len(linesdict[k].keys())
                    maxk = k
    
            print "maximum numer of targets: " + str(maxl)
            print "the key is : " + str(k) + " and the values are " + str(linesdict[k])
            
            print "finished constructing dico"
            fullSourceLines = fullCorpus.getSourceFile().readlines()
            fullTargetLines = fullCorpus.getTargetFile().readlines()
            
            linesIndices = [None for i in range(0, len(sourceLines))]
            print "finished reading lines"
            for i in range(0, len(fullSourceLines)):
                fullSourceLine = fullSourceLines[i]
                fullTargetLine = fullTargetLines[i]
                if fullSourceLine in linesdict:
                    targetdict = linesdict[fullSourceLine]
                    if fullTargetLine in targetdict:
                        linesIndices[targetdict[fullTargetLine]] = i
            
            print linesIndices
                    
        self.origin = {"corpus":fullCorpus, "indices":lineIndices}
                                
        
    def getStem(self):
        return self.stem
    
    def getSourceFile(self):
        return Path(self.stem + "." + self.sourceLang)
        
    def getTargetFile(self):
        return Path(self.stem + "." + self.targetLang)
            
    
    def filterLmData(self, lmFile, newLmFile):
        
        print "Filtering language model to remove sentences from test set..."
        
        lmFile = Path(lmFile)
        if not lmFile.exists():
            raise RuntimeError(lmFile + " does not exist")
        
        lmLines = lmFile.readlines()

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
                    
        if addHistory and self.origin:
            origTargetLines = self.origin["corpus"].getTargetFile().readlines()
            for i in range(0, len(alignments)):
                align = alignments[i]
                testingIndex = self.origin["indices"][i]
                if testingIndex:
                    align["previoustarget"] = origTargetLines[testingIndex-1].strip()
        
        elif addHistory:
            for i in range(0, len(alignments)):
                align = alignments[i]
                align["previoustarget"] = targetLines[i-1].strip()
                
        return alignments
            


class TranslatedCorpus(AlignedCorpus):
    
    def __init__(self, stem, sourceLang, targetLang, translationFile):
        AlignedCorpus.__init__(self, stem, sourceLang, targetLang)
        
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
        
        translationLines = self.translationFile.readlines()
        alignments = AlignedCorpus.getAlignments(self, addHistory)
        for i in range(0, len(alignments)):
            alignment = alignments[i]
            alignment["translation"] = translationLines[i].strip()
                
        return alignments
            
    

def splitData(inputFile, outputDir, nbSplits):

    if inputFile.exists():  
        extension = "." + Path(inputFile).getLang()
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

