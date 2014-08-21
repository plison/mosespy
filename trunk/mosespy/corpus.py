# -*- coding: utf-8 -*-


__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"


import re, threading
import random
from mosespy.system import Path

rootDir = Path(__file__).getUp().getUp()
moses_root = rootDir + "/moses" 

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
         
        if randomPick:
            window = 4
            duplicates = self.getDuplicateSources(window=window)
            tuningIndices =self. _drawRandom(nbTuning, exclusion=duplicates, window=window)
            testingIndices = self._drawRandom(nbTesting, exclusion=duplicates + tuningIndices, window=window)
        else:
            nbLines = self.getSourceFile().countNbLines()
            tuningIndices = range(0,nbLines)[-nbTuning-nbTesting:-nbTesting]
            testingIndices = range(0,nbLines)[-nbTesting:]

        sourceLines = self.getSourceFile().readlines()
        targetLines = self.getTargetFile().readlines()
    
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
        


    def _drawRandom(self, number, exclusion=None, window=4):

        numbers = set()     
        while len(numbers) < number:
            choice = random.randrange(0, self.getSourceFile().countNbLines() -window)
            if not exclusion or choice not in exclusion:
                numbers.add(choice)

        return numbers
     
     
                    
    
    def getDuplicateSources(self, window=4, nbThreads=16):
 
        print "making pairs..."
        sourceLines = self.getSourceFile().readlines()
        nbLines = len(sourceLines)
        indices = range(0, nbLines)
        print "start sorting..."
        indices.sort(key=lambda x : sourceLines[x])
        print "finished sorting..."
       
        duplicates = set()
        chunck = len(indices)/nbThreads
        allThreads = []
        for t in range(0, nbThreads):
            subindices = indices[t*chunck:t*chunck + chunck]
            tr = threading.Thread(target=_getDuplicateSources, args=(subindices, sourceLines, duplicates, window))
            tr.start()
            allThreads.append(tr)
        
        for t in allThreads:
            t.join()
  
        percent = len(duplicates)*100.0 / self.getSourceFile().countNbLines()
        print "Percentage of duplicates: "+ str(percent)
        print "Duplicates: " + str(duplicates)
        for d in duplicates:
            print str(sourceLines[d:d+window])
        return duplicates
     
  
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
                if histories.has_key(i) and len(histories[i]) > 0:
                    align["previoustarget"] = histories[i][-1]
                 
        return alignments


def _getDuplicateSources(indices, sourceLines, duplicates, window=4):
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

 


class CorpusProcessor():
    
    def __init__(self, workPath, executor, nbThreads=2):
        self.workPath = Path(workPath)
        self.executor = executor
        self.tokeniser = Tokeniser(executor, nbThreads)
        self.truecaser = TrueCaser(executor, workPath+"/truecasingmodel")
        
    def processCorpus(self, corpus, maxLength=80):
        
        if not isinstance(corpus, AlignedCorpus):
            raise RuntimeError("aligned data must be of type AlignedCorpus")
   
        trueSource = self.processFile(corpus.getSourceFile())
        trueTarget = self.processFile(corpus.getTargetFile())
     
        trueCorpus = AlignedCorpus(trueSource.getStem(), corpus.sourceLang, corpus.targetLang)
        
        if maxLength:
            cleanStem = trueSource.getStem().changeProperty("clean")
            cleanCorpus = self.cutCorpus(trueCorpus, cleanStem, maxLength)
            trueSource.remove()
            trueTarget.remove()
            return cleanCorpus
        else:
            return trueCorpus
    

    def processFile(self, rawFile):
         
        rawFile = Path(rawFile)
        
        # STEP 1: tokenisation
        normFile = self.workPath + "/" + rawFile.basename().addProperty("norm")
        self.tokeniser.normaliseFile(rawFile, normFile)
        tokFile = normFile.changeProperty("tok")
        self.tokeniser.tokeniseFile(normFile, tokFile)
        
        # STEP 2: train truecaser if not already existing
        if not self.truecaser.isModelTrained(rawFile.getLang()):
            self.truecaser.trainModel(tokFile)
            
        # STEP 3: truecasing   
        trueFile = tokFile.changeProperty("true")
        self.truecaser.truecaseFile(tokFile, trueFile) 
        
        normFile.remove()
        tokFile.remove()
        
        if (rawFile.getStem() + ".indices").exists():
            (rawFile.getStem() + ".indices").copy((trueFile.getStem() + ".indices"))
        return trueFile  
    
    
    def processText(self, text, lang):
        tokText = self.tokeniser.tokenise(text, lang)                 
        trueText = self.truecaser.truecase(tokText, lang)        
        return trueText
 
 
    def revertCorpus(self, corpus):
        if not isinstance(corpus, TranslatedCorpus):
            raise RuntimeError("aligned data must be of type TranslatedCorpus")
        
        revertedSource = self.revertFile(corpus.getSourceFile())
        self.revertFile(corpus.getTargetFile())
        translation = self.revertFile(corpus.getTranslationFile())
        aCorpus = AlignedCorpus(revertedSource.getStem(), corpus.sourceLang, corpus.targetLang)
        newCorpus = TranslatedCorpus(aCorpus, translation)

        return newCorpus
 
 
    def revertFile(self, processedFile):
        
        processedFile = Path(processedFile)
        if not processedFile.exists():
            raise RuntimeError(processedFile + " does not exist")
        
        untokFile = self.workPath + "/" + processedFile.basename().changeProperty("detok") 
        self.tokeniser.detokeniseFile(processedFile,untokFile)
         
        finalFile = untokFile.changeProperty("read")
        self.tokeniser.deescapeSpecialCharacters(untokFile, finalFile)
    
        untokFile.remove()
        if (processedFile.getStem() + ".indices").exists():
            (processedFile.getStem() + ".indices").copy(finalFile.getStem() + ".indices")
        return finalFile

   
    def cutCorpus(self, inputCorpus, outputStem, maxLength):
                   
        cleanScript = (moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                       inputCorpus.getStem() + " " + inputCorpus.sourceLang + " " + inputCorpus.targetLang + " " 
                       + outputStem + " 1 " + str(maxLength))
        result = self.executor.run(cleanScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")
        outputSource = outputStem+"."+inputCorpus.sourceLang
        outputTarget = outputStem+"."+inputCorpus.targetLang
        print "New cleaned files: " + outputSource.getDescription() + " and " + outputTarget.getDescription()
        return AlignedCorpus(outputStem, inputCorpus.sourceLang, inputCorpus.targetLang)
  
        
    def getBleuScore(self, translatedCorpus):
        bleuScript = (moses_root  + "/scripts/generic/multi-bleu.perl -lc " 
                      + translatedCorpus.getTargetFile())
        bleu_output = self.executor.run_output(bleuScript, stdin=translatedCorpus.getTranslationFile())       
        s = re.search(r"=\s(([0-9,\.])+)\,", bleu_output)
        if s:
            bleu = float(s.group(1))
            return bleu, bleu_output
        else:
            raise RuntimeError("BLEU score could not be extracted")

         
class Tokeniser():
    
    def __init__(self, executor, nbThreads=2):
        self.executor = executor
        self.nbThreads = nbThreads
     
    def normaliseFile(self, inputFile, outputFile):
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        cleanScript = moses_root + "/scripts/tokenizer/normalize-punctuation.perl " + lang
        tmpFile = outputFile + "_tmp"
        result = self.executor.run(cleanScript, inputFile, tmpFile)
        if not result:
            raise RuntimeError("Normalisation of %s has failed"%(inputFile))
        
        outlines = []
        for line in Path(tmpFile).readlines():
            outlines.append(line[0].upper() + line[1:])
        outputFile.writelines(outlines)
        tmpFile.remove()
        
   
    
    def deescapeSpecialCharacters(self, inputFile, outputFile):
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        deescapeScript = moses_root + "/scripts/tokenizer/deescape-special-chars.perl "
        result = self.executor.run(deescapeScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Deescaping of special characters in %s has failed"%(inputFile))

      
    def detokeniseFile(self, inputFile, outputFile):
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        print "Start detokenisation of file \"" + inputFile + "\""
        detokScript = moses_root + "/scripts/tokenizer/detokenizer.perl -l " + lang
        result = self.executor.run(detokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Detokenisation of %s has failed"%(inputFile))

        print "New detokenised file: " + outputFile.getDescription() 

      
    def tokeniseFile(self, inputFile, outputFile):
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        print "Start tokenisation of file \"" + inputFile + "\""
        tokScript = (moses_root + "/scripts/tokenizer/tokenizer.perl" 
                     + " -l " + lang + " -threads " + str(self.nbThreads))
        result = self.executor.run(tokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Tokenisation of %s has failed"%(inputFile))

        print "New tokenised file: " + outputFile.getDescription() 
            
        return outputFile
    
    
    def tokenise(self, inputText, lang):
        tokScript = moses_root + "/scripts/tokenizer/tokenizer.perl" + " -l " + lang
        return self.executor.run_output(tokScript, stdin=inputText).strip()
                


class TrueCaser():
          
    def __init__(self, executor, modelStem):
        self.executor = executor
        self.modelStem = Path(modelStem)
               
    def trainModel(self, inputFile):
        if not inputFile.exists():
            raise RuntimeError("Tokenised file " + inputFile + " does not exist")
        
        modelFile = self.modelStem + "." + inputFile.getLang()
        print "Start building truecasing model based on " + inputFile
        truecaseModelScript = (moses_root + "/scripts/recaser/train-truecaser.perl" 
                               + " --model " + modelFile + " --corpus " + inputFile)
        result = self.executor.run(truecaseModelScript)
        if not result:
            raise RuntimeError("Training of truecasing model with %s has failed"%(inputFile))

        print "New truecasing model: " + modelFile.getDescription()
    
    
    def isModelTrained(self, lang):
        return Path(self.modelStem + "." + lang).exists()
        
            
    def truecaseFile(self, inputFile, outputFile):
       
        if not inputFile.exists():
            raise RuntimeError("tokenised file " + inputFile + " does not exist")
    
        if not self.isModelTrained(inputFile.getLang()):
            raise RuntimeError("model file for " + inputFile.getLang() + " does not exist")
    
        modelFile = Path(self.modelStem + "." + inputFile.getLang())
        print "Start truecasing of file \"" + inputFile + "\""
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        result = self.executor.run(truecaseScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Truecasing of %s has failed"%(inputFile))

        print "New truecased file: " + outputFile.getDescription()
        return outputFile
    
    
    def truecase(self, inputText, lang):
        modelFile = Path(self.modelStem + "." + lang)
        if not modelFile.exists():
            raise RuntimeError("model file " + modelFile + " does not exist")
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        return self.executor.run_output(truecaseScript, stdin=inputText)
    
