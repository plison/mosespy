# -*- coding: utf-8 -*-

import re
from mosespy.system import Path 
from mosespy.corpus import AlignedCorpus, TranslatedCorpus
rootDir = Path(__file__).getUp().getUp()
moses_root = rootDir + "/moses" 

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"


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
        if not processedFile.exists():
            raise RuntimeError(processedFile + " does not exist")
        
        untokFile = self.workPath + "/" + processedFile.basename().addProperty("detok") 
        self.tokeniser.detokeniseFile(processedFile,untokFile)
         
        finalFile = untokFile.changeProperty("read")
        self.tokeniser.deescapeSpecialCharacters(untokFile, finalFile)
    
        untokFile.remove()
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
    

