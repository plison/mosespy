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


"""Module for creating, manipulating and processing various types
of corpus.  A corpus can either be monolingual (BasicCorpus) or 
bilingual (AlignedCorpus and TranslatedCorpus).  The class
CorpusProcessor provides functions for easily preprocessing 
and splitting such corpora.

"""
__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

import re
import mosespy.system as system
import mosespy.install as install
from mosespy.system import Path


class BasicCorpus(Path):
    """A basic, monolingual corpus, composed of a sequence of lines.
    
    """
         
    def __init__(self, corpusFile):
        """Creates a corpus object based on the corpus file. If isNew is set to 
        True, a new empty file is created.
               
       """ 
        Path.__init__(corpusFile)
            
        if not self.exists():
            raise IOError(self + " does not exist")    
  
             
    def printlines(self):
        """Prints the content of the corpus.
         
        """
        if self.exists():
            for l in self.readlines():
                print l
        else:
            raise RuntimeError(self + " not an existing file")
    
         
    def getOccurrences(self):
        """Returns a dictionary where the keys correspond to occurrences
        of particular lines, and the values are sets of line numbers
        from the corpus.
        
        """
        occurrences = {} 
        corpusLines = self.readlines()
        for i in range(0, len(corpusLines)):
            corpusLine = corpusLines[i].strip()
            if corpusLine not in occurrences:
                occurrences[corpusLine] = set()
            occurrences[corpusLine].add(i)
                
        return occurrences
   
    def getHistories(self, historyWindow=2):
        """Returns a dictionary where the keys correspond to the line
        numbers, and the values represent the list of lines that immediately
        precedes this line.
        
        If the corpus is derived from a bigger corpus, this bigger corpus
        is employed to extract these histories.
        
        Args:
            historyWindow (int): the number of lines to include in the
                local history for each line.
        """
        
        histories = {}
        corpusLines = self.readlines()
            
        originLines = [originLine.strip("\n") for originLine in corpusLines]
        
        for i in range(0, len(corpusLines)):
            histories[i] = originLines[max(0,i-historyWindow):max(0,i)]
 
        return histories



class AlignedPair():
    """Representation of a pair of aligned (source,target) sentences, 
    along with some optional information such as the history of preceding 
    sentences.
    
    """
    def __init__(self, source, target):
        """Creates a new pair with a source and target sentence.
        
        """
        self.source = source
        self.target = target
        self.targethistory = None
        
    def addTargetHistory(self, history):
        """Adds a history of previous sentence to the pair.
        
        """
        self.targethistory = history

        

class AlignedCorpus(object):
    """Representation of an aligned corpus.
    
    """
    
    def __init__(self, stem, sourceLang, targetLang):
        """Constructs the aligned corpus from a corpus stem, a source
        language code and a target language code. The two files
        {stem}.{sourceLang} and {stem}.{targetLang} must exist.
        
        """
        self.stem = Path(stem)
        self.sourceLang = sourceLang
        self.targetLang = targetLang
        self.sourceCorpus = BasicCorpus(self.stem + "." + sourceLang)
        self.targetCorpus = BasicCorpus(self.stem + "." + targetLang)
              
        if self.sourceCorpus.countNbLines() != self.targetCorpus.countNbLines():
            raise RuntimeError("Nb. of lines for source and target are different")          
  
    def getStem(self):
        """Returns the stem for the aligned corpus.
        
        """
        return self.stem
   
    
    def getSourceCorpus(self):
        """Returns a BasicCorpus object based on the source data.
        
        """
        return self.sourceCorpus

    def getTargetCorpus(self):
        """Returns a BasicCorpus object based on the target data.
        
        """
        return self.targetCorpus
    
    
    def countNbLines(self):
        """Returns the number of lines in the corpus.
        
        """
        return self.sourceCorpus.countNbLines()
             

    
    def remove(self):
        """Deletes the files containing the corpus.
        
        """
        self.sourceCorpus.remove()
        self.targetCorpus.remove()
        
    
    def getAlignments(self, addHistory=False): 
        """Returns a list of alignment objects (of length corresponding
        to the number of lines in the corpus), where each alignment
        entry encodes the source sentence, the target sentence, and
        (if addHistory is set to true), the history of the target
        sentence.
        
        """
        sourceLines = self.getSourceCorpus().readlines()
        targetLines = self.getTargetCorpus().readlines()      
            
        alignments = []
        for i in range(0, len(sourceLines)):
            pair = AlignedPair(sourceLines[i].strip(),targetLines[i].strip())
            alignments.append(pair)
            
        if addHistory:
            targetCorpus = self.getTargetCorpus()
            histories = targetCorpus.getHistories()
            for i in range(0, len(alignments)):
                pair = alignments[i]
                if histories.has_key(i) and len(histories[i]) > 0:
                    pair.addTargetHistory(histories[i][-1])
                 
        return alignments



class AlignedReference(AlignedPair):
    
    def __init__(self, source, targets):
        if not hasattr(targets, "__iter__"):
            targets = (targets,)
        AlignedPair.__init__(self, source, targets)
        self.translation = None
    
    
    def addTranslation(self, translation):
        self.translation = translation



class ReferenceCorpus(object):
    
    def __init__(self, stem, sourceLang, targetLang):
        self.stem = Path(stem)
        self.sourceLang = sourceLang
        self.sourceCorpus = BasicCorpus(self.stem + "." + sourceLang)
        self.targetLang = targetLang
        
        self.refCorpora = []
        for inDir in stem.getUp().listdir():
            if re.search(r"%s\.%s(\d)+"%(stem,targetLang), inDir):
                refCorpus = BasicCorpus(inDir)
                self.refCorpora.append(refCorpus)                         
                if self.sourceCorpus.countNbLines() != refCorpus.countNbLines():
                    raise RuntimeError("Nb. of lines for source and reference are different")  
        if not self.refCorpora:
            self.refCorpora.append(BasicCorpus(stem+"."+targetLang))
        
        self.translation = None


    def getStem(self):
        """Returns the stem for the aligned corpus.
        
        """
        return self.stem
    
    
    def addTranslation(self, translationFile):
        self.translation = BasicCorpus(translationFile)
        if self.translation.getLang() != self.targetLang:
            raise IOError("language for reference and actual translations differ")
        elif self.translation.countNbLines() != self.countNbLines():
            raise IOError("Nb. of lines in reference and translation are different")
           
    
    def getSourceCorpus(self):
        """Returns a BasicCorpus object based on the source data.
        
        """
        return self.sourceCorpus

    def getReferenceCorpora(self):
        """Returns a list of reference translations.
        
        """
        return self.refCorpora
 
      
    def getTranslationCorpus(self):
        """Returns the corpus of actual translations.
        
        """
        return self.translation
    
       
    
    def countNbLines(self):
        """Returns the number of lines in the corpus.
        
        """
        return self.sourceCorpus.countNbLines()
             

    
    def remove(self):
        """Deletes the files containing the corpus.
        
        """
        self.sourceCorpus.remove()
        for refCorpus in self.refCorpora:
            refCorpus.remove()
        
    
    def getAlignments(self, addHistory=False): 
        """Returns a list of alignment objects (of length corresponding
        to the number of lines in the corpus), where each alignment
        entry encodes the source sentence, the reference sentences,
        the actual translations (if provided), and  (if addHistory is 
        set to true), the history of the target sentence.
        
        """
        sourceLines = self.getSourceCorpus().readlines()
        
        targetLines = []
        for refCorpus in self.refCorpora:
            targetLines.append(refCorpus.readlines())    
            
        alignments = []
        for i in range(0, len(sourceLines)):
            targets = [targetLine[i].strip() for targetLine in targetLines]
            pair = AlignedReference(sourceLines[i].strip(), targets)
            alignments.append(pair)
            
        if addHistory:
            targetCorpus = self.refCorpora[0]
            histories = targetCorpus.getHistories()
            for i in range(0, len(alignments)):
                pair = alignments[i]
                if histories.has_key(i) and len(histories[i]) > 0:
                    pair.addTargetHistory(histories[i][-1])
                    
        if self.translation:       
            translationLines = self.translation.readlines()
            for i in range(0, len(alignments)):
                pair = alignments[i]
                pair.addTranslation(translationLines[i].strip())
                 
        return alignments     



 
class CorpusProcessor():
    """Processor for various types of corpus data.  The processor
    is used to tokenise, detokenise, truecase, clean, and split
    the content of monolingual and bilingual corpora.
    
    """
    
    def __init__(self, workPath, executor=None, nbThreads=2):
        """Creates a new processor.
        
        Args:
            workPath (str): directory in which to store intermediate files
            executor: executor for the processing commands
            nbThreads (int): number of threads to employ
        
        """
        self.workPath = Path(workPath)
        self.executor = executor if executor else system.ShellExecutor()
        self.tokeniser = Tokeniser(executor, nbThreads)
        self.truecaser = TrueCaser(executor, workPath+"/truecasingmodel")
        
        
    def processAlignedCorpus(self, corpus, maxLength=80):
        """Process an aligned corpus. Both the source and target side of the
        corpus are normalised, tokenised, truecased, and cleaned (cut-off 
        beyond a certain length).
        
        Args:
            corpus: an aligned corpus
            maxLength (int): the maximum length for sentence, in number of
                words. If maxLength==False, no maximum length is set.
        
        """
        
        if isinstance(corpus,AlignedCorpus):
            trueSource = self.processCorpus(corpus.getSourceCorpus())
            trueTarget = self.processCorpus(corpus.getTargetCorpus())
     
            trueCorpus = AlignedCorpus(trueSource.getStem(), corpus.sourceLang, corpus.targetLang)
        
        elif isinstance(corpus,ReferenceCorpus):
            trueSource = self.processCorpus(corpus.getSourceCorpus())
            processedRefs = []
            for refCorpus in corpus.getReferenceCorpora():
                processedRefs.append(self.processCorpus(refCorpus))
     
            trueCorpus = ReferenceCorpus(trueSource.getStem(), corpus.sourceLang, corpus.targetLang)
                 
        else:
            raise RuntimeError("aligned data must be of type AlignedCorpus")
   
       
        if maxLength:
            cleanStem = trueSource.getStem().changeFlag("clean")
            cleanCorpus = self.cutCorpus(trueCorpus, cleanStem, maxLength)
            trueSource.remove()
            trueTarget.remove()
            return cleanCorpus
        else:
            return trueCorpus


    def processCorpus(self, rawCorpus, tokenise=False):
        """Process a basic corpus by normalising, tokenising and
        truecasing it.  Intermediary files are deleted, and the final
        truecased file is returned.
         
         """
        if not isinstance(rawCorpus, BasicCorpus):
            rawCorpus = BasicCorpus(rawCorpus)
        
        # STEP 1: tokenisation
        if tokenise:
            normFile = self.workPath + "/" + rawCorpus.basename().addFlag("norm")
            self.tokeniser.normaliseFile(rawCorpus, normFile)
            tokFile = normFile.changeFlag("tok")
            self.tokeniser.tokeniseFile(normFile, tokFile)
            normFile.remove()
        else:
            tokFile = rawCorpus
        
        # STEP 2: train truecaser if not already existing
        if not self.truecaser.isModelTrained(rawCorpus.getLang()):
            self.truecaser.trainModel(tokFile)
            
        # STEP 3: truecasing   
        trueFile = tokFile.changeFlag("true")
        self.truecaser.truecaseFile(tokFile, trueFile) 
        
        if tokenise:
            tokFile.remove()
      
        return BasicCorpus(trueFile)  
    
    
    def processText(self, text, lang):
        """Tokenise and truecase the text, and returns the result.
        
        """
        tokText = self.tokeniser.tokenise(text, lang).strip("\n") + "\n" 
        trueText = self.truecaser.truecase(tokText, lang).strip("\n") + "\n" 
        return trueText
 
 
    def revertReferenceCorpus(self, corpus):
        """Reverts the corpus content by 'detokenising' it and deescaping
        special characters.
        
        """
        if not isinstance(corpus, ReferenceCorpus):
            raise RuntimeError("aligned data must be of type TranslatedCorpus")
        
        revertedSource = self.revertCorpus(corpus.getSourceCorpus())
        for refCorpus in corpus.getReferenceCorpora():
            self.revertCorpus(refCorpus)
        aCorpus = ReferenceCorpus(revertedSource.getStem(), corpus.sourceLang, corpus.targetLang)
        if corpus.translation:
            aCorpus.addTranslation(self.revertCorpus(corpus.getTranslationCorpus()))
        return aCorpus
 
 
    def revertCorpus(self, processedCorpus):
        """Reverts the corpus content by 'detokenising' it and deescaping
        special characters.
        
        """
        if not isinstance(processedCorpus, BasicCorpus):
            processedCorpus = BasicCorpus(processedCorpus)
                        
        untokFile = self.workPath + "/" + processedCorpus.basename().changeFlag("detok") 
        self.tokeniser.detokeniseFile(processedCorpus,untokFile)
         
        finalFile = untokFile.changeFlag("read")
        self.tokeniser.deescapeFile(untokFile, finalFile)
    
        untokFile.remove()
     
        return BasicCorpus(finalFile)
    
    
    def revertText(self, text, lang):
        """Reverts the text by 'detokenising' it, deescaping special characters
        and returning the result.
        
        """
        detokText = self.tokeniser.detokenise(text, lang).strip("\n") + "\n" 
        finalText = self.tokeniser.deescape(detokText).strip("\n")
        return finalText

   
    def cutCorpus(self, inputCorpus, outputStem, maxLength):
        """Cleans the corpus by pruning out sentences with a length
        beyond the maximum length.
        
        """
        cleanScript = (install.moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                       inputCorpus.getStem() + " " + inputCorpus.sourceLang 
                       + " " + inputCorpus.targetLang + " " 
                       + outputStem + " 1 " + str(maxLength))
        result = self.executor.run(cleanScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")
        outputSource = outputStem+"."+inputCorpus.sourceLang
        outputTarget = outputStem+"."+inputCorpus.targetLang
        print ("New cleaned files: " + outputSource.getDescription() 
               + " and " + outputTarget.getDescription())
        
        if isinstance(inputCorpus, AlignedCorpus):
            return AlignedCorpus(outputStem, inputCorpus.sourceLang, inputCorpus.targetLang)
        elif isinstance(inputCorpus, ReferenceCorpus):
            return ReferenceCorpus(outputStem, inputCorpus.sourceLang, inputCorpus.targetLang)
        raise RuntimeError("input corpus is ill-formed")
  
        
    def getBleuScore(self, translatedCorpus):
        """Returns the BLEU score for the translated corpus.
        
        """
        bleuScript = (install.moses_root  + "/scripts/generic/multi-bleu.perl -lc " 
                      + translatedCorpus.getStem()+"."+ translatedCorpus.targetLang)
        translation = translatedCorpus.getTranslationCorpus()
        bleu_output = self.executor.run_output(bleuScript, stdin=translation)       
        s = re.search(r"=\s(([0-9,\.])+)\,", bleu_output)
        if s:
            bleu = float(s.group(1))
            return bleu, bleu_output
        else:
            raise RuntimeError("BLEU score could not be extracted")
          
    
    def splitData(self, corpus, nbSplits, outputDir=None):
        """Splits the corpus into a number of splits.
        
        Args:
            corpus: a basic or aligned corpus to split
            nbSplits: the number of splits to use
            outputDir: the output directory for the splits. Defaults
                to self.workPath.
        
        """
        outputDir = outputDir if outputDir else self.workPath
        if isinstance(corpus, AlignedCorpus):
            
            sourceFiles = self.splitData(corpus.getSourceCorpus(), nbSplits, outputDir)
            targetFiles = self.splitData(corpus.getTargetCorpus(), nbSplits, outputDir)
            stems = [filename.getStem() for filename in sourceFiles]
            if stems != [filename.getStem() for filename in targetFiles]:
                raise RuntimeError("stems from split data in source and target are different")
            return stems
        
        elif isinstance(corpus, BasicCorpus):
              
            lines = corpus.readlines()
            if corpus.getLang():
                extension = "." + corpus.getLang()
            else:
                extension = ""
                
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
        
        else:
            return self.splitData(BasicCorpus(corpus), nbSplits, outputDir)

         
class Tokeniser():
    """Tokeniser component for processing corpora.
    
    """
    def __init__(self, executor, nbThreads=2):
        """Creates a new tokeniser based on the provided executor and number
        of threads.
        
        """
        self.executor = executor
        self.nbThreads = nbThreads
     
    def normaliseFile(self, inputFile, outputFile):
        """Normalises the punctuation of the file and write the output in
        outputFile.
        
        """
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise IOError("raw file " + inputFile + " does not exist")
                        
        cleanScript = (install.moses_root + "/scripts/tokenizer" 
                       +"/normalize-punctuation.perl " + lang)
        result = self.executor.run(cleanScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Normalisation of %s has failed"%(inputFile))

        
       
    def tokeniseFile(self, inputFile, outputFile):
        """Tokenises inputFile and writes the output in outputFile.
        
        
        """
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise IOError("raw file " + inputFile + " does not exist")
                        
        print "Start tokenisation of file \"" + inputFile + "\""
        tokScript = (install.moses_root + "/scripts/tokenizer/tokenizer.perl" 
                     + " -l " + lang + " -threads " + str(self.nbThreads))
        result = self.executor.run(tokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Tokenisation of %s has failed"%(inputFile))

        print "New tokenised file: " + outputFile.getDescription() 
            
        return outputFile
    
    
    def tokenise(self, inputText, lang):
        """Tokenises the text (for the given language) and returns the
        output.
        
        """
        tokScript = (install.moses_root + "/scripts/tokenizer"
                     + "/tokenizer.perl" + " -l " + lang)
        return self.executor.run_output(tokScript, stdin=inputText)
    

    def detokeniseFile(self, inputFile, outputFile):
        """Detokenises inputFile and write the result in outputFile.
        
        """
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise IOError("raw file " + inputFile + " does not exist")
                        
        print "Start detokenisation of file \"" + inputFile + "\""
        detokScript = (install.moses_root + "/scripts/tokenizer"
                       + "/detokenizer.perl -l " + lang)
        result = self.executor.run(detokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Detokenisation of %s has failed"%(inputFile))

        print "New detokenised file: " + outputFile.getDescription() 

            
    def detokenise(self, inputText, lang):
        """Detokenises the text (for the provided language) and returns the output.
        
        """
        tokScript = (install.moses_root + "/scripts/tokenizer" 
                     + "/detokenizer.perl" + " -l " + lang)
        return self.executor.run_output(tokScript, stdin=inputText)
  

    def deescapeFile(self, inputFile, outputFile):
        """Deescapes special characters in the input file and write the result in
        outputFile.
        
        """
        if not inputFile.exists():
            raise IOError("File " + inputFile + " does not exist")
                        
        deescapeScript = (install.moses_root + "/scripts/tokenizer"
                          + "/deescape-special-chars.perl ")
        result = self.executor.run(deescapeScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Deescaping of special characters in %s has failed"%(inputFile))
  
    
    def deescape(self, inputText):   
        """Deescapes special characters in the text and returns the result.
        
        """             
        deescapeScript = (install.moses_root + "/scripts/tokenizer"
                          + "/deescape-special-chars.perl ")
        return self.executor.run_output(deescapeScript, inputText)



class TrueCaser():
    """Truecaser to process corpora content.
    
    """
    
    def __init__(self, executor, modelStem):
        """Creates a new truecaser with the following executor, and stem for model
        files.
        
        """
        self.executor = executor
        self.modelStem = Path(modelStem)
               
               
    def trainModel(self, inputFile):
        """Trains a truecasing model based on the provided input file.
        
        """
        if not inputFile.exists():
            raise IOError("Tokenised file " + inputFile + " does not exist")
        
        modelFile = self.modelStem + "." + inputFile.getLang()
        print "Start building truecasing model based on " + inputFile
        truecaseModelScript = (install.moses_root + "/scripts/recaser/train-truecaser.perl" 
                               + " --model " + modelFile + " --corpus " + inputFile)
        result = self.executor.run(truecaseModelScript)
        if not result:
            raise RuntimeError("Training of truecasing model with %s has failed"%(inputFile))

        print "New truecasing model: " + modelFile.getDescription()
    
    
    def isModelTrained(self, lang):
        """Returns true if a truecasing model has been trained for the provided
        language, and false otherwise.
        
        """
        return Path(self.modelStem + "." + lang).exists()
        
            
    def truecaseFile(self, inputFile, outputFile):
        """Truecase the input file and write the result in outputFile. 
        A truecasing model for the input file language must be present.
        
        """
        if not inputFile.exists():
            raise IOError("tokenised file " + inputFile + " does not exist")
    
        if not self.isModelTrained(inputFile.getLang()):
            raise RuntimeError("Truecasing model for " + inputFile.getLang()+ " is not yet trained")
    
        modelFile = Path(self.modelStem + "." + inputFile.getLang())
        print "Start truecasing of file \"" + inputFile + "\""
        truecaseScript = (install.moses_root + "/scripts/recaser" 
                          + "/truecase.perl" + " --model " + modelFile)
        result = self.executor.run(truecaseScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Truecasing of %s has failed"%(inputFile))

        print "New truecased file: " + outputFile.getDescription()
        return outputFile
    
    
    def truecase(self, inputText, lang):
        """Truecase the text (for the provided language) and returns the result.
        A truecasing model for the input language must be present.
        
        """
        if not self.isModelTrained(lang):
            raise RuntimeError("Truecasing model for " + lang + " is not yet trained")

        modelFile = Path(self.modelStem + "." + lang)
        truecaseScript = (install.moses_root + "/scripts/recaser"
                          + "/truecase.perl" + " --model " + modelFile)
        return self.executor.run_output(truecaseScript, stdin=inputText)
 
 
 
              
