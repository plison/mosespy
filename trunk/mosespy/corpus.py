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
        
        If an associated 'indices' file can also be found in the same directory 
        as the corpus file, the corpus is assumed to be derived from a bigger
        corpus (see method divideData in the datadivision module).
               
       """ 
        Path.__init__(self, corpusFile)
            
        if not self.exists():
            raise IOError(self + " does not exist")    

        self.originCorpus = None
        self.originIndices = None
        
        indicesFile = (self.getStem() + ".indices")
        if indicesFile.exists():
            indLines = indicesFile.readlines()
            self.originCorpus = BasicCorpus(indLines[0].strip() + "." + self.getLang())
            self.originIndices = [int(i.strip()) for i in indLines[1:]]
  
             
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
        if self.originCorpus:
            originLines = self.originCorpus.readlines()
        else:
            originLines = corpusLines
            
        originLines = [originLine.strip("\n") for originLine in originLines]
        
        for i in range(0, len(corpusLines)):
            origindex = self.originIndices[i] if self.originIndices else i
            histories[i] = originLines[max(0,origindex-historyWindow):max(0,origindex)]
 
        return histories


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
        """Returns a list of dictionaries (of length corresponding
        to the number of lines in the corpus), where each dictionary
        entry encodes the source sentence, the target sentence, and
        (if addHistory is set to true), the history of the target
        sentence.
        
        """
        sourceLines = self.getSourceCorpus().readlines()
        targetLines = self.getTargetCorpus().readlines()      
            
        alignments = []
        for i in range(0, len(sourceLines)):
            align = {"source": sourceLines[i].strip(), 
                     "target": targetLines[i].strip()}
            alignments.append(align)
            
        if addHistory:
            targetCorpus = BasicCorpus(self.getTargetCorpus())
            histories = targetCorpus.getHistories()
            for i in range(0, len(alignments)):
                align = alignments[i]
                if histories.has_key(i) and len(histories[i]) > 0:
                    align["previoustarget"] = histories[i][-1]
                 
        return alignments



class TranslatedCorpus(AlignedCorpus):
    """Representation of an aligned corpus that also includes
    actual translations in addition to the source and target references.
    
    """
    
    def __init__(self, corpus, translationFile):
        """Creates a new translated corpus based on an aligned corpus
        and a file of actual translations.
        
        """
        if not isinstance(corpus, AlignedCorpus):
            raise RuntimeError("corpus must be an AlignedCorpus object")
        AlignedCorpus.__init__(self, corpus.getStem(), corpus.sourceLang, corpus.targetLang)
        
        self.translationCorpus = BasicCorpus(translationFile)

        if self.translationCorpus.getLang() != self.targetLang:
            raise IOError("language for reference and actual translations differ")
        elif self.translationCorpus.countNbLines() != self.countNbLines():
            raise IOError("Nb. of lines in reference and translation are different")
        
          
          
    def getTranslationCorpus(self):
        """Returns the corpus of actual translations.
        
        """
        return self.translationCorpus
    
    
    
    def remove(self):
        """Deletes the files containing the corpus.
        
        """
        AlignedCorpus.remove(self)
        self.translationCorpus.remove()
            
   
    def getAlignments(self, addHistory=False): 
        """Returns the list of alignments for the corpus, including for 
        each line both the source, target, and translation sentences.
        
        """
        alignments = AlignedCorpus.getAlignments(self, addHistory)
        
        translationLines = self.translationCorpus.readlines()
        for i in range(0, len(alignments)):
            alignment = alignments[i]
            alignment["translation"] = translationLines[i].strip()
                
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
        if not isinstance(corpus, AlignedCorpus):
            raise RuntimeError("aligned data must be of type AlignedCorpus")
   
        trueSource = self.processCorpus(corpus.getSourceCorpus())
        trueTarget = self.processCorpus(corpus.getTargetCorpus())
     
        trueCorpus = AlignedCorpus(trueSource.getStem(), corpus.sourceLang, corpus.targetLang)
        
        if maxLength:
            cleanStem = trueSource.getStem().changeFlag("clean")
            cleanCorpus = self.cutCorpus(trueCorpus, cleanStem, maxLength)
            trueSource.remove()
            trueTarget.remove()
            return cleanCorpus
        else:
            return trueCorpus


    def processCorpus(self, rawCorpus):
        """Process a basic corpus by normalising, tokenising and
        truecasing it.  Intermediary files are deleted, and the final
        truecased file is returned.
         
         """
        if not isinstance(rawCorpus, BasicCorpus):
            rawCorpus = BasicCorpus(rawCorpus)
        
        # STEP 1: tokenisation
        normFile = self.workPath + "/" + rawCorpus.basename().addFlag("norm")
        self.tokeniser.normaliseFile(rawCorpus, normFile)
        tokFile = normFile.changeFlag("tok")
        self.tokeniser.tokeniseFile(normFile, tokFile)
        
        # STEP 2: train truecaser if not already existing
        if not self.truecaser.isModelTrained(rawCorpus.getLang()):
            self.truecaser.trainModel(tokFile)
            
        # STEP 3: truecasing   
        trueFile = tokFile.changeFlag("true")
        self.truecaser.truecaseFile(tokFile, trueFile) 
        
        normFile.remove()
        tokFile.remove()
        
        if (rawCorpus.getStem() + ".indices").exists():
            (rawCorpus.getStem() + ".indices").copy((trueFile.getStem() + ".indices"))
        return BasicCorpus(trueFile)  
    
    
    def processText(self, text, lang):
        """Tokenise and truecase the text, and returns the result.
        
        """
        tokText = self.tokeniser.tokenise(text, lang).strip("\n") + "\n" 
        trueText = self.truecaser.truecase(tokText, lang).strip("\n") + "\n" 
        return trueText
 
 
    def revertTranslatedCorpus(self, corpus):
        """Reverts the corpus content by 'detokenising' it and deescaping
        special characters.
        
        """
        if not isinstance(corpus, TranslatedCorpus):
            raise RuntimeError("aligned data must be of type TranslatedCorpus")
        
        revertedSource = self.revertCorpus(corpus.getSourceCorpus())
        self.revertCorpus(corpus.getTargetCorpus())
        translation = self.revertCorpus(corpus.getTranslationCorpus())
        aCorpus = AlignedCorpus(revertedSource.getStem(), corpus.sourceLang, corpus.targetLang)
        newCorpus = TranslatedCorpus(aCorpus, translation)
        return newCorpus
 
 
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
        if (processedCorpus.getStem() + ".indices").exists():
            (processedCorpus.getStem() + ".indices").copy(finalFile.getStem() + ".indices")
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
        return AlignedCorpus(outputStem, inputCorpus.sourceLang, inputCorpus.targetLang)
  
        
    def getBleuScore(self, translatedCorpus):
        """Returns the BLEU score for the translated corpus.
        
        """
        bleuScript = (install.moses_root  + "/scripts/generic/multi-bleu.perl -lc " 
                      + translatedCorpus.getTargetCorpus())
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
        tmpFile = outputFile + "_tmp"
        result = self.executor.run(cleanScript, inputFile, tmpFile)
        if not result:
            raise RuntimeError("Normalisation of %s has failed"%(inputFile))
        
        outlines = []
        for line in Path(tmpFile).readlines():
            outlines.append(line[0].upper() + line[1:])
        outputFile.writelines(outlines)
        tmpFile.remove()
        
       
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
 
 
 
              
