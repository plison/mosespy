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


"""Creation, update and analysis of machine translation experiments
based on the Moses platform (http://www.statmt.org/moses for details). 
The module relies on the Moses platform, the MGIZA word alignment tool 
and the IRSTLM language modelling tool, which need to be installed
and compile in the base directory. 

The central entity of this module is the Experiment class which allows
the user to easily configure and run translation experiments. 

A typical way to run an entire experiment (from training to evaluation)
is as such:
    exp = Experiment("name of experiment", sourceLang, targetLang)
    exp.trainLanguageModel(lmData)
    exp.trainTranslationModel(trainingData)
    exp.tuneTranslationModel(tuningData)
    exp.evaluateBLEU(testData)
    
SourceLang and targetLang refer to language codes(such as 'fr' or 'de').  
lmData corresponds to the training file used to estimate the language model, 
while trainingData, tuningData and testData refer to aligned corpora (respectively
for constructing the translation models, tuning the parameters and evaluating
the translation quality).

As is the convention in SMT,  an aligned corpora is represented by two files, 
"{corpus-name}.{source-lang} and {corpus-name}.{target-lang} with the
same number of lines.  The aligned corpus is then referred to by its stem 
(i.e. {corpus-name}).    
    
"""

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

import json,  re
import mosespy.system as system
import mosespy.install as install
from mosespy.system import Path
from mosespy.corpus import BasicCorpus, AlignedCorpus, ReferenceCorpus, CorpusProcessor



class Experiment(object):
    """Representation of a translation experiment. The experiment 
    initially consists of a name and a (source, target) language pair.  
    The experiment can be subsequently updated by the following core 
    operations:
    - train a language model from data in the target language,
    - train a translation model (phrase and reordering tables) from 
      aligned data,
    - tune the feature weights from aligned data,
    - evaluate the resulting setup on test data (using e.g. BLEU).
    
    The data and models produced during the experiment are stored in 
    the directory {expDir}/{name of experiment}.  In this directory, 
    the JSON file settings.json functions as a permanent representation 
    of the experiment, allowing experiments to be easily restarted.
    
    """
    
    def __init__(self, expName, sourceLang=None, targetLang=None, nbThreads=2):
        """Start a new experiment with the given name.  If an experiment of 
        same name already exists, its state is reloaded (based on the JSON
        file that records the experiment state). 
        
        Args: 
            sourceLang (str): language code for the source language
            targetLang (str): language code for the target language
            nbThreads (int): number of parallel threads to use
        
        """
                
        self.expPath = Path(install.expDir+expName).getAbsolute()
        self.lm = None
        self.continuous_lm = None
        self.tm = None
        self.iniFile = None
        self.results = None
        
        jsonFile = self.expPath+"/settings.json"
        if jsonFile.exists():
            self._reloadState()
        else:
            self.expPath.resetdir()
            if sourceLang:
                self.sourceLang = sourceLang
            if targetLang:
                self.targetLang = targetLang
        
        checkEnvironment()
        
        self._recordState()
        print ("Experiment " + expName + " (" + self.sourceLang  
               + "-" + self.targetLang + ") successfully started")
        
        self.executor = system.ShellExecutor()
        self.nbThreads = nbThreads
        self.processor = CorpusProcessor(self.expPath, self.executor, self.nbThreads)
        self.decoder = install.decoder
                   
    
    def trainLanguageModel(self, trainFile, preprocess= True, ngram_order=3):
        """Trains the language model used for the experiment.  The method starts
        by inserting start and end characters <s> and </s> to the lines of the
        training files, then estimates the model parameters, builds the model, and
        finally binarises it in the KenLM format.
        
        Args:
            trainFile (str): path to the file containing the training data.      
            preprocess (bool): whether to tokenise and truecase the training data
                before estimating the model parameters
            ngram_order: order of the N-gram
            continuous (bool): whether to use a continuous language model.
    
        If the operation is successful, the binarised language model is set to the
        instance self.lm as a tuple (file path, n-gram order).
        
        """     
           
        print "Building language model based on " + trainFile
        train = BasicCorpus(trainFile)
        if preprocess:
            train = self.processor.processCorpus(train)
        
        sbFile = self.expPath + "/" + train.basename().changeFlag("sb")              
        self.executor.run(install.irstlm_root+"/bin/add-start-end.sh", train, sbFile)
        
        blmFile = self.expPath + "/langmodel.blm." + self.targetLang
        self._estimateLanguageModel(sbFile, ngram_order, blmFile)
        
        sbFile.remove()
        self.lm = (blmFile, ngram_order)
        self._recordState()
  
     
    def trainTranslationModel(self, trainStem, alignment=install.defaultAlignment, 
                              reordering=install.defaultReordering, 
                              preprocess=True, pruning=True):  
        """Trains the translation model for the experiment.  The method relies on
        the Moses script train-model.perl to construct the phrase and reordering
        tables.  MGIZA++ is employed for the word alignment.
        
        The language model must be trained prior to calling this method (else,
        a runtime error is raised).  
        
        Args:
            trainStem (str): path to the aligned corpus used for training the model.  
                The files {trainStem}.{sourceLang} and {trainStem}.{targetLang} must 
                be present in the file system and include the same number of lines.
            alignment (str): optional heuristic for the word alignment. cf. the Moses 
                website for details. Default is 'grow-diag-final-and'
            reordering (str): optional type of model for the reordering. Default is
                'msd-bidirectional-fe'.
            preprocess (bool): whether to tokenise and truecase the training data
                prior to the model estimation.
            pruning (bool): whether to prune the phrase table after constructing the
                model, to remove phrase pairs with near-zero probabilities.
        
        Once all training operations are completed, the method sets the self.tm
        variable to the directory containing the phrase and reordering tables, and
        the variable self.ini to the Moses.ini file.
        
        """
        if not self.lm:
            raise RuntimeError("Language model not yet constructed")
        
        train = AlignedCorpus(trainStem, self.sourceLang, self.targetLang)
        
        if preprocess:         
            train = self.processor.processAlignedCorpus(train)
       
        print ("Building translation model " + self.sourceLang + "-" 
               + self.targetLang + " with " + train.getStem())

        tmDir = self._constructTranslationModel(train, alignment, reordering)
        
        if ((tmDir + "/model/phrase-table.gz").getSize() < 1000
            or not (tmDir +"/model/moses.ini").exists()):
            raise RuntimeError("Construction of translation model FAILED")
            
        print "Finished building translation model in directory " + tmDir.getDescription()
        self.tm= tmDir + "/model"
        self.iniFile = self.tm +"/moses.ini"
        if pruning:
            self._prunePhraseTable()
        if self.continuous_lm:
            features = {"factor":0, "path": self.continuous_lm[0], 
                        "order":self.continuous_lm[1], "lazyken":0}
            MosesConfig(self.iniFile).addFeatureFunction("KENLM", "LM1", features, 0.5)
        self._recordState()
        
 
    def tuneTranslationModel(self, tuningStem, preprocess=True):
        """Tunes the weights of the translation model components in order
        to optimise the translation accuracy on the tuning set.  The method
        employs the mert-moses.pl script for this purpose.
        
        The method requires the translation model to be constructed prior to 
        called this method (else, a runtime error is raised).
        
        Args:
            tuningStem (str): path to the aligned corpus for tuning. The files 
                {tuningStem}.{sourceLang} and {tuningStem}.{targetLang} must 
                be present and include the same number of lines.
            preprocess (bool): whether to tokenise and truecase the data
                prior to the tuning process.
        
        At the end of the operation, the method changes the self.iniFile to
        the new moses.ini file that contains the final component weights.
        
        """
        
        if not self.tm:
            raise RuntimeError("Translation model not yet constructed")
        
        tuning = AlignedCorpus(tuningStem, self.sourceLang, self.targetLang)
        
        if preprocess:         
            tuning = self.processor.processAlignedCorpus(tuning, False)
        
        print ("Tuning translation model " + self.sourceLang + "-" 
               + self.targetLang + " with " + tuning.getStem())
        
        tuneDir = self.expPath+"/tunedmodel"
        tuningScript = self._getTuningScript(tuneDir, tuning.getStem())
        tuneDir.resetdir()
        result = self.executor.run(tuningScript)
        if not result or not (tuneDir + "/moses.ini").exists():
            raise RuntimeError("Tuning of translation model FAILED")
            
        print "Finished tuning translation model in directory " + tuneDir.getDescription()
        self.iniFile = tuneDir + "/moses.ini"
        self._recordState()
      
      
   
    def translate(self, text, preprocess=True):
        """ Translates the text given as argument and returns the result.
        
        The translation model must be constructed (and tuned) prior to calling
        this method (else, a runtime error is raised).
        
        Args:
            text (str): the text to translate.  Each sentence must be separated
                by a line break.
            preprocess (bool): whether to tokenise and truecase the text
                prior to translation.
        
        After translation, the translated output is automatically 'detokenised' and 
        special characters are also 'deescaped' in order to get printable output.
        
        """   
        
        if not self.iniFile:
            raise RuntimeError("Translation model is not yet trained and tuned!")
        print ("Translating text: \"" + text + "\" from " + 
               self.sourceLang + " to " + self.targetLang)

        text = text.strip("\n") + "\n"
        if preprocess:
            text = self.processor.processText(text, self.sourceLang)
            
        transScript = self._getTranslateScript()
        translation = self.executor.run_output(transScript, stdin=text)
        return self.processor.revertText(translation, self.targetLang)
        
   
    def translateFile(self, infile, outfile, preprocess=True, filterModel=True,
                      revertOutput=True):
        """Translates sentences from 'infile' and writes the results in 'outfile'.
        
        The translation model must be constructed (and tuned) prior to calling
        this method (else, a runtime error is raised).
        
        Args:
            infile (str): the input file to translate. The input file must
                include sentences in the source language, and its file extension
                must correspond to the source language code.
            outfile (str): the output file in which to write the translations.
                Its file extension must correspond to the target language code.
            preprocess (bool): whether to tokenise and truecase the input
                prior to translation.
            filterModel (bool): whether to filter the phrase-table to reduce
                it to the pairs necessary for translating 'infile'.  The 
                filtering takes some time but speeds up the decoding.
            revertOutput (bool): whether to detokenise and deescape the translation
                outputs (useful to get good-looking output, but not appropriate
                for evaluation on reference translations).     
        
        """   
        
        inCorpus = BasicCorpus(infile)
        Path(outfile).resetfile()
        outCorpus = BasicCorpus(outfile)
        if inCorpus.getLang()!=self.sourceLang:
            print "Input file must have extension %s"%(self.sourceLang)
        if outCorpus.getLang()!=self.targetLang:
            print "Output file must have extension %s"%(self.targetLang)
            
        if preprocess:
            inCorpus = self.processor.processCorpus(inCorpus)
       
        if filterModel:
            filterDir = self._getFilteredModel(inCorpus)
            initFile = filterDir + "/moses.ini"
        elif self.iniFile:
            initFile = self.iniFile
        else:
            raise RuntimeError("Translation model is not yet trained!")
        
        print ("Translating file \"" + inCorpus + "\" from " + 
               self.sourceLang + " to " + self.targetLang)

        transScript = self._getTranslateScript(initFile, inCorpus)
        
        result = self.executor.run(transScript, stdout=outCorpus)
                    
        if filterDir:
            filterDir.remove()
       
        if not result:
            raise RuntimeError("Translation of file " + str(inCorpus) + " FAILED")
        
        if revertOutput: 
            outCorpus = self.processor.revertCorpus(outCorpus)
            outCorpus.rename(outfile)
        
       
    def evaluateBLEU(self, testStem, preprocess=True):
        """Evaluates the translation quality on development/test data, and 
        returns its BLEU score.
        
        Args:
            testStem (str): the path to the aligned corpus for the evaluation.  
                The files {testStem}.{sourceLang} and {testStem}.{targetLang} 
                must be present and include the same number of lines.
            preprocess (bool): whether to tokenise and truecase the test data
                prior to the evaluation.
                
        At the end of the evaluation, the translation results and their BLEU score 
        are returned, and the instance variable self.results records the translation 
        results (useful for later analysis).
                
        TODO: allow for more than one reference translation
        TODO: allow for other metrics than BLEU
        
        """
 
        testCorpus = ReferenceCorpus(testStem, self.sourceLang, self.targetLang)
        print ("Evaluating BLEU scores with test data: " + testStem 
               + " (Number of references: %i)"%(len(testCorpus.getReferenceCorpora())))
        
        if preprocess:
            testCorpus = self.processor.processAlignedCorpus(testCorpus, False)
                    
        transPath = (self.expPath + "/" + testCorpus.getStem().basename().
                     addFlag("translated", reverseOrder=True) + "." + self.targetLang)
        
        self.translateFile(testCorpus.getSourceCorpus(), transPath, False, True, False)    
        testCorpus.addTranslation(transPath)      
        self.results = self.processor.revertReferenceCorpus(testCorpus)
        self._recordState()
        
        bleu, bleu_output = self.processor.getBleuScore(testCorpus)
        print bleu_output
        return testCorpus, bleu
    


    def binariseModel(self):
        """Binarises the phrase and reordering tables.  This operation takes some
        time but makes the models must faster to load at decoding time.
        
        The translation model must already be constructed before calling this method.
        
        """
        
        print "Binarise translation model " + self.sourceLang + " -> " + self.targetLang
        if not self.iniFile:
            raise RuntimeError("Translation model has not yet been trained and tuned")
        
        binaDir = self.expPath+"/binmodel"
        config = MosesConfig(self.iniFile)
        phraseTable = config.getPhraseTable()
        reorderingTable = config.getReorderingTable()
        
        binaDir.resetdir()
        binScript = (install.moses_root + "/bin/processPhraseTable" + " -ttable 0 0 " + phraseTable 
                     + " -nscores 5 -out " + binaDir + "/phrase-table")
        result1 = self.executor.run(binScript)
        if not result1:
            raise RuntimeError("could not binarise translation model (phrase table process)")
        
        binScript2 = (install.moses_root + "/bin/processLexicalTable" + " -in " + reorderingTable 
                      + " -out " + binaDir + "/reordering-table")
        result2 = self.executor.run(binScript2)
        if not result2:
            raise RuntimeError("could not binarise translation model (lexical table process)")
         
        config.replacePhraseTable(binaDir+"/phrase-table", "PhraseDictionaryBinary")
        config.replaceReorderingTable(binaDir+"/reordering-table")
        
        self.tm = binaDir
        self._recordState()
        print "Finished binarising the translation model in directory " + binaDir.getDescription()
      

    def trainContinuousLanguageModel(self, trainFile, preprocess= True, ngram_order=3):
           
        print "Building language model based on " + trainFile
        train = BasicCorpus(trainFile)
        if preprocess:
            train = self.processor.processCorpus(train)
        
        sbFile = self.expPath + "/" + train.basename().changeFlag("sb")              
        with open(sbFile, 'w') as sbContent:
            lines = train.readlines()
            for i in range(0, len(lines)):
                line = lines[i].replace("\n", "")
                sbContent.write(line + " <t> " if i < len(lines)-1 else line)

        
        blmFile = self.expPath + "/langmodel.continuous.blm." + self.targetLang
        self._estimateLanguageModel(sbFile, ngram_order, blmFile, True)
        
        sbFile.remove()

        self.continuous_lm = (blmFile, ngram_order)
        self._recordState()
             
     
     
    def queryLanguageModel(self, text):
        """Queries the language model with a given sentence.  The method returns the
        log-prob, perplexity, number of tokens and out-of-vocabulary tokens for the
        sentence given the current language model.
        
        The language model must already be constructed before calling this method.
        
        Args:
            text (str): the sentence to query.
            
        Returns:
            A dictionary with the following values: 'logprob' (log-probability for the 
                sentence), 'perplexity' (perplexity including OOVs), 'perplexity2' (excluding 
                OOVs), 'OOVs' (number of OOVs tokens), and 'tokens' (number of tokens).
        
        """
        if not self.lm:
            raise RuntimeError("Language model is not yet trained")
        queryScript = (install.moses_root + "/bin/query "+ self.lm[0])
        output = self.executor.run_output(queryScript, text+"\n")
        regex = (r".*" + re.escape("Total:") + r"\s+([-+]?[0-9]*\.?[0-9]*).+" 
                 + re.escape("Perplexity including OOVs:") + r"\s+([-+]?[0-9]*\.?[0-9]*).+"  
                 + re.escape("Perplexity excluding OOVs:") + r"\s+([-+]?[0-9]*\.?[0-9]*).+" 
                 + re.escape("OOVs:") + r"\s+([0-9]*).+" 
                 + re.escape("Tokens:") + r"\s+([0-9]*)")
        s = re.search(regex, output, flags=re.DOTALL)
        if s:
            return {"logprob":float(s.group(1)), "perplexity":float(s.group(2)), 
                    "perplexity2":float(s.group(3)),
                    "OOVs":int(s.group(4)), "tokens": int(s.group(5))}
        else:
            print "Query results could not be parsed: " + str(output)
    
   
    def reduceSize(self):
        """Reduces the size of the experiment directory by removing all uncessary files, 
        such as intermediary corpus files and optional files generated during the model
        training and tuning.
        
        """
        if self.tm:
            (self.tm.getUp()+"/corpus").remove()
            (self.tm.getUp()+"/giza." + self.sourceLang + "-" + self.targetLang).remove()
            (self.tm.getUp()+"/giza." + self.targetLang + "-" + self.sourceLang).remove()
            config = MosesConfig(self.iniFile)
            paths = config.getPaths()
            for f in (self.tm).listdir():
                absolutePath = Path(self.tm+"/" + f)
                if absolutePath not in paths and f !="moses.ini":
                    print "Removing " + absolutePath
                    absolutePath.remove()
        
        if self.iniFile.getUp() != self.tm:
            for f in self.iniFile.getUp().listdir():
                fi = self.iniFile.getUp() + "/" + f
                if f !="moses.ini":
                    fi.remove()
        for f in self.expPath.listdir():
            if "model" not in f and "settings" not in f:
                (self.expPath+"/" + f).remove()

        print "Finished reducing the size of experiment directory " + self.expPath
 
    
    def copy(self, nexExpName):
        """Copies the current experiment under a new name.
        
        Args:
            newExpName (str): the new name for the copied experiment.
        
        """
        newexp = Experiment(nexExpName, self.sourceLang, self.targetLang)
        newexp.lm = self.lm
        newexp.continuous_lm = self.continuous_lm
        newexp.tm = self.tm
        newexp.nbThreads = self.nbThreads
        newexp.iniFile = self.iniFile
        newexp.sourceLang = self.sourceLang
        newexp.targetLang = self.targetLang
        newexp.results = self.results
        newexp.processor = self.processor
        return newexp
 
    
    def _estimateLanguageModel(self, corpusFile, ngram_order, outputFile, continuous=False):
        
        lmFile = outputFile.changeFlag("rawlm")
        system.setEnv("IRSTLM", install.irstlm_root)
        lmScript = ((install.irstlm_root + "/bin/build-lm.sh" + " -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp-%s"
                    )%(corpusFile, lmFile, ngram_order, self.expPath.basename())) 
        self.executor.run(lmScript)
                           
        arpaFile = lmFile.changeFlag("arpa")
        arpaScript = (install.irstlm_root + "/bin/compile-lm "
                      + "--text=yes %s %s"%(lmFile+".gz", arpaFile))
        self.executor.run(arpaScript)  

        blmScript = (install.moses_root + "/bin/build_binary -w after " 
                     + (" -s " if continuous else "")
                     + " -i " + arpaFile + " " + outputFile)
        self.executor.run(blmScript)
        print "New binarised language model: " + outputFile.getDescription() 
        
        (lmFile + ".gz").remove()
        arpaFile.remove()

        if outputFile.getSize() == 0:
            raise RuntimeError("Error: generated language model is empty")
        

   
    def _prunePhraseTable(self, probThreshold=0.0001):
        """Prune the phrase table with the provided probability threshold.
        
        The translation model must already be constructed before calling this method.
        
        Args:
            probThreshold: the probability threshold under which phrase pairs are pruned.
            
        """
        
        if not self.tm or not self.iniFile:
            raise RuntimeError("Translation model is not yet constructed")
        
        config = MosesConfig(self.iniFile)
        phrasetable = config.getPhraseTable()
        newtable = Path(config.getPhraseTable()[:-2] + "reduced.gz")

        if not phrasetable.exists():
            print "Original phrase table has been removed, pruning cancelled"
            return
        
        zcatExec = "gzcat" if system.existsExecutable("gzcat") else "zcat"
        pruneScript = (zcatExec + " %s|" + install.moses_root 
                       + "/scripts/training/threshold-filter.perl " 
                       + str(probThreshold) + " | gzip - > %s")%(phrasetable, newtable)
        result = self.executor.run(pruneScript)
        if result:        
            config.replacePhraseTable(newtable)                          
            phrasetable.remove()              
        else:
            print "Pruning of translation table FAILED"
        

    def _constructTranslationModel(self, trainCorpus, alignment, reordering):
        """Internal method for constructing a translation model given a training 
        corpus, an alignment heuristic and a reordering method. The method returns 
        the directory containing the resulting model data.
        
        The method should not be called from outside the module, please use 
        trainTranslationModel(...) instead.
        
        """
        tmDir = self.expPath + "/translationmodel"
        tmDir.resetdir()
        tmScript = self._getTrainScript(tmDir, trainCorpus.getStem(), alignment, reordering)
        result = self.executor.run(tmScript)
        if not result:
            raise RuntimeError("construction of translation model FAILED")
        return tmDir



    def _getTrainScript(self ,tmDir, trainData, alignment, reordering, 
                        firstStep=1, lastStep=9, direction=None):
        """Forges the training script (based on train-model.perl) given the provided
        arguments.
        
        Args:
            tmDir (str): directory in which to put all the files
            trainData (str): stem for the training data
            alignment (str): alignment heuristic
            reordering (str): reordering method
            firstStep (int): first step for the training (see http://statmt.org/moses)
            lastStep (int): last step for the training
            direction (int): direction for the estimation in step 2.  If left
                unspecified, both directions are estimated.
        
        """
        if not self.lm: 
            raise RuntimeError("LM for " + self.targetLang  + " not yet trained")
        tmScript = (install.moses_root + "/scripts/training/train-model.perl" + " "
                    + "--root-dir " + tmDir + " -corpus " +  trainData
                    + " -f " + self.sourceLang + " -e " + self.targetLang 
                    + " -alignment " + alignment + " " 
                    + " -reordering " + reordering + " "
                    + " -lm 0:" +str(self.lm[1])
                    +":"+self.lm[0]+":8"       # 8 because binarised with KenLM
                    + " -external-bin-dir " + install.mgizapp_root + "/bin" 
                    + " -cores %i -mgiza -mgiza-cpus %i -parallel "
                    + " --first-step %i --last-step %i "
                    + " -sort-buffer-size 20%% -sort-compress gzip -sort-parallel %i" 
                    )%(self.nbThreads, self.nbThreads, firstStep, lastStep, self.nbThreads)
        if direction:
            tmScript += " --direction " + str(direction)
        return tmScript
                       
        
        
    def _getTuningScript(self, tuneDir, tuningStem):
        """Forges the tuning script (based on mert-moses.pl) given the provided arguments.
        
        Args:
            tuneDir: directory in which to put all the tuning files
            tuningStem: stem for the tuning data.
            
        """

        tuneScript = (install.moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + tuningStem + "." + self.sourceLang + " " 
                      + tuningStem + "." + self.targetLang + " "
                      + self.decoder + " "
                      + self.iniFile
                      + " --mertdir " + install.moses_root + "/bin/"
                      + " --decoder-flags=\'-threads %i -v 0' --working-dir " + tuneDir
                      )%(self.nbThreads)
        return tuneScript
        

    
    def _getTranslateScript(self, initFile=None, inputFile=None):
        """Forges the translation script (based on the Moses decoder) given the provided
        moses.ini configuration file and the input file to translate.
        
        Args:
            initFile: Moses configuration file.  If left unspecified, uses self.iniFile.
            inputFile: input file to translate.  If left unspecified, translated from
                standard input.
        
        """
        if not initFile:
            initFile = self.iniFile
        script = (self.decoder  + " -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(self.nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
        return script
                                                                   
    
    def _getFilteredModel(self, testSource):
        """Constructs a filtered translation model that is tailored for the particular
        testing data.
        
        Args:
            testSource: the aligned data to apply for the filter.
            
        """
        if not self.iniFile:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.expPath+ "/filteredmodel-" +  testSource.basename().getStem()
        filteredDir.remove()

        filterScript = (install.moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + self.iniFile + " "
                        + testSource)
                        #+ " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        return filteredDir
            
    
    def _recordState(self):
        """Records the current state of the experiment in the JSON file.
        
        """
        settings = {"path":self.expPath, "source":self.sourceLang, "target":self.targetLang}
        if self.lm:
            settings["lm"] = {"lm":self.lm[0], "ngram_order":self.lm[1]}
        if self.continuous_lm:
            settings["continuous_lm"] = {"lm":self.lm[0], "ngram_order":self.lm[1]}
        if self.tm:
            settings["tm"] = self.tm
        if self.iniFile:
            settings["ini"] = self.iniFile
        if self.results:
            settings["results"] = {"stem":self.results.getStem(), 
                                   "translation":self.results.getTranslationCorpus()}
        dump = json.dumps(settings)
        with open(self.expPath+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)
            
            
    def _reloadState(self):
        """Reloads the experiment variables given the existing JSON file.
        
        """
        print "Existing experiment, reloading known settings..."
        with open(self.expPath+"/settings.json", 'r') as jsonFile:
            settings = json.loads(jsonFile.read())
            if settings.has_key("source"):
                self.sourceLang = settings["source"]
            if settings.has_key("target"):
                self.targetLang = settings["target"]
            if settings.has_key("lm"):
                lm = settings["lm"]
                self.lm = (Path(lm["lm"]), int(lm["ngram_order"]))
            if settings.has_key("continuous_lm"):
                lm = settings["continuous_lm"]
                self.continuous_lm = (Path(lm["lm"]), int(lm["ngram_order"]))
            if settings.has_key("tm"):
                self.tm = Path(settings["tm"])
            if settings.has_key("ini"):
                self.iniFile = Path(settings["ini"])
            if settings.has_key("results"):
                self.results = ReferenceCorpus(settings["results"]["stem"], self.sourceLang, self.targetLang)
                self.results.addTranslation(settings["results"]["translation"])            
           
    
class MosesConfig():
    """Representation of a moses.ini configuration file.  The class provides
    functions to easily extract and modify information in this file.
    
    """
    
    def __init__(self, configFile):
        """Creates a new MosesConfig object based on the configuration file.
        
        """
        self.configFile = Path(configFile)
        if not self.configFile.exists():
            raise RuntimeError("File " + self.configFile + " does not exist")

    def getPhraseTable(self):
        """Returns the path to the phrase table file specified in the
        configuration file.
        
        """
        parts = self._getParts() 
        if parts.has_key("feature"):
            for l in parts["feature"]:
                if "PhraseDictionary" in l:
                    s = re.search(re.escape("path=") + r"((\S)+)", l)
                    if s:
                        return Path(s.group(1))
        print "Cannot find path to phrase table"
        
    
    def replacePhraseTable(self, newPath, phraseType="PhraseDictionaryMemory"):
        """Replaces the path to the phrase table with a new path.  In addition,
        the type of the phrase table can be modified (useful when doing e.g.
        binarisation).
        
        """
        parts = self._getParts() 
        if parts.has_key("feature"):
            newList = []
            for l in parts["feature"]:
                if "PhraseDictionary" in l:
                    s = re.search(re.escape("path=") + r"((\S)+)", l)
                    if s:
                        existingPath = s.group(1)
                        l = l.replace(existingPath, newPath)
                        l = l.replace(l.split()[0], phraseType)
                newList.append(l)
            parts["feature"] = newList
        self._updateFile(parts)
        

    def getReorderingTable(self):
        """Returns the path to the reordering table file as specified in the
        configuration file.
        
        """
        parts = self._getParts() 
        if parts.has_key("feature"):
            for l in parts["feature"]:
                if "LexicalReordering" in l:
                    s = re.search(re.escape("path=") + r"((\S)+)", l)
                    if s:
                        return Path(s.group(1))
        print "Cannot find path to reordering table"
        
    
    def replaceReorderingTable(self, newPath):
        """Replaces the path to the reordering table with a new path.
        
        """
        parts = self._getParts() 
        if parts.has_key("feature"):
            newList = []
            for l in parts["feature"]:
                if "LexicalReordering" in l:
                    s = re.search(re.escape("path=") + r"((\S)+)", l)
                    if s:
                        existingPath = s.group(1)
                        l = l.replace(existingPath, newPath)
                newList.append(l)
            parts["feature"] = newList
        self._updateFile(parts)
        
    
    def replaceLanguageModel(self, languageModel):
        parts = self._getParts() 
        if parts.has_key("feature"):
            newList = []
            for l in parts["feature"]:
                if "KENLM" in l:
                    l = re.sub(re.escape("path=") + r"((\S)+)", 
                               "path=" + languageModel[0], l)
                    l = re.sub(re.escape("order=") + r"((\d)+)", 
                               "order=" + str(languageModel[1]), l)
                newList.append(l)
            parts["feature"] = newList
        self._updateFile(parts)
    
    def addFeatureFunction(self, featType, featName, features, *weights):
        newFunction = "%s name=%s "%(featType, featName)
        for feat in features:
            newFunction += " %s=%s "%(feat, str(features[feat]))
        parts = self._getParts()
        parts["feature"].append(newFunction)
        parts["weight"].append(featName + "= " + " ".join([str(w) for w in weights]))
        self._updateFile(parts)
    
    def removePart(self, partname):
        """Removes a section in the configuration file.
        
        """
        parts = self._getParts()
        if parts.has_key(partname):
            del parts[partname]
        self._updateFile(parts)
        
    
    def getPaths(self):
        """Returns all file paths specified in the configuration file.
        
        """
        paths = set()
        parts = self._getParts() 
        for part in parts:
            for l in parts[part]:
                s = re.search(re.escape("path=") + r"((\S)+)", l)
                if s:
                    paths.add(Path(s.group(1)).getAbsolute())
        return paths
        
    
    def display(self):
        """ Prints out the configuration file to standard output.
        
        """
        lines = self.configFile.readlines()
        for l in lines:
            print l.strip()
        
    def _updateFile(self, newParts):
        """ Updates the configuration file with new sections, and writes
        the result onto the file.
        
        """
        with open(self.configFile, 'w') as configFileD:
            for part in newParts:
                configFileD.write("[" + part + "]\n")
                for l in newParts[part]:
                    configFileD.write(l+"\n")
                configFileD.write("\n")
        
    
    def _getParts(self):
        """Returns a dictionary containing the text in each section of
        the moses.ini configuration file.
        
        """
        lines = self.configFile.readlines()
        parts = {}
        for  i in range(0, len(lines)):
            l = lines[i].strip()
            if l.startswith("[") and l.endswith("]"):
                partType = l[1:-1]
                start = i+1
                end = len(lines)
                for  j in range(i+1, len(lines)):
                    l2 = lines[j].strip()
                    if l2.startswith("[") and l2.endswith("]"):
                        end = j-1
                        break
                parts[partType] = []
                for line in lines[start:end]:
                    if line.strip():
                        parts[partType].append(line.strip())
        return parts
    
    

def checkEnvironment():
    """Checking that all executables and binaries are in place for the experiment.
    If not, raises a runtime error. All third-party tools (Moses, MGIZA++ IRSTLM)
    must be present and compiled. Furthermore, the method also checks that some
    shell tools such as sort and zcat are present with a recent version (to 
    allow for e.g. parallel sorting). 
    
    Note that on Mac OS X, one may need to install the 'coreutils' package using
    brew to get things to work properly.
    
    """
    if not install.moses_root.exists():
        raise RuntimeError("Moses directory does not exist")
    elif not (install.moses_root + "/bin/moses").exists():
        raise RuntimeError("Moses is not compiled!")
    elif not install.mgizapp_root.exists():
        raise RuntimeError("MGIZA++ directory does not exist")
    elif not (install.mgizapp_root + "/bin/mgiza").exists():
        raise RuntimeError("MGIZA++ is not compiled!")
    elif not install.irstlm_root.exists():
        raise RuntimeError("IRSTLM directory does not exist")
    elif not (install.irstlm_root+"/bin/compile-lm").exists():
        raise RuntimeError("IRSTLM is not compiled!")
    
    # Correcting strange bug when Eclipse changes the default PATH
    if "/usr/local/bin" not in system.getEnv()["PATH"]:
        system.setEnv("PATH", "/usr/local/bin", override=False)

    sortCmd = "gsort" if system.existsExecutable("gsort") else "sort"
    if len(str(system.run_output(sortCmd + " --help | grep \"parallel\""))) < 2:
        raise RuntimeError("sort command does not accept parallel switch, please upgrade!")
    zcatCmd = "gzcat" if system.existsExecutable("gzcat") else "zcat"
    zcatTest = "touch test ; gzip test ; %s test.gz ; rm test.gz"%(zcatCmd)
    if len(str(system.run_output(zcatTest))) > 2:
        raise RuntimeError("zcat command does not work properly, please upgrade!")
        

    

