# -*- coding: utf-8 -*- 

import os, json, copy,  re
from mosespy import shellutils, pathutils, nlputils
from mosespy.pathutils import Path
from mosespy.nlputils import Tokeniser,TrueCaser
from mosespy.textutils import AlignedCorpus

rootDir = Path(__file__).getUp().getUp()
expDir = rootDir + "/experiments/"
moses_root = rootDir + "/moses" 
mgizapp_root = rootDir + "/mgizapp"
irstlm_root = rootDir + "/irstlm"
os.environ["IRSTLM"] = irstlm_root
defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"


class Experiment(object):
    
    executor = shellutils.CommandExecutor()
    
    def __init__(self, expName, sourceLang=None, targetLang=None):
        self.settings = {}
        self.settings["name"] = expName
        
        self.settings["path"] = Path(expDir+self.settings["name"])
        
        jsonFile = self.settings["path"]+"/settings.json"
        if jsonFile.exists():
            print "Existing experiment, reloading known settings..."
            self.settings = json.loads(open(jsonFile).read())
            self.settings = pathutils.convertToPaths(self.settings)
        else:
            self.settings["path"].make()
            if sourceLang:
                self.settings["source"] = sourceLang
                self.settings["source_long"] = nlputils.getLanguage(sourceLang)
            if targetLang:
                self.settings["target"] = targetLang
                self.settings["target_long"] = nlputils.getLanguage(targetLang)
                
        self._recordState()
        print ("Experiment " + expName + " (" + self.settings["source"]  
               + "-" + self.settings["target"] + ") successfully started")
        
        self.tokeniser = Tokeniser(self.executor)
        self.truecaser = {}
        self.truecaser[sourceLang] = TrueCaser(self.executor, self.settings["path"]+"/truecasingmodel."+sourceLang)
        self.truecaser[targetLang] = TrueCaser(self.executor, self.settings["path"]+"/truecasingmodel."+targetLang)
      
    
    def doWholeShibang(self, alignedStem, lmFile=None):
        
        corpus = AlignedCorpus(alignedStem, self.settings["source"], self.settings["target"])
        trainCorpus, tuneCorpus, testCorpus = corpus.divideData(self.settings["path"])
        
        if not lmFile:
            lmFile = alignedStem + "." + self.settings["target"]
        newLmFile = Path(lmFile).changePath(self.settings["path"]).setInfix("wotest")
        corpus.filterLmData(lmFile, newLmFile)
        self.trainLanguageModel(newLmFile)
        
        self.trainTranslationModel(trainCorpus.getAlignedStem())
        self.tuneTranslationModel(tuneCorpus.getAlignedStem())
        self.evaluateBLEU(testCorpus.getAlignedStem())
                    
    
    def trainLanguageModel(self, trainFile, preprocess= True, ngram_order=3):
  
        trainFile = Path(trainFile)
        lang = trainFile.getSuffix()

        if preprocess:
            trainFile = self._processRawData(trainFile)["true"]

        print "Building language model based on " + trainFile
        
        sbFile = trainFile.changePath(self.settings["path"]).setInfix("sb")
                
        self.executor.run(irstlm_root + "/bin/add-start-end.sh", trainFile, sbFile)
        
        lmFile = self.settings["path"] + "/langmodel.lm." + lang
        lmScript = ((irstlm_root + "/bin/build-lm.sh" + " -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp-%s"
                    )%(sbFile, lmFile, ngram_order, self.settings["name"])) 
        self.executor.run(lmScript)
                           
        arpaFile = self.settings["path"] + "/langmodel.arpa." + lang
        arpaScript = (irstlm_root + "/bin/compile-lm" + " --text=yes %s %s"%(lmFile+".gz", arpaFile))
        self.executor.run(arpaScript)  

        blmFile = self.settings["path"] + "/langmodel.blm." + lang
        blmScript = moses_root + "/bin/build_binary -w after " + " " + arpaFile + " " + blmFile
        self.executor.run(blmScript)
        print "New binarised language model: " + blmFile.getDescription() 

        sbFile.remove()
        (lmFile + "gz").remove()
        arpaFile.remove()

        self.settings["lm"] = {"ngram_order":ngram_order, "blm": blmFile}
        self._recordState()
    
    
    def trainTranslationModel(self, trainStem, alignment=defaultAlignment, 
                              reordering=defaultReordering, preprocess=True, nbThreads=2):
        
        trainStem = Path(trainStem)
        if preprocess:         
            trainStem = self._processAlignedData(trainStem)["clean"]
       
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + trainStem)

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self._getTrainScript(tmDir, trainStem, nbThreads, alignment, reordering)
        tmDir.reset()
        result = self.executor.run(tmScript)
        if result:
            print "Finished building translation model in directory " + tmDir.getDescription()
            self.settings["tm"]=tmDir
            self._prunePhraseTable()
            self._recordState()
        else:
            print "Construction of translation model FAILED"
  

    def tuneTranslationModel(self, tuningStem, preprocess=True, nbThreads=2):
        
        tuningStem = Path(tuningStem)
        if preprocess:         
            tuningStem = self._processAlignedData(tuningStem)["clean"]
        
        print ("Tuning translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + tuningStem)
        
        tuneDir = self.settings["path"]+"/tunedmodel"
        tuningScript = self._getTuningScript(tuneDir, tuningStem, nbThreads)
        tuneDir.reset()
        result = self.executor.run(tuningScript)
        if result:
            print "Finished tuning translation model in directory " + tuneDir.getDescription()
            self.settings["ttm"]=tuneDir
            self._recordState()
        else:
            print "Tuning of translation model FAILED"
          



    def binariseModel(self):
        print "Binarise translation model " + self.settings["source"] + " -> " + self.settings["target"]
        if not self.settings.has_key("ttm"):
            raise RuntimeError("Translation model has not yet been trained and tuned")
        
        binaDir = self.settings["path"]+"/binmodel"
        phraseTable = self.settings["tm"]+"/model/phrase-table.gz"
        reorderingTable = self.settings["tm"]+"/model/reordering-table.wbe-" + self.settings["reordering"] + ".gz"
        
        binaDir.reset()
        binScript = (moses_root + "/bin/processPhraseTable" + " -ttable 0 0 " + phraseTable 
                     + " -nscores 5 -out " + binaDir + "/phrase-table")
        result1 = self.executor.run(binScript)
        if not result1:
            raise RuntimeError("could not binarise translation model (phrase table process)")
        
        binScript2 = (moses_root + "/bin/processLexicalTable" + " -in " + reorderingTable 
                      + " -out " + binaDir + "/reordering-table")
        result2 = self.executor.run(binScript2)
        if not result2:
            raise RuntimeError("could not binarise translation model (lexical table process)")
        
        initLines = Path(self.settings["ttm"]+"/moses.ini").readlines()
        with open(binaDir+"/moses.ini", 'w') as newConfig:
            for l in initLines:
                l = l.replace("PhraseDictionaryMemory", "PhraseDictionaryBinary")
                l = l.replace(phraseTable, binaDir + "/phrase-table")
                l = l.replace(reorderingTable, binaDir + "/reordering-table")
                newConfig.write(l)
        
        self.settings["btm"] = binaDir
        self._recordState()
        print "Finished binarising the translation model in directory " + binaDir.getDescription()

      
   
    def translate(self, text, preprocess=True, nbThreads=2):
        if self.settings.has_key("btm"):
            initFile = self.settings["btm"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained and tuned!")
        print ("Translating text: \"" + text + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        if preprocess:
            text = self.tokeniser.tokenise(text, self.settings["source"])
            text = self.truecaser[self.settings["source"]].truecase(text)

        transScript = self._getTranslateScript(initFile, nbThreads)

        return self.executor.run_output(transScript, stdin=text)
        
   
    def translateFile(self, infile, outfile, preprocess=True, filterModel=True, nbThreads=2):
           
        infile = Path(infile)
        if filterModel:
            filterDir = self._getFilteredModel(infile)
            initFile = filterDir + "/moses.ini"
        elif self.settings.has_key("btm"):
            initFile = self.settings["btm"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained and tuned!")
        print ("Translating file \"" + infile + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        if preprocess:
            infile = self._processRawData(infile)["true"]

        transScript = self._getTranslateScript(initFile, nbThreads, inputFile=infile)
        
        result = self.executor.run(transScript, stdout=outfile)

        if filterDir:
            filterDir.remove()
        
        if not result:
            print "Translation of file " + infile + " FAILED"
        return result
        
       
    def evaluateBLEU(self, testData, preprocess=True):
 
        testCorpus = AlignedCorpus(testData, self.settings["source"], self.settings["target"])
        print ("Evaluating BLEU scores with test data: " + testData)
        
        if preprocess:
            testSource = self._processRawData(testCorpus.getSourceFile())["true"]
            testTarget = self._processRawData(testCorpus.getTargetFile())["true"]
          
        translationfile = testTarget.setInfix("translated")
        trans_result = self.translateFile(testSource, translationfile, filterModel=True, preprocess=False)
        
        if trans_result:
            if not self.settings.has_key("tests"):
                self.settings["tests"] = []
            test = {"stem":testData, "translation":translationfile}
            self.settings["tests"].append(test)
            print "Appending new test description in settings"

            bleuScript = moses_root + "/scripts/generic/multi-bleu.perl -lc " + testTarget
            bleu_output = shellutils.run_output(bleuScript, stdin=translationfile)
            print bleu_output.strip()
            s = re.search(r"=\s(([0-9,\.])+)\,", bleu_output)
            if s:
                test["bleu"] = s.group(1)
            self._recordState()
        

    def analyseErrors(self):
        
        if not self.settings.has_key("tests"):
            raise RuntimeError("you must first perform an evaluation before the analysis")

        testStem = self._revertProcessedData(self.settings["tests"][-1]["stem"])
        translation = self._revertProcessedData(self.settings["tests"][-1]["translation"])
  
        corpus = AlignedCorpus(testStem, self.settings["source"], self.settings["target"])
        corpus.addActualTranslations(translation)  
        
        alignments = corpus.getAlignments(addHistory=True)      
     
        analyseShortAnswers(alignments)
        analyseQuestions(alignments)
        analyseBigErrors(alignments)

   
    def reduceSize(self):
        if self.settings.has_key("tm"):
            (self.settings["tm"]+"/corpus").remove()
            (self.settings["tm"]+"/giza." + self.settings["source"] + "-" + self.settings["target"]).remove()
            (self.settings["tm"]+"/giza." + self.settings["target"] + "-" + self.settings["source"]).remove()
            with open(self.settings["tm"]+"/model/moses.ini", 'r') as iniFile:
                iniContent = iniFile.read()
            for f in (self.settings["tm"]+"/model").listdir():
                if f not in iniContent and f != "moses.ini":
                    (self.settings["tm"]+"/model/" + f).remove()
        
        if self.settings.has_key("ttm"):
            for f in self.settings["ttm"].listdir():
                fi = self.settings["ttm"] + "/" + f
                if f != "moses.ini":
                    fi.remove()

        print "Finished reducing the size of experiment directory " + self.settings["path"]
 
    
    def copy(self, nexExpName):
        newexp = Experiment(nexExpName, self.settings["source"], self.settings["target"])
        settingscopy = copy.deepcopy(self.settings)
        for k in settingscopy.keys():
            if k != "name" and k!= "path":
                newexp.settings[k] = settingscopy[k]
        return newexp
 
   
    def _prunePhraseTable(self):
        
        if not self.settings.has_key("tm"):
            raise RuntimeError("Translation model is not yet constructed")
        
        phrasetable = self.settings["tm"]+"/model/phrase-table.gz"
        newtable = phrasetable.setInfix("reduced")
        pruneScript = ("zcat %s | " + moses_root + "/scripts/training" 
                       + "/threshold-filter.perl " + " 0.0001 | gzip - > %s"
                       )%(phrasetable, newtable)
        result = self.executor.run(pruneScript)
        if result:
            print "Finished pruning translation table " + phrasetable
            
            with open(self.settings["tm"]+"/model/moses.ini", 'r') as iniFile:
                initContent = iniFile.read()
            with open(self.settings["tm"]+"/model/moses.ini", 'w') as iniFile:
                iniFile.write(initContent.replace(phrasetable, newtable))
                
            if self.settings.has_key("ttm"):
                with open(self.settings["ttm"]+"/moses.ini", 'r') as iniFile:
                    initContent = iniFile.read()
                with open(self.settings["ttm"]+"/moses.ini", 'w') as iniFile:
                    iniFile.write(initContent.replace(phrasetable, newtable))
                
        else:
            print "Pruning of translation table FAILED"
        


    def _getTrainScript(self ,tmDir, trainData, nbThreads, alignment, reordering):
        if not self.settings.has_key("lm") or not self.settings["lm"].has_key("blm"): 
            raise RuntimeError("Language model for " + self.settings["target_long"] 
                               + " is not yet trained")

        tmScript = (moses_root + "/scripts/training/train-model.perl" + " "
                    + "--root-dir " + tmDir + " -corpus " +  trainData
                    + " -f " + self.settings["source"] + " -e " + self.settings["target"] 
                    + " -alignment " + alignment + " " 
                    + " -reordering " + reordering + " "
                    + " -lm 0:" +str(self.settings["lm"]["ngram_order"])
                    +":"+self.settings["lm"]["blm"]+":8"       # 8 because binarised with KenLM
                    + " -external-bin-dir " + mgizapp_root + "/bin" 
                    + " -cores %i -mgiza -mgiza-cpus %i -parallel"
                    )%(nbThreads, nbThreads)
        return tmScript
                       
        
        
    def _getTuningScript(self, tuneDir, tuningStem, nbThreads):

        tuneScript = (moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + tuningStem + "." + self.settings["source"] + " " 
                      + tuningStem + "." + self.settings["target"] + " "
                      + moses_root + "/bin/moses "
                      + self.settings["tm"] + "/model/moses.ini " 
                      + " --mertdir " + moses_root + "/bin/"
                      + " --batch-mira "
                      + " --decoder-flags=\'-threads %i -v 0' --working-dir " + tuneDir
                      )%(nbThreads)
        return tuneScript
        

    
    def _getTranslateScript(self, initFile, nbThreads, inputFile=None):
        script = (moses_root + "/bin/moses -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
        return script
                                     
                                

    def _processAlignedData(self, dataStem, maxLength=80):

        sourceFile = dataStem+"."+self.settings["source"]
        targetFile = dataStem+"."+self.settings["target"]
        if not sourceFile.exists():
            raise RuntimeError("File " + sourceFile + " cannot be found, aborting")
        elif not targetFile.exists():
            raise RuntimeError("File " + targetFile + " cannot be found, aborting")
    
        dataset = {"stem": dataStem,
                   "source":self._processRawData(sourceFile), 
                   "target":self._processRawData(targetFile)} 
        
        trueStem = dataset["source"]["true"].getStem()
        cleanStem = dataset["source"]["true"].setInfix("clean").getStem()
        self._cutoffFiles(trueStem, cleanStem, self.settings["source"], self.settings["target"], maxLength)
        dataset["clean"] = cleanStem
        return dataset
    
    

    def _processRawData(self, rawFile):
         
        lang = rawFile.getSuffix()
        dataset = {}
        dataset["raw"] = rawFile
        
        # STEP 1: tokenisation
        normFile = rawFile.changePath(self.settings["path"]).setInfix("norm")
        
        self.tokeniser.normaliseFile(rawFile, normFile)
        tokFile = normFile.setInfix("tok")
        self.tokeniser.tokeniseFile(normFile, tokFile)
        
        # STEP 2: train truecaser if not already existing
        
        if not self.truecaser[lang].isModelTrained():
            self.truecaser[lang] = self.truecaser[lang].trainModel(tokFile)
            
        # STEP 3: truecasing   
        trueFile = tokFile.setInfix("true")
        modelFile = self.settings["truecasing"][lang]       
        dataset["true"] = self.truecaser[lang].truecaseFile(tokFile, trueFile) 
        normFile.remove()
        tokFile.remove()
        return dataset  



    def _revertProcessedData(self, processedFile, isStem=False):
        
        if isStem:
            dataSource = processedFile +"." + self.settings["source"]
            dataTarget = processedFile +"." + self.settings["target"]
            if dataSource.exists() and dataTarget.exists():
                dataSource = self._revertProcessedData(dataSource)
                dataTarget = self._revertProcessedData(dataTarget)
                return dataSource.getStem()
            else:
                raise RuntimeError("cannot revert data with stem "+ processedFile)
        elif processedFile.exists():
            raise RuntimeError(processedFile + " does not exist")
        
        detokinfix = "detok"
        if processedFile.getInfix():
            detokinfix += "." + processedFile.getInfix
        untokFile = processedFile.changePath(self.settings["path"]).setInfix(detokinfix)
         
        self.tokeniser.detokeniseFile(processedFile,untokFile)
         
        finalinfix = "final"
        if processedFile.getInfix():
            finalinfix += "." + processedFile.getInfix
        finalFile = untokFile.setInfix(finalinfix)
        self.tokeniser.deescapeSpecialCharacters(untokFile, finalFile)

        return finalFile
    
    
    def _getFilteredModel(self, testSource):
        
        if self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.settings["path"]+ "/filteredmodel-" +  testSource.getPath()
        filteredDir.remove()

        filterScript = (moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + initFile + " "
                        + testSource + " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        return filteredDir
            
    
    def _recordState(self):
        dump = json.dumps(self.settings)
        with open(self.settings["path"]+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)
           
        
       
    def _cutoffFiles(self, inputStem, outputStem, source, target, maxLength):
                   
        cleanScript = (moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                       inputStem + " " + source + " " + target + " " 
                       + outputStem + " 1 " + str(maxLength))
        result = self.executor.run(cleanScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")
        outputSource = outputStem+"."+source
        outputTarget = outputStem+"."+target
        print "New cleaned files: " + outputSource.getDescription() + " and " + outputTarget.getDescription()
        return outputSource, outputTarget
     
     
     
def analyseShortAnswers(alignments):

    print "Analysis of short words"
    print "----------------------"
    for align in alignments:
        WER = nlputils.getWER(align["target"], align["translation"])
        if len(align["target"].split()) <= 3 and WER >= 0.5:
            if align.has_key("previous"):
                print "Previous line (reference):\t" + align["previous"]
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"
   


def analyseQuestions(alignments):
        
    print "Analysis of questions"
    print "----------------------"
    for align in alignments:
        WER = nlputils.getWER(align["target"], align["translation"])
        if "?" in align["target"] and WER >= 0.25:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"


def analyseBigErrors(alignments):
    
    
    print "Analysis of large translation errors"
    print "----------------------"
    for align in alignments:
        WER = nlputils.getWER(align["target"], align["translation"])
        if WER >= 0.7:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"

        