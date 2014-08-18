# -*- coding: utf-8 -*- 

import os, json, copy,  re
import paths, process, analyser
from paths import Path
from nlp import CorpusProcessor
from corpus import AlignedCorpus, TranslatedCorpus

rootDir = Path(__file__).getUp().getUp()
expDir = rootDir + "/experiments/"
moses_root = rootDir + "/moses" 
mgizapp_root = rootDir + "/mgizapp"
irstlm_root = rootDir + "/irstlm"
os.environ["IRSTLM"] = irstlm_root
defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"


class Experiment(object):
    
    executor = process.CommandExecutor()
    
    def __init__(self, expName, sourceLang=None, targetLang=None):
        self.settings = {}
        self.settings["name"] = expName
        
        self.settings["path"] = Path(expDir+self.settings["name"])
        
        jsonFile = self.settings["path"]+"/settings.json"
        if jsonFile.exists():
            print "Existing experiment, reloading known settings..."
            self.settings = json.loads(open(jsonFile).read())
            self.settings = paths.convertToPaths(self.settings)
        else:
            self.settings["path"].make()
            if sourceLang:
                self.settings["source"] = sourceLang
                self.settings["source_long"] = paths.getLanguage(sourceLang)
            if targetLang:
                self.settings["target"] = targetLang
                self.settings["target_long"] = paths.getLanguage(targetLang)
                
        self._recordState()
        print ("Experiment " + expName + " (" + self.settings["source"]  
               + "-" + self.settings["target"] + ") successfully started")
        
        self.processor = CorpusProcessor(self.settings["path"], self.executor)
      
    
    def doWholeShibang(self, alignedStem, lmFile=None):
        
        corpus = AlignedCorpus(alignedStem, self.settings["source"], self.settings["target"])
        trainCorpus, tuneCorpus, testCorpus = corpus.divideData(self.settings["path"])
        
        if not lmFile:
            lmFile = alignedStem + "." + self.settings["target"]
        newLmFile = self.settings["path"] + "/" + Path(lmFile).basename().addProperty("filtered") 
        testCorpus.filterLmData(lmFile, newLmFile)
        self.trainLanguageModel(newLmFile)
        
        self.trainTranslationModel(trainCorpus.getStem())
        self.tuneTranslationModel(tuneCorpus.getStem())
        self.evaluateBLEU(testCorpus.getStem())
                    
    
    def trainLanguageModel(self, trainFile, preprocess= True, ngram_order=3):
  
        trainFile = Path(trainFile)
        lang = trainFile.getLang()

        if preprocess:
            trainFile = self.processor.processFile(trainFile)

        print "Building language model based on " + trainFile
        
        sbFile = self.settings["path"] + "/" + trainFile.basename().changeProperty("sb")
                
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
        
        train = AlignedCorpus(trainStem, self.settings["source"], self.settings["target"])
        
        if preprocess:         
            train = self.processor.processCorpus(train)
       
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + train.getStem())

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self._getTrainScript(tmDir, trainStem, nbThreads, alignment, reordering)
        tmDir.reset()
        result = self.executor.run(tmScript)
        if result:
            print "Finished building translation model in directory " + tmDir.getDescription()
            self.settings["tm"]=tmDir
        #    self._prunePhraseTable()
            self._recordState()
        else:
            print "Construction of translation model FAILED"
  

    def tuneTranslationModel(self, tuningStem, preprocess=True, nbThreads=2):
        
        tuning = AlignedCorpus(tuningStem, self.settings["source"], self.settings["target"])
        
        if preprocess:         
            tuning = self.processor.processCorpus(tuning)
        
        print ("Tuning translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + tuning.getStem())
        
        tuneDir = self.settings["path"]+"/tunedmodel"
        tuningScript = self._getTuningScript(tuneDir, tuning.getStem(), nbThreads)
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
            text = self.processor.processText(text, self.settings["source"])
            
        transScript = self._getTranslateScript(initFile, nbThreads)

        return self.executor.run_output(transScript, stdin=text)
        
   
    def translateFile(self, infile, outfile, preprocess=True, filterModel=True, nbThreads=2):
    
        if preprocess:
            infile = self.processor.processFile(infile)
       
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
        
        testSource = testCorpus.getSourceFile()
        testTarget = testCorpus.getTargetFile()          
        translationfile = self.settings["path"] + "/" + testTarget.basename().addProperty("translated")
        
        trans_result = self.translateFile(testSource, translationfile, filterModel=True, preprocess=preprocess)
        
        if trans_result:
            self.settings["test"] = {"stem":testData, "translation":translationfile}

            if preprocess:
                testTarget = self.processor.processFile(testTarget)

            bleuScript = moses_root + "/scripts/generic/multi-bleu.perl -lc " + testTarget
            bleu_output = process.run_output(bleuScript, stdin=translationfile)
            print bleu_output.strip()
            s = re.search(r"=\s(([0-9,\.])+)\,", bleu_output)
            if s:
                self.settings["test"]["bleu"] = s.group(1)
            self._recordState()
        

    def analyseErrors(self, fullCorpus=None):
        
        if not self.settings.has_key("test"):
            raise RuntimeError("you must first perform an evaluation before the analysis")
        
        lastTest = self.settings["test"]
        translatedCorpus = TranslatedCorpus(lastTest["stem"], self.settings["source"], 
                                            self.settings["target"], lastTest["translation"])
        if fullCorpus:
            fullCorpus = AlignedCorpus(fullCorpus, self.settings["source"], self.settings["target"])     
            translatedCorpus.linkWithOriginalCorpus(fullCorpus)
            
        translatedCorpus = self.processor.revertCorpus(translatedCorpus)
      
        alignments = translatedCorpus.getAlignments(addHistory=True)   
        analyser.printSummary(alignments)
        
        translatedCorpus.getSourceFile().remove()
        translatedCorpus.getTargetFile().remove()
        translatedCorpus.getTranslationFile().remove()

   
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
 
   
    def _prunePhraseTable(self, probThreshold=0.0001):
        
        if not self.settings.has_key("tm"):
            raise RuntimeError("Translation model is not yet constructed")
        
        phrasetable = self.settings["tm"]+"/model/phrase-table.gz"
        newtable = self.settings["tm"]+"/model/phrase-table.reduced.gz"
        
        if not phrasetable.exists():
            print "Original phrase table has been removed, pruning canceled"
            return
        
        pruneScript = ("zcat %s | " + moses_root + "/scripts/training" 
                       + "/threshold-filter.perl " + str(probThreshold) + " | gzip - > %s"
                       )%(phrasetable, newtable)
        result = self.executor.run(pruneScript)
        if result:
            print "Finished pruning translation table " + phrasetable
            
            with open(self.settings["tm"]+"/model/moses.ini", 'r') as iniFile:
                initContent = iniFile.read()
            with open(self.settings["tm"]+"/model/moses.ini", 'w') as iniFile:
                iniFile.write(initContent.replace(phrasetable, newtable))
                
            if self.settings.has_key("ttm") and (self.settings["ttm"] + "/moses.ini").exists():
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
                      + " --decoder-flags=\'-threads %i -v 0' --working-dir " + tuneDir
                      )%(nbThreads)
        return tuneScript
        

    
    def _getTranslateScript(self, initFile, nbThreads, inputFile=None):
        script = (moses_root + "/bin/moses -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
        return script
                                     
                                
    
    def _getFilteredModel(self, testSource):
        
        if self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.settings["path"]+ "/filteredmodel-" +  testSource.basename().getStem()
        filteredDir.remove()

        filterScript = (moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + initFile + " "
                        + testSource)
                        #+ " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        return filteredDir
            
    
    def _recordState(self):
        dump = json.dumps(self.settings)
        with open(self.settings["path"]+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)
           
    
     