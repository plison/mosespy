# -*- coding: utf-8 -*- 

"""Creation, update and analysis of machine translation experiments
based on the Moses platform (http://www.statmt.org/moses for details). 
The central entity of this module is the Experiment class which allows
the user to easily configure and run translation experiments.

The module relies on the Moses platform, the MGIZA word alignment tool 
and the IRSTLM language modelling tool, which need to be installed
and compile in the base directory. 
"""

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

# TODO: run tests on abel machines as well
# TODO: make a unified treatment of threads (experiment, processor, etc.)
# TODO: corpus and processing in same module corpus
# TODO: Replace settings dictionary with objects (and change copy and record accordingly)
# TODO: make sure all the path objects are named ...Path
# TODO: get Moses, IRSTLM and MGIZA included as external SVN resources
# TODO: write routines to automatically compile the code above
# TODO: document the rest of the modules
# TODO: more PyUnit tests (analyse errors, corpus, processing, etc.)
# TODO: refactor code, make it more readable

import json, copy,  re
import mosespy.system as system
import mosespy.analyser as analyser
from mosespy.system import Path
from mosespy.corpus import BasicCorpus, AlignedCorpus, TranslatedCorpus, CorpusProcessor

rootDir = Path(__file__).getUp().getUp()
expDir = rootDir + "/experiments/"
moses_root = rootDir + "/moses" 
mgizapp_root = rootDir + "/mgizapp"
irstlm_root = rootDir + "/irstlm"
defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"


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
        
        self.settings = {}
        self.settings["name"] = expName
        
        self.settings["path"] = Path(expDir+self.settings["name"]).getAbsolute()
        
        jsonFile = self.settings["path"]+"/settings.json"
        if jsonFile.exists():
            print "Existing experiment, reloading known settings..."
            self.settings = json.loads(open(jsonFile).read())
            self.settings = system.convertToPaths(self.settings)
        else:
            self.settings["path"].make()
            if sourceLang:
                self.settings["source"] = sourceLang
                self.settings["source_long"] = system.getLanguage(sourceLang)
            if targetLang:
                self.settings["target"] = targetLang
                self.settings["target_long"] = system.getLanguage(targetLang)
                
        self._recordState()
        print ("Experiment " + expName + " (" + self.settings["source"]  
               + "-" + self.settings["target"] + ") successfully started")
        
        self.executor = system.CommandExecutor()
        self.nbThreads = nbThreads
        self.processor = CorpusProcessor(self.settings["path"], self.executor, self.nbThreads)
              
    
    def trainLanguageModel(self, trainFile, preprocess= True, ngram_order=3):
  
        system.setEnv("IRSTLM", irstlm_root)
        trainFile = Path(trainFile).getAbsolute()
        if not trainFile.exists():
            raise RuntimeError("File " + trainFile + " does not exist")
        
        if preprocess:
            trainFile = self.processor.processFile(trainFile)

        print "Building language model based on " + trainFile
        
        sbFile = self.settings["path"] + "/" + trainFile.basename().changeProperty("sb")
                
        self.executor.run(irstlm_root + "/bin/add-start-end.sh", trainFile, sbFile)
        
        lmFile = self.settings["path"] + "/langmodel.lm." + trainFile.getLang()
        lmScript = ((irstlm_root + "/bin/build-lm.sh" + " -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp-%s"
                    )%(sbFile, lmFile, ngram_order, self.settings["name"])) 
        self.executor.run(lmScript)
                           
        arpaFile = self.settings["path"] + "/langmodel.arpa." + trainFile.getLang()
        arpaScript = (irstlm_root + "/bin/compile-lm" + " --text=yes %s %s"%(lmFile+".gz", arpaFile))
        self.executor.run(arpaScript)  

        blmFile = self.settings["path"] + "/langmodel.blm." + trainFile.getLang()
        blmScript = moses_root + "/bin/build_binary -w after -i " + " " + arpaFile + " " + blmFile
        self.executor.run(blmScript)
        print "New binarised language model: " + blmFile.getDescription() 
        
        sbFile.remove()
        (lmFile + ".gz").remove()
        arpaFile.remove()

        if blmFile.getSize() == 0:
            raise RuntimeError("Error: generated language model is empty")

        self.settings["lm"] = {"ngram_order":ngram_order, "blm": blmFile}
        self._recordState()
    
     
    def trainTranslationModel(self, trainStem, alignment=defaultAlignment, 
                              reordering=defaultReordering, preprocess=True, 
                              pruning=True):
        
        train = AlignedCorpus(trainStem, self.settings["source"], self.settings["target"])
        
        if preprocess:         
            train = self.processor.processCorpus(train)
       
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + train.getStem())

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self._getTrainScript(tmDir, train.getStem(), alignment, reordering)
        tmDir.reset()
        result = self.executor.run(tmScript)
        if result:
            print "Finished building translation model in directory " + tmDir.getDescription()
            self.settings["tm"]=tmDir
            if pruning:
                self.prunePhraseTable()
            self._recordState()
        else:
            print "Construction of translation model FAILED"
  

    def tuneTranslationModel(self, tuningStem, preprocess=True):
        
        tuning = AlignedCorpus(tuningStem, self.settings["source"], self.settings["target"])
        
        if preprocess:         
            tuning = self.processor.processCorpus(tuning, False)
        
        print ("Tuning translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + tuning.getStem())
        
        tuneDir = self.settings["path"]+"/tunedmodel"
        tuningScript = self._getTuningScript(tuneDir, tuning.getStem())
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
         
        config = MosesConfig(self.settings["ttm"]+"/moses.ini")
        config.replacePhraseTable(binaDir+"/phrase-table", "PhraseDictionaryBinary")
        config.replaceReorderingTable(binaDir+"/reordering-table")
        
        self.settings["btm"] = binaDir
        self._recordState()
        print "Finished binarising the translation model in directory " + binaDir.getDescription()

      
   
    def translate(self, text, preprocess=True):
        if self.settings.has_key("btm"):
            initFile = self.settings["btm"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        elif self.settings.has_key("tm"):
            initFile = self.settings["tm"] + "/model/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained and tuned!")
        print ("Translating text: \"" + text + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        if preprocess:
            text = self.processor.processText(text, self.settings["source"])
            
        transScript = self._getTranslateScript(initFile)

        return self.executor.run_output(transScript, stdin=text)
        
   
    def translateFile(self, infile, outfile, preprocess=True, filterModel=True):

        infile = Path(infile)
        if preprocess:
            infile = self.processor.processFile(infile)
       
        if filterModel:
            filterDir = self._getFilteredModel(infile)
            initFile = filterDir + "/moses.ini"
        elif self.settings.has_key("btm"):
            initFile = self.settings["btm"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        elif self.settings.has_key("tm"):
            initFile = self.settings["tm"] + "/model/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print ("Translating file \"" + infile + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        transScript = self._getTranslateScript(initFile, infile)
        
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
            testCorpus = self.processor.processCorpus(testCorpus, False)
                    
        transFile = testCorpus.getTargetFile().basename().addProperty("translated")  
        transPath = self.settings["path"] + "/" + transFile
        
        result = self.translateFile(testCorpus.getSourceFile(), transPath, False, True)    
        if result:
            transCorpus = TranslatedCorpus(testCorpus, transPath)
            bleu, bleu_output = self.processor.getBleuScore(transCorpus)
            print bleu_output
            self.settings["test"] = {"stem":transCorpus.getStem(),
                                     "translation":transPath,
                                     "bleu":bleu}                            
            self._recordState()
            return bleu
 
 
    def analyseErrors(self):
        
        if not self.settings.has_key("test"):
            raise RuntimeError("you must first perform an evaluation before the analysis")
        
        lastTest = self.settings["test"]
        testCorpus = AlignedCorpus(lastTest["stem"], self.settings["source"], self.settings["target"])
        translatedCorpus = TranslatedCorpus(testCorpus, lastTest["translation"])
                     
        translatedCorpus = self.processor.revertCorpus(translatedCorpus)
      
        alignments = translatedCorpus.getAlignments(addHistory=True)   
        analyser.printSummary(alignments)
        
        translatedCorpus.getSourceFile().remove()
        translatedCorpus.getTargetFile().remove()
        translatedCorpus.getTranslationFile().remove()


    def queryLanguageModel(self, text):
        if not self.settings.has_key("lm") or not self.settings["lm"].has_key("blm"):
            raise RuntimeError("Language model is not yet trained")
        blmFile = self.settings["lm"]["blm"]
        queryScript = (moses_root + "/bin/query "+ blmFile)
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
        if self.settings.has_key("tm"):
            (self.settings["tm"]+"/corpus").remove()
            (self.settings["tm"]+"/giza." + self.settings["source"] + "-" + self.settings["target"]).remove()
            (self.settings["tm"]+"/giza." + self.settings["target"] + "-" + self.settings["source"]).remove()
            config = MosesConfig(self.settings["tm"]+"/model/moses.ini")
            paths = config.getPaths()
            for f in (self.settings["tm"]+"/model").listdir():
                absolutePath = Path(self.settings["tm"]+"/model/" + f)
                if absolutePath not in paths and f !="moses.ini":
                    print "Removing " + absolutePath
                    absolutePath.remove()
        
        if self.settings.has_key("ttm"):
            for f in self.settings["ttm"].listdir():
                fi = self.settings["ttm"] + "/" + f
                if f !="moses.ini":
                    fi.remove()
        for f in self.settings["path"].listdir():
            if "model" not in f and "settings" not in f:
                (self.settings["path"]+"/" + f).remove()

        print "Finished reducing the size of experiment directory " + self.settings["path"]
 
    
    def copy(self, nexExpName):
        newexp = Experiment(nexExpName, self.settings["source"], self.settings["target"])
        settingscopy = copy.deepcopy(self.settings)
        for k in settingscopy.keys():
            if k != "name" and k!= "path":
                newexp.settings[k] = settingscopy[k]
        newexp.processor = self.processor
        return newexp
 
   
    def prunePhraseTable(self, probThreshold=0.0001):
        
        if not self.settings.has_key("tm"):
            raise RuntimeError("Translation model is not yet constructed")
        
        config = MosesConfig(self.settings["tm"]+"/model/moses.ini")
        phrasetable = config.getPhraseTable()
        newtable = Path(config.getPhraseTable()[:-2] + "reduced.gz")

        if not phrasetable.exists():
            print "Original phrase table has been removed, pruning canceled"
            return
        
        zcatExec = "gzcat" if system.existsExecutable("gzcat") else "zcat"
        pruneScript = (zcatExec + " %s | " + moses_root + "/scripts/training" 
                       + "/threshold-filter.perl " + str(probThreshold) + " | gzip - > %s"
                       )%(phrasetable, newtable)
        result = self.executor.run(pruneScript)
        if result:        
            config.replacePhraseTable(newtable)                          
            if self.settings.has_key("ttm") and (self.settings["ttm"] + "/moses.ini").exists():
                config = MosesConfig(self.settings["ttm"]+"/moses.ini")
                config.replacePhraseTable(newtable)        
            phrasetable.remove()              
        else:
            print "Pruning of translation table FAILED"
        


    def _getTrainScript(self ,tmDir, trainData, alignment, reordering):
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
                    )%(self.nbThreads, self.nbThreads)
        return tmScript
                       
        
        
    def _getTuningScript(self, tuneDir, tuningStem):

        tuneScript = (moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + tuningStem + "." + self.settings["source"] + " " 
                      + tuningStem + "." + self.settings["target"] + " "
                      + moses_root + "/bin/moses "
                      + self.settings["tm"] + "/model/moses.ini " 
                      + " --mertdir " + moses_root + "/bin/"
                      + " --decoder-flags=\'-threads %i -v 0' --working-dir " + tuneDir
                      )%(self.nbThreads)
        return tuneScript
        

    
    def _getTranslateScript(self, initFile, inputFile=None):
        script = (moses_root + "/bin/moses -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(self.nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
        return script
                                                                   
    
    def _getFilteredModel(self, testSource):
        
        if self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        if self.settings.has_key("tm"):
            initFile = self.settings["tm"] + "/model/moses.ini"
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
           
    
    
    

class MosesConfig():
    
    def __init__(self, configFile):
        self.configFile = Path(configFile)

    def getPhraseTable(self):
        parts = self._getParts() 
        if parts.has_key("feature"):
            for l in parts["feature"]:
                if "PhraseDictionary" in l:
                    s = re.search(re.escape("path=") + r"((\S)+)", l)
                    if s:
                        return Path(s.group(1))
        print "Cannot find path to phrase table"
        
    
    def replacePhraseTable(self, newPath, phraseType="PhraseDictionaryMemory"):
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
        parts = self._getParts() 
        if parts.has_key("feature"):
            for l in parts["feature"]:
                if "LexicalReordering" in l:
                    s = re.search(re.escape("path=") + r"((\S)+)", l)
                    if s:
                        return Path(s.group(1))
        print "Cannot find path to reordering table"
        
    
    def replaceReorderingTable(self, newPath):
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
        
    
    def removePart(self, partname):
        parts = self._getParts()
        if parts.has_key(partname):
            del parts[partname]
        self._updateFile(parts)
        
    
    def getPaths(self):
        paths = set()
        parts = self._getParts() 
        for part in parts:
            for l in parts[part]:
                s = re.search(re.escape("path=") + r"((\S)+)", l)
                if s:
                    paths.add(Path(s.group(1)).getAbsolute())
        return paths
        
    
    def display(self):
        lines = self.configFile.readlines()
        for l in lines:
            print l.strip()
        
    def _updateFile(self, newParts):
        with open(self.configFile, 'w') as configFileD:
            for part in newParts:
                configFileD.write("[" + part + "]\n")
                for l in newParts[part]:
                    configFileD.write(l+"\n")
                configFileD.write("\n")
        
    
    def _getParts(self):
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
    

