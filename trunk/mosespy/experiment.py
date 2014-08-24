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
from mosespy.corpus import AlignedCorpus, TranslatedCorpus, CorpusProcessor

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
                
        self.expPath = Path(expDir+expName).getAbsolute()
        self.lm = None
        self.ngram_order = None
        self.tm = None
        self.iniFile = None
        self.test = None
        
        jsonFile = self.expPath+"/settings.json"
        if jsonFile.exists():
            self._reloadState()
        else:
            self.expPath.make()
            if sourceLang:
                self.sourceLang = sourceLang
            if targetLang:
                self.targetLang = targetLang
                
        self._recordState()
        print ("Experiment " + expName + " (" + self.sourceLang  
               + "-" + self.targetLang + ") successfully started")
        
        self.executor = system.CommandExecutor()
        self.nbThreads = nbThreads
        self.processor = CorpusProcessor(self.expPath, self.executor, self.nbThreads)
        self.decoder = moses_root + "/bin/moses"
           
    
    def trainLanguageModel(self, trainFile, preprocess= True, ngram_order=3):
  
        system.setEnv("IRSTLM", irstlm_root)
        trainFile = Path(trainFile).getAbsolute()
        if not trainFile.exists():
            raise RuntimeError("File " + trainFile + " does not exist")
        
        if preprocess:
            trainFile = self.processor.processFile(trainFile)

        print "Building language model based on " + trainFile
        
        sbFile = self.expPath + "/" + trainFile.basename().changeProperty("sb")
                
        self.executor.run(irstlm_root + "/bin/add-start-end.sh", trainFile, sbFile)
        
        lmFile = self.expPath + "/langmodel.lm." + trainFile.getLang()
        lmScript = ((irstlm_root + "/bin/build-lm.sh" + " -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp-%s"
                    )%(sbFile, lmFile, ngram_order, self.expPath.basename())) 
        self.executor.run(lmScript)
                           
        arpaFile = self.expPath + "/langmodel.arpa." + trainFile.getLang()
        arpaScript = (irstlm_root + "/bin/compile-lm" + " --text=yes %s %s"%(lmFile+".gz", arpaFile))
        self.executor.run(arpaScript)  

        blmFile = self.expPath + "/langmodel.blm." + trainFile.getLang()
        blmScript = moses_root + "/bin/build_binary -w after -i " + " " + arpaFile + " " + blmFile
        self.executor.run(blmScript)
        print "New binarised language model: " + blmFile.getDescription() 
        
        sbFile.remove()
        (lmFile + ".gz").remove()
        arpaFile.remove()

        if blmFile.getSize() == 0:
            raise RuntimeError("Error: generated language model is empty")

        self.lm = blmFile
        self.ngram_order = ngram_order
        self._recordState()
    
     
    def trainTranslationModel(self, trainStem, alignment=defaultAlignment, 
                              reordering=defaultReordering, preprocess=True, 
                              pruning=True):    
        if not self.lm:
            raise RuntimeError("Language model not yet constructed")
        
        train = AlignedCorpus(trainStem, self.sourceLang, self.targetLang)
        
        if preprocess:         
            train = self.processor.processCorpus(train)
       
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
            self.prunePhraseTable()
        self._recordState()
        
    
    def _constructTranslationModel(self, trainCorpus, alignment, reordering):
        tmDir = self.expPath + "/translationmodel"
        tmDir.reset()
        tmScript = self._getTrainScript(tmDir, trainCorpus.getStem(), alignment, reordering)
        result = self.executor.run(tmScript)
        if not result:
            raise RuntimeError("construction of translation model FAILED")
        return tmDir

  

    def tuneTranslationModel(self, tuningStem, preprocess=True):
        
        if not self.tm:
            raise RuntimeError("Translation model not yet constructed")
        
        tuning = AlignedCorpus(tuningStem, self.sourceLang, self.targetLang)
        
        if preprocess:         
            tuning = self.processor.processCorpus(tuning, False)
        
        print ("Tuning translation model " + self.sourceLang + "-" 
               + self.targetLang + " with " + tuning.getStem())
        
        tuneDir = self.expPath+"/tunedmodel"
        tuningScript = self._getTuningScript(tuneDir, tuning.getStem())
        tuneDir.reset()
        result = self.executor.run(tuningScript)
        if not result or not (tuneDir + "/moses.ini").exists():
            raise RuntimeError("Tuning of translation model FAILED")
            
        print "Finished tuning translation model in directory " + tuneDir.getDescription()
        self.iniFile = tuneDir + "/moses.ini"
        self._recordState()
      


    # BROKEN!!
    def binariseModel(self):
        print "Binarise translation model " + self.sourceLang + " -> " + self.targetLang
        if not self.iniFile:
            raise RuntimeError("Translation model has not yet been trained and tuned")
        
        binaDir = self.expPath+"/binmodel"
        phraseTable = self.tm+"/phrase-table.gz"
        reorderingTable = self.tm+"/reordering-table.wbe-" + " --- " + ".gz"
        
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
         
        config = MosesConfig(self.iniFile)
        config.replacePhraseTable(binaDir+"/phrase-table", "PhraseDictionaryBinary")
        config.replaceReorderingTable(binaDir+"/reordering-table")
        
        self.tm = binaDir
        self._recordState()
        print "Finished binarising the translation model in directory " + binaDir.getDescription()

      
   
    def translate(self, text, preprocess=True):
        if not self.iniFile:
            raise RuntimeError("Translation model is not yet trained and tuned!")
        print ("Translating text: \"" + text + "\" from " + 
               self.sourceLang + " to " + self.targetLang)

        if preprocess:
            text = self.processor.processText(text, self.sourceLang)
            
        transScript = self._getTranslateScript(self.iniFile)

        return self.executor.run_output(transScript, stdin=text)
        
   
    def translateFile(self, infile, outfile, preprocess=True, filterModel=True):

        infile = Path(infile)
        if preprocess:
            infile = self.processor.processFile(infile)
       
        if filterModel:
            filterDir = self._getFilteredModel(infile)
            initFile = filterDir + "/moses.ini"
        elif self.iniFile:
            initFile = self.iniFile
        else:
            raise RuntimeError("Translation model is not yet trained!")
        
        print ("Translating file \"" + infile + "\" from " + 
               self.sourceLang + " to " + self.targetLang)

        transScript = self._getTranslateScript(initFile, infile)
        
        result = self.executor.run(transScript, stdout=outfile)

        if filterDir:
            filterDir.remove()
       
        if not result:
            raise RuntimeError("Translation of file " + str(infile) + " FAILED")
        
       
    def evaluateBLEU(self, testData, preprocess=True):
 
        testCorpus = AlignedCorpus(testData, self.sourceLang, self.targetLang)
        print ("Evaluating BLEU scores with test data: " + testData)
        
        if preprocess:
            testCorpus = self.processor.processCorpus(testCorpus, False)
                    
        transFile = testCorpus.getTargetFile().basename().addProperty("translated")  
        transPath = self.expPath + "/" + transFile
        
        result = self.translateFile(testCorpus.getSourceFile(), transPath, False, True)    
        transCorpus = TranslatedCorpus(testCorpus, transPath)
        bleu, bleu_output = self.processor.getBleuScore(transCorpus)
        print bleu_output
        self.test = {"stem":transCorpus.getStem(),
                     "translation":transPath,
                     "bleu":bleu}                            
        self._recordState()
        return bleu
 
 
    def analyseErrors(self):
        
        if not self.test:
            raise RuntimeError("you must first perform an evaluation before the analysis")
        
        testCorpus = AlignedCorpus(self.test["stem"], self.sourceLang, self.targetLang)
        translatedCorpus = TranslatedCorpus(testCorpus, self.test["translation"])
                     
        translatedCorpus = self.processor.revertCorpus(translatedCorpus)
      
        alignments = translatedCorpus.getAlignments(addHistory=True)   
        analyser.printSummary(alignments)
        
        translatedCorpus.getSourceFile().remove()
        translatedCorpus.getTargetFile().remove()
        translatedCorpus.getTranslationFile().remove()


    def queryLanguageModel(self, text):
        if not self.lm:
            raise RuntimeError("Language model is not yet trained")
        queryScript = (moses_root + "/bin/query "+ self.lm)
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
        newexp = Experiment(nexExpName, self.sourceLang, self.targetLang)
        newexp.lm = self.lm
        newexp.tm = self.tm
        newexp.nbThreads = self.nbThreads
        newexp.ngram_order = self.ngram_order
        newexp.iniFile = self.iniFile
        newexp.sourceLang = self.sourceLang
        newexp.targetLang = self.targetLang
        newexp.test = self.test
        newexp.processor = self.processor
        return newexp
 
   
    def prunePhraseTable(self, probThreshold=0.0001):
        
        if not self.tm or not self.iniFile:
            raise RuntimeError("Translation model is not yet constructed")
        
        config = MosesConfig(self.iniFile)
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
            phrasetable.remove()              
        else:
            print "Pruning of translation table FAILED"
        


    def _getTrainScript(self ,tmDir, trainData, alignment, reordering, 
                        firstStep=1, lastStep=7, direction=None):
        if not self.lm: 
            raise RuntimeError("LM for " + self.targetLang  + " not yet trained")
        tmScript = (moses_root + "/scripts/training/train-model.perl" + " "
                    + "--root-dir " + tmDir + " -corpus " +  trainData
                    + " -f " + self.sourceLang + " -e " + self.targetLang 
                    + " -alignment " + alignment + " " 
                    + " -reordering " + reordering + " "
                    + " -lm 0:" +str(self.ngram_order)
                    +":"+self.lm+":8"       # 8 because binarised with KenLM
                    + " -external-bin-dir " + mgizapp_root + "/bin" 
                    + " -cores %i -mgiza -mgiza-cpus %i -parallel "
                    + " -- first-step %i --last-step %i "
                    + " -sort-buffer-size 20%% -sort-compress gzip -sort-parallel %i" 
                    )%(self.nbThreads, self.nbThreads, firstStep, lastStep, self.nbThreads)
        if direction:
            tmScript += " --direction " + str(direction)
        return tmScript
                       
        
        
    def _getTuningScript(self, tuneDir, tuningStem):

        tuneScript = (moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + tuningStem + "." + self.sourceLang + " " 
                      + tuningStem + "." + self.targetLang + " "
                      + self.decoder + " "
                      + self.iniFile
                      + " --mertdir " + moses_root + "/bin/"
                      + " --decoder-flags=\'-threads %i -v 0' --working-dir " + tuneDir
                      )%(self.nbThreads)
        return tuneScript
        

    
    def _getTranslateScript(self, initFile, inputFile=None):
        script = (self.decoder  + " -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(self.nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
        return script
                                                                   
    
    def _getFilteredModel(self, testSource):
        
        if not self.iniFile:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.expPath+ "/filteredmodel-" +  testSource.basename().getStem()
        filteredDir.remove()

        filterScript = (moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + self.iniFile + " "
                        + testSource)
                        #+ " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        return filteredDir
            
    
    def _recordState(self):
        settings = {"path":self.expPath, "source":self.sourceLang, "target":self.targetLang}
        if self.lm:
            settings["lm"] = self.lm
        if self.ngram_order:
            settings["ngram_order"] = self.ngram_order
        if self.tm:
            settings["tm"] = self.tm
        if self.iniFile:
            settings["ini"] = self.iniFile
        if self.test:
            settings["test"] = self.test
        dump = json.dumps(settings)
        with open(self.expPath+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)
            
            
    def _reloadState(self):
        print "Existing experiment, reloading known settings..."
        with open(self.expPath+"/settings.json", 'r') as jsonFile:
            settings = json.loads(jsonFile.read())
            if settings.has_key("source"):
                self.sourceLang = settings["source"]
            if settings.has_key("target"):
                self.targetLang = settings["target"]
            if settings.has_key("lm"):
                self.lm = Path(settings["lm"])
            if settings.has_key("ngram_order"):
                self.ngram_order = int(settings["ngram_order"])
            if settings.has_key("tm"):
                self.tm = Path(settings["tm"])
            if settings.has_key("ini"):
                self.iniFile = Path(settings["ini"])
            if settings.has_key("test"):
                self.test = settings["test"]
            
           
    
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
    

