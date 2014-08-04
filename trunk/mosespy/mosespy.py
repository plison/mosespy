# -*- coding: utf-8 -*- 

import os, shutil, json, time, re
import shellutils
from xml.dom import minidom


# TODO:
# - change commands according to Experiment or SlurmExperiment
# - get things to work independently of start directory (and test)
# - test whole pipeline with and without Slurm
# - refactor code
# - add bleu evaluation
# - copy from previous experiments

#Next steps:
# - copy from one experiment to another
# - test for BLEU or other metrics
# - refactor


rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
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
        self.system = {}
        self.system["name"] = expName
        
        self.system["path"] = expDir+self.system["name"]
        if not os.path.exists(self.system["path"]):
            os.makedirs(self.system["path"]) 
        elif os.path.exists(self.system["path"]+"/settings.json"):
            print "Existing experiment, reloading known settings..."
            self.system = json.loads(open(self.system["path"]+"/settings.json").read())
        if sourceLang:
            self.system["source"] = sourceLang
            self.system["source_long"] = getLanguage(sourceLang)
        if targetLang:
            self.system["target"] = targetLang
            self.system["target_long"] = getLanguage(targetLang)
            
        if not self.system.has_key("alignment"):
            self.system["alignment"] = defaultAlignment
        if not self.system.has_key("reordering"):
            self.system["reordering"] = defaultReordering
                        
        print ("Experiment " + expName + " (" + self.system["source"]  
               + "-" + self.system["target"] + ") successfully started")

                      
    
    def trainLanguageModel(self, trainFile, ngram_order=3):
        lang = trainFile.split(".")[len(trainFile.split("."))-1]

        processedTrain = self.processRawData(trainFile)
        self.system["lm"] = {"ngram_order":ngram_order, "data":processedTrain}
        self.recordState()

        print "Building language model based on " + processedTrain["true"]
        
        sbFile = processedTrain["true"].replace(".true.", ".sb.")
        self.executor.run(irstlm_root + "/bin/add-start-end.sh", processedTrain["true"], sbFile)
        self.system["lm"]["sb"] = sbFile
        self.recordState()
        
        lmFile = self.system["path"] + "/langmodel.lm." + lang
        lmScript = ((irstlm_root + "/bin/build-lm.sh" + " -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp-%s"
                    )%(sbFile, lmFile, ngram_order, self.system["name"])) 
        self.executor.run(lmScript)
        self.system["lm"]["lm"] = lmFile
        self.recordState()
                           
        arpaFile = self.system["path"] + "/langmodel.arpa." + lang
        arpaScript = (irstlm_root + "/bin/compile-lm" + " --text=yes %s %s"%(lmFile+".gz", arpaFile))
        self.executor.run(arpaScript)  
        self.system["lm"]["arpa"] = arpaFile
        self.recordState()

        blmFile = self.system["path"] + "/langmodel.blm." + lang
        blmScript = moses_root + "/bin/build_binary" + " " + arpaFile + " " + blmFile + " -w after"
        self.executor.run(blmScript)
        print "New binarised language model: " + shellutils.getsize(blmFile)   
        self.system["lm"]["blm"] = blmFile
        self.recordState()
    
    
    
    def trainTranslationModel(self, trainStem=None, nbThreads=16):
           
        if trainStem:         
            trainData = self.processAlignedData(trainStem)
            self.system["tm"] = {"data": trainData}
            self.recordState()        
        elif not self.system.has_key("tm") or not self.system["tm"].has_key("data"):
            raise RuntimeError("Aligned training data is not yet processed")    
        
        print ("Building translation model " + self.system["source"] + "-" 
               + self.system["target"] + " with " + self.system["tm"]["data"]["clean"])

        tmDir = self.system["path"] + "/translationmodel"
        tmScript = self.getTrainScript(tmDir, nbThreads)
        shutil.rmtree(tmDir, ignore_errors=True)  
        os.makedirs(tmDir) 
        result = self.executor.run(tmScript)
        if result:
            print "Finished building translation model in directory " + shellutils.getsize(tmDir)
            self.system["tm"]["dir"]=tmDir
            self.recordState()
        else:
            print "Construction of translation model FAILED"
            self.executor.run("rm -rf " + tmDir)



    def getTrainScript(self ,tmDir, nbThreads):
        if not self.system.has_key("lm") or not self.system["lm"].has_key("blm"): 
            raise RuntimeError("Language model for " + self.system["target_long"] 
                               + " is not yet trained")

        tmScript = (moses_root + "/scripts/training/train-model.perl" + " "
                    + "--root-dir " + tmDir + " -corpus " +  self.system["tm"]["data"]["clean"]
                    + " -f " + self.system["source"] + " -e " + self.system["target"] 
                    + " -alignment " + self.system["alignment"] + " " 
                    + " -reordering " + self.system["reordering"] + " "
                    + " -lm 0:" +str(self.system["lm"]["ngram_order"])+":"+self.system["lm"]["blm"]+":8" 
                    + " -external-bin-dir " + mgizapp_root + "/bin" 
                    + " -cores %i -mgiza -mgiza-cpus %i -parallel"
                    + " -sort-buffer-size 6G -sort-batch-size 1021 " 
                    + " -sort-compress gzip -sort-parallel %i"
                    )%(nbThreads, nbThreads, nbThreads)
        return tmScript
                       

    def tuneTranslationModel(self, tuningStem=None, memoryGb=32, nbThreads=16):
        
        if tuningStem:         
            tuningData = self.processAlignedData(tuningStem)
            self.system["ttm"] = {"data":tuningData}
            self.recordState()
        elif not self.system.has_key("ttm") or not self.system["ttm"].has_key("data"):
            raise RuntimeError("Aligned tuning data is not yet processed")    
        
        print ("Tuning translation model " + self.system["source"] + "-" 
               + self.system["target"] + " with " + tuningData["clean"])
        
        tuneDir = self.system["path"]+"/tunedmodel"
        tuningScript = self.getTuningScript(tuneDir, nbThreads)
        shutil.rmtree(tuneDir, ignore_errors=True)
        self.executor.run(tuningScript)
        print "Finished tuning translation model in directory " + shellutils.getsize(tuneDir)
        self.system["ttm"]["dir"]=tuneDir
        self.recordState()
        
        
    def getTuningScript(self, tuneDir, nbThreads):
        if not self.system.has_key("tm") or not self.system["tm"].has_key("dir"): 
            raise RuntimeError("Translation model is not yet trained")

        tuneScript = (moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + self.system["ttm"]["data"]["clean"] + "." + self.system["source"] + " " 
                      + self.system["ttm"]["data"]["clean"] + "." + self.system["target"] + " "
                      + moses_root + "/bin/moses" + " "
                      + self.system["tm"]["dir"] + "/model/moses.ini " 
                      + " --mertdir " + moses_root + "/bin/"
                      + " --batch-mira "
                      + " --decoder-flags=\'-threads %i\' --working-dir " + tuneDir
                      )%(nbThreads)
        return tuneScript
        
                               

    def processAlignedData(self, dataStem, maxLength=80):

        sourceFile = dataStem+"."+self.system["source"]
        targetFile = dataStem+"."+self.system["target"]
        if not os.path.exists(sourceFile):
            raise RuntimeError("File " + sourceFile + " cannot be found, aborting")
        elif not os.path.exists(targetFile):
            raise RuntimeError("File " + targetFile + " cannot be found, aborting")
    
        dataset = {"stem": dataStem,
                   "source":self.processRawData(sourceFile), 
                   "target":self.processRawData(targetFile)} 
        
        trueStem = dataset["source"]["true"][:-len(self.system["source"])-1]
        cleanStem = trueStem.replace(".true", ".clean")
        cleanFiles(trueStem, cleanStem, self.system["source"], self.system["target"], maxLength)
        dataset["clean"] = cleanStem
        return dataset
    
    

    def processRawData(self, rawFile):
        if self.system.has_key("lm") and self.system["lm"]["data"]["raw"] == rawFile:
            return self.system["lm"]["data"]
         
        lang = rawFile.split(".")[len(rawFile.split("."))-1]
        dataset = {}
        dataset["raw"] = rawFile
        
        # STEP 1: tokenisation
        tokFile = self.system["path"] + "/" + os.path.basename(rawFile)[:-len(lang)] + "tok." + lang
        dataset["tok"] = tokeniseFile(rawFile, tokFile)
        
        # STEP 2: train truecaser if not already existing
        if not self.system.has_key("truecasing"):
            self.system["truecasing"] = {}
        if not self.system["truecasing"].has_key(lang):
            self.system["truecasing"][lang] = trainTruecasingModel(tokFile, self.system["path"]
                                                                    + "/truecasingmodel."+lang)
         
        # STEP 3: truecasing   
        trueFile = tokFile.replace(".tok.", ".true.") 
        modelFile = self.system["truecasing"][lang]       
        dataset["true"] = truecaseFile(tokFile, trueFile, modelFile)   
        return dataset  
    


    def binariseModel(self):
        print "Binarise translation model " + self.system["source"] + " -> " + self.system["target"]
        if not self.system.has_key("ttm") or not self.system["ttm"].has_key("dir"):
            raise RuntimeError("Translation model has not yet been trained and tuned")
        
        binaDir = self.system["path"]+"/binmodel"
        phraseTable = self.system["tm"]["dir"]+"/model/phrase-table.gz"
        reorderingTable = self.system["tm"]["dir"]+"/model/reordering-table.wbe-" + self.system["reordering"] + ".gz"
        
        shutil.rmtree(binaDir, ignore_errors=True)
        os.makedirs(binaDir)
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
        
        with open(self.system["ttm"]["dir"]+"/moses.ini") as initConfig:
            with open(binaDir+"/moses.ini", 'w') as newConfig:
                for l in initConfig.readlines():
                    l = l.replace("PhraseDictionaryMemory", "PhraseDictionaryBinary")
                    l = l.replace(phraseTable, binaDir + "/phrase-table")
                    l = l.replace(reorderingTable, binaDir + "/reordering-table")
                    newConfig.write(l)
        
        self.system["btm"] = {"dir":binaDir}
        self.recordState()
        print "Finished binarising the translation model in directory " + shellutils.getsize(binaDir)
      
   
    def translate(self, text, preprocess=True, customModel=None):
        if customModel:
            if not os.path.exists(customModel+"/moses.ini"):
                raise RuntimeError("Custom model " + customModel + " does not exist")
            initFile = customModel+"/moses.ini"
        elif self.system.has_key("btm"):
            initFile = self.system["btm"]["dir"] + "/moses.ini"
        elif self.system.has_key("ttm"):
            print "Warning: translation model is not yet binarised"
            initFile = self.system["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print ("Translating text: \"" + text + "\" from " + 
               self.system["source"] + " to " + self.system["target"])

        if preprocess:
            text = tokenise(text, self.system["source"])
            text = truecase(text, self.system["truecasing"][self.system["source"]])

        transScript = ("echo \"" + text + "\" | " + moses_root + "/bin/moses" 
                       + " -f " + initFile.encode('utf-8'))

        return self.executor.run(transScript, return_output=True)
        
   
    def translateFile(self, infile, outfile, preprocess=True, customModel=None):
        if customModel:
            if not os.path.exists(customModel+"/moses.ini"):
                raise RuntimeError("Custom model " + customModel + " does not exist")
            initFile = customModel+"/moses.ini"
        elif self.system.has_key("btm"):
            initFile = self.system["btm"]["dir"] + "/moses.ini"
        elif self.system.has_key("ttm"):
            print "Warning: translation model is not yet binarised"
            initFile = self.system["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print ("Translating file \"" + infile + "\" from " + 
               self.system["source"] + " to " + self.system["target"])

        if preprocess:
            infile = self.processRawData(infile)["true"]

        transScript = (moses_root + "/bin/moses" + " -f " + initFile.encode('utf-8'))
        self.executor.run(transScript, infile=infile, outfile=outfile)
                                        
    
    def evaluateBLEU(self, testData, preprocess=True):
 
        print ("Evaluating BLEU scores with test data: " + testData)
        
        testSource = testData + "." + self.system["source"]
        testTarget = testData + "." + self.system["target"]
        if not (os.path.exists(testSource) and os.path.exists(testTarget)):
            raise RuntimeError("Test data cannot be found")

        if preprocess:
            testSource = self.processRawData(testSource)["true"]
            testTarget = self.processRawData(testTarget)["true"]
                 
        if self.system.has_key("ttm"):
            initFile = self.system["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.system["path"]+ "/filteredmodel"
        shutil.rmtree(filteredDir, ignore_errors=True)
        
        filterScript = (moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + initFile + " "
                        + testSource + " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        
        translationfile = testTarget.replace(".true.", ".translated.")
        self.translateFile(testSource, translationfile, customModel=filteredDir)
       
        bleuScript = moses_root + "/scripts/generic/multi-bleu.perl -lc " + testTarget
        self.executor.run(bleuScript, infile=translationfile)

            
    def recordState(self):
        dump = json.dumps(self.system)
        with open(self.system["path"]+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)



def getLanguage(langcode):
    isostandard = minidom.parse(os.path.dirname(__file__)+"/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code') 
            and item.attributes[u'iso_639_1_code'].value == langcode):
                return item.attributes['name'].value
    raise RuntimeError("Language code '" + langcode + "' could not be related to a known language")

    
def tokeniseFile(inputFile, outputFile):
    lang = inputFile.split(".")[len(inputFile.split("."))-1]
    if not os.path.exists(inputFile):
        raise RuntimeError("raw file " + inputFile + " does not exist")
                    
    print "Start tokenisation of file \"" + inputFile + "\""
    tokScript = moses_root + "/scripts/tokenizer/tokenizer.perl" + " -l " + lang
    shellutils.run(tokScript, inputFile, outputFile)
    print "New tokenised file: " + shellutils.getsize(outputFile)            
    return outputFile


def tokenise(inputText, lang):
    tokScript = moses_root + "/scripts/tokenizer/tokenizer.perl" + " -l " + lang
    return shellutils.run("echo \"" + inputText + "\"|" + tokScript, return_output=True)
            
            
def trainTruecasingModel(inputFile, modelFile):
    if not os.path.exists(inputFile):
        raise RuntimeError("tokenised file " + inputFile + " does not exist")
        
    print "Start building truecasing model based on " + inputFile
    truecaseModelScript = (moses_root + "/scripts/recaser/train-truecaser.perl" + " --model " + modelFile + " --corpus " + inputFile)
    shellutils.run(truecaseModelScript)
    print "New truecasing model: " + shellutils.getsize(modelFile)
    return modelFile
    
    
def truecaseFile(inputFile, outputFile, modelFile):
   
    if not os.path.exists(inputFile):
        raise RuntimeError("tokenised file " + inputFile + " does not exist")

    if not os.path.exists(modelFile):
        raise RuntimeError("model file " + modelFile + " does not exist")

    print "Start truecasing of file \"" + inputFile + "\""
    truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
    shellutils.run(truecaseScript, inputFile, outputFile)
    print "New truecased file: " + shellutils.getsize(outputFile)
    return outputFile


def truecase(inputText, modelFile):
    if not os.path.exists(modelFile):
        raise RuntimeError("model file " + modelFile + " does not exist")
    truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
    return shellutils.run("echo \""+inputText + "\" | " 
                          + truecaseScript, return_output=True)
    
   
def cleanFiles(inputStem, outputStem, source, target, maxLength):
               
    cleanScript = (moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                   inputStem + " " + source + " " + target + " " 
                   + outputStem + " 1 " + str(maxLength))
    shellutils.run(cleanScript)
    outputSource = outputStem+"."+source
    outputTarget = outputStem+"."+target
    print "New cleaned files: " + (shellutils.getsize(outputSource) + " and " + 
                                   shellutils.getsize(outputTarget))
    return outputSource, outputTarget


   
 

