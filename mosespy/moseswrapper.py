# -*- coding: utf-8 -*- 

import os, json, copy
import shellutils
from xml.dom import minidom


# TODO:
# - get things to work independently of start directory (and test)
# - test whole pipeline with and without Slurm
# - refactor code
# - add bleu evaluation
# - copy from previous experiments

#Next steps:
# - copy from one experiment to another
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
        self.settings = {}
        self.settings["name"] = expName
        
        self.settings["path"] = expDir+self.settings["name"]
        
        if os.path.exists(self.settings["path"]+"/settings.json"):
            print "Existing experiment, reloading known settings..."
            self.settings = json.loads(open(self.settings["path"]+"/settings.json").read())

        else:
            os.makedirs(self.settings["path"]) 
            if sourceLang:
                self.settings["source"] = sourceLang
                self.settings["source_long"] = getLanguage(sourceLang)
            if targetLang:
                self.settings["target"] = targetLang
                self.settings["target_long"] = getLanguage(targetLang)
                
                                   
        self.recordState()
        print ("Experiment " + expName + " (" + self.settings["source"]  
               + "-" + self.settings["target"] + ") successfully started")
        
    
                      
    
    def trainLanguageModel(self, trainFile, ngram_order=3):
        lang = trainFile.split(".")[len(trainFile.split("."))-1]

        processedTrain = self.processRawData(trainFile)
        self.settings["lm"] = {"ngram_order":ngram_order, "data":processedTrain}
        self.recordState()

        print "Building language model based on " + processedTrain["true"]
        
        sbFile = processedTrain["true"].replace(".true.", ".sb.")
        self.executor.run(irstlm_root + "/bin/add-start-end.sh", processedTrain["true"], sbFile)
        self.settings["lm"]["sb"] = sbFile
        self.recordState()
        
        lmFile = self.settings["path"] + "/langmodel.lm." + lang
        lmScript = ((irstlm_root + "/bin/build-lm.sh" + " -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp-%s"
                    )%(sbFile, lmFile, ngram_order, self.settings["name"])) 
        self.executor.run(lmScript)
        self.settings["lm"]["lm"] = lmFile
        self.recordState()
                           
        arpaFile = self.settings["path"] + "/langmodel.arpa." + lang
        arpaScript = (irstlm_root + "/bin/compile-lm" + " --text=yes %s %s"%(lmFile+".gz", arpaFile))
        self.executor.run(arpaScript)  
        self.settings["lm"]["arpa"] = arpaFile
        self.recordState()

        blmFile = self.settings["path"] + "/langmodel.blm." + lang
        blmScript = moses_root + "/bin/build_binary -w after " + " " + arpaFile + " " + blmFile
        self.executor.run(blmScript)
        print "New binarised language model: " + shellutils.getsize(blmFile)   
        self.settings["lm"]["blm"] = blmFile
        self.recordState()
    
    
    
    def trainTranslationModel(self, trainStem=None, nbThreads=2, alignment=defaultAlignment, 
                              reordering=defaultReordering):
           
        if trainStem:         
            trainData = self.processAlignedData(trainStem)
            self.settings["tm"] = {"data": trainData}
            self.recordState()        
        elif not self.settings.has_key("tm") or not self.settings["tm"].has_key("data"):
            raise RuntimeError("Aligned training data is not yet processed")    
        
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + self.settings["tm"]["data"]["clean"])

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self.getTrainScript(tmDir, nbThreads, alignment, reordering)
        shellutils.resetDir(tmDir)
        result = self.executor.run(tmScript)
        if result:
            print "Finished building translation model in directory " + shellutils.getsize(tmDir)
            self.settings["tm"]["dir"]=tmDir
            self.recordState()
        else:
            print "Construction of translation model FAILED"
            self.executor.run("rm -rf " + tmDir)



    def getTrainScript(self ,tmDir, nbThreads, alignment, reordering):
        if not self.settings.has_key("lm") or not self.settings["lm"].has_key("blm"): 
            raise RuntimeError("Language model for " + self.settings["target_long"] 
                               + " is not yet trained")

        tmScript = (moses_root + "/scripts/training/train-model.perl" + " "
                    + "--root-dir " + tmDir + " -corpus " +  self.settings["tm"]["data"]["clean"]
                    + " -f " + self.settings["source"] + " -e " + self.settings["target"] 
                    + " -alignment " + alignment + " " 
                    + " -reordering " + reordering + " "
                    + " -lm 0:" +str(self.settings["lm"]["ngram_order"])
                    +":"+self.settings["lm"]["blm"]+":8"                        # 8 because binarised with KenLM
                    + " -external-bin-dir " + mgizapp_root + "/bin" 
                    + " -cores %i -mgiza -mgiza-cpus %i -parallel"
                    )%(nbThreads, nbThreads)
        return tmScript
                       

    def tuneTranslationModel(self, tuningStem=None, memoryGb=32, nbThreads=16):
        
        if tuningStem:         
            tuningData = self.processAlignedData(tuningStem)
            self.settings["ttm"] = {"data":tuningData}
            self.recordState()
        elif not self.settings.has_key("ttm") or not self.settings["ttm"].has_key("data"):
            raise RuntimeError("Aligned tuning data is not yet processed")    
        
        print ("Tuning translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + tuningData["clean"])
        
        tuneDir = self.settings["path"]+"/tunedmodel"
        tuningScript = self.getTuningScript(tuneDir, nbThreads)
        shellutils.resetDir(tuneDir)
        self.executor.run(tuningScript)
        print "Finished tuning translation model in directory " + shellutils.getsize(tuneDir)
        self.settings["ttm"]["dir"]=tuneDir
        self.recordState()
        
        
    def getTuningScript(self, tuneDir, nbThreads):
        if not self.settings.has_key("tm") or not self.settings["tm"].has_key("dir"): 
            raise RuntimeError("Translation model is not yet trained")

        tuneScript = (moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + self.settings["ttm"]["data"]["clean"] + "." + self.settings["source"] + " " 
                      + self.settings["ttm"]["data"]["clean"] + "." + self.settings["target"] + " "
                      + moses_root + "/bin/moses" + " "
                      + self.settings["tm"]["dir"] + "/model/moses.ini " 
                      + " --mertdir " + moses_root + "/bin/"
                      + " --batch-mira "
                      + " --decoder-flags=\'-threads %i\' --working-dir " + tuneDir
                      )%(nbThreads)
        return tuneScript
        
                               

    def processAlignedData(self, dataStem, maxLength=80):

        sourceFile = dataStem+"."+self.settings["source"]
        targetFile = dataStem+"."+self.settings["target"]
        if not os.path.exists(sourceFile):
            raise RuntimeError("File " + sourceFile + " cannot be found, aborting")
        elif not os.path.exists(targetFile):
            raise RuntimeError("File " + targetFile + " cannot be found, aborting")
    
        dataset = {"stem": dataStem,
                   "source":self.processRawData(sourceFile), 
                   "target":self.processRawData(targetFile)} 
        
        trueStem = dataset["source"]["true"][:-len(self.settings["source"])-1]
        cleanStem = trueStem.replace(".true", ".clean")
        self.cutoffFiles(trueStem, cleanStem, self.settings["source"], 
                         self.settings["target"], maxLength)
        dataset["clean"] = cleanStem
        return dataset
    
    

    def processRawData(self, rawFile):
         
        lang = rawFile.split(".")[len(rawFile.split("."))-1]
        dataset = {}
        dataset["raw"] = rawFile
        
        # STEP 1: tokenisation
        normFile = self.settings["path"] + "/" + os.path.basename(rawFile)[:-len(lang)] + "norm." + lang
        dataset["norm"] = self.normaliseFile(rawFile, normFile)
        tokFile = normFile.replace(".norm.", ".tok.") 
        dataset["tok"] = self.tokeniseFile(normFile, tokFile)
        
        # STEP 2: train truecaser if not already existing
        if not self.settings.has_key("truecasing"):
            self.settings["truecasing"] = {}
        if not self.settings["truecasing"].has_key(lang):
            self.settings["truecasing"][lang] = self.trainTruecasingModel(tokFile, self.settings["path"]
                                                                    + "/truecasingmodel."+lang)
         
        # STEP 3: truecasing   
        trueFile = tokFile.replace(".tok.", ".true.") 
        modelFile = self.settings["truecasing"][lang]       
        dataset["true"] = self.truecaseFile(tokFile, trueFile, modelFile)   
        return dataset  
    


    def binariseModel(self):
        print "Binarise translation model " + self.settings["source"] + " -> " + self.settings["target"]
        if not self.settings.has_key("ttm") or not self.settings["ttm"].has_key("dir"):
            raise RuntimeError("Translation model has not yet been trained and tuned")
        
        binaDir = self.settings["path"]+"/binmodel"
        phraseTable = self.settings["tm"]["dir"]+"/model/phrase-table.gz"
        reorderingTable = self.settings["tm"]["dir"]+"/model/reordering-table.wbe-" + self.settings["reordering"] + ".gz"
        
        shellutils.resetDir(binaDir)
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
        
        with open(self.settings["ttm"]["dir"]+"/moses.ini") as initConfig:
            with open(binaDir+"/moses.ini", 'w') as newConfig:
                for l in initConfig.readlines():
                    l = l.replace("PhraseDictionaryMemory", "PhraseDictionaryBinary")
                    l = l.replace(phraseTable, binaDir + "/phrase-table")
                    l = l.replace(reorderingTable, binaDir + "/reordering-table")
                    newConfig.write(l)
        
        self.settings["btm"] = {"dir":binaDir}
        self.recordState()
        print "Finished binarising the translation model in directory " + shellutils.getsize(binaDir)
      
   
    def translate(self, text, preprocess=True, customModel=None):
        if customModel:
            if not os.path.exists(customModel+"/moses.ini"):
                raise RuntimeError("Custom model " + customModel + " does not exist")
            initFile = customModel+"/moses.ini"
        elif self.settings.has_key("btm"):
            initFile = self.settings["btm"]["dir"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            print "Warning: translation model is not yet binarised"
            initFile = self.settings["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print ("Translating text: \"" + text + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        if preprocess:
            text = self.tokenise(text, self.settings["source"])
            text = self.truecase(text, self.settings["truecasing"][self.settings["source"]])

        transScript = (moses_root + "/bin/moses -f " + initFile.encode('utf-8'))

        # maybe we should try to untokenise the translation before sending it back?
        return self.executor.run_output(transScript, stdin=text)
        
   
    def translateFile(self, infile, outfile, preprocess=True, customModel=None):
        if customModel:
            if not os.path.exists(customModel+"/moses.ini"):
                raise RuntimeError("Custom model " + customModel + " does not exist")
            initFile = customModel+"/moses.ini"
        elif self.settings.has_key("btm"):
            initFile = self.settings["btm"]["dir"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            print "Warning: translation model is not yet binarised"
            initFile = self.settings["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print ("Translating file \"" + infile + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        if preprocess:
            infile = self.processRawData(infile)["true"]

        transScript = (moses_root + "/bin/moses" + " -f " + initFile.encode('utf-8'))
        self.executor.run(transScript, stdin=infile, outfile=outfile)
                                        
    
    def evaluateBLEU(self, testData=None, preprocess=True):
 
        print ("Evaluating BLEU scores with test data: " + testData)
        
        testSource = testData + "." + self.settings["source"]
        testTarget = testData + "." + self.settings["target"]
        if not (os.path.exists(testSource) and os.path.exists(testTarget)):
            raise RuntimeError("Test data cannot be found")

        if preprocess:
            testSource = self.processRawData(testSource)["true"]
            testTarget = self.processRawData(testTarget)["true"]
                 
        if self.settings.has_key("ttm"):
            initFile = self.settings["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.settings["path"]+ "/filteredmodel"
        shellutils.rmDir(filteredDir)
        
        filterScript = (moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + initFile + " "
                        + testSource + " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        
        translationfile = testTarget.replace(".true.", ".translated.")
        self.translateFile(testSource, translationfile, customModel=filteredDir,preprocess=False)
       
        bleuScript = moses_root + "/scripts/generic/multi-bleu.perl -lc " + testTarget
        self.executor.run(bleuScript, stdin=translationfile)

            
    def recordState(self):
        dump = json.dumps(self.settings)
        with open(self.settings["path"]+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)
            
    
    def copy(self, nexExpName):
        newexp = Experiment(nexExpName, self.settings["source"], self.settings["target"])
        settingscopy = copy.deepcopy(self.settings)
        for k in settingscopy.keys():
            if k != "name" and k!= "path":
                newexp.settings[k] = settingscopy[k]
        newexp.recordState()   
        return newexp
    
    def normaliseFile(self, inputFile, outputFile):
        lang = inputFile.split(".")[len(inputFile.split("."))-1]
        if not os.path.exists(inputFile):
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        cleanScript = moses_root + "/scripts/tokenizer/normalize-punctuation.perl " + lang
        result = self.executor.run(cleanScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")

        return outputFile
        
    
      
    def tokeniseFile(self, inputFile, outputFile, nbThreads=2):
        lang = inputFile.split(".")[len(inputFile.split("."))-1]
        if not os.path.exists(inputFile):
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        print "Start tokenisation of file \"" + inputFile + "\""
        tokScript = (moses_root + "/scripts/tokenizer/tokenizer.perl" 
                     + " -l " + lang + " -threads " + str(nbThreads))
        result = self.executor.run(tokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")

        print "New tokenised file: " + shellutils.getsize(outputFile)    
            
    #    specialchars = set()
    #    with open(outputFile, 'r') as tmp:
    #        for l in tmp.readlines():
    #            m = re.search("((\S)*&(\S)*)", l)
    #            if m:
    #                specialchars.add(m.group(1))
    #    print "Special characters: " + str(specialchars)
            
        return outputFile
    
    
    def tokenise(self, inputText, lang):
        tokScript = moses_root + "/scripts/tokenizer/tokenizer.perl" + " -l " + lang
        return shellutils.run_output(tokScript, stdin=inputText).strip()
                
                
    def trainTruecasingModel(self, inputFile, modelFile):
        if not os.path.exists(inputFile):
            raise RuntimeError("tokenised file " + inputFile + " does not exist")
            
        print "Start building truecasing model based on " + inputFile
        truecaseModelScript = (moses_root + "/scripts/recaser/train-truecaser.perl" + " --model " + modelFile + " --corpus " + inputFile)
        result = self.executor.run(truecaseModelScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")

        print "New truecasing model: " + shellutils.getsize(modelFile)
        return modelFile
        
        
    def truecaseFile(self, inputFile, outputFile, modelFile):
       
        if not os.path.exists(inputFile):
            raise RuntimeError("tokenised file " + inputFile + " does not exist")
    
        if not os.path.exists(modelFile):
            raise RuntimeError("model file " + modelFile + " does not exist")
    
        print "Start truecasing of file \"" + inputFile + "\""
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        result = self.executor.run(truecaseScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")

        print "New truecased file: " + shellutils.getsize(outputFile)
        return outputFile
    
    
    def truecase(self, inputText, modelFile):
        if not os.path.exists(modelFile):
            raise RuntimeError("model file " + modelFile + " does not exist")
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        return shellutils.run_output(truecaseScript, stdin=inputText)
        
       
    def cutoffFiles(self, inputStem, outputStem, source, target, maxLength):
                   
        cleanScript = (moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                       inputStem + " " + source + " " + target + " " 
                       + outputStem + " 1 " + str(maxLength))
        result = self.executor.run(cleanScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")
        outputSource = outputStem+"."+source
        outputTarget = outputStem+"."+target
        print "New cleaned files: " + (shellutils.getsize(outputSource) + " and " + 
                                       shellutils.getsize(outputTarget))
        return outputSource, outputTarget
    
    
    
def divideData(fullData, nbTuning=1000, nbTesting=3000):
    if not os.path.exists(fullData):
        raise RuntimeError("Data " + fullData + " does not exist")
    datasize = int(shellutils.run_output("wc -l " + fullData).split()[0])
    if datasize <= nbTuning + nbTesting:
        raise RuntimeError("Data " + fullData + " is too small")
    
    lang = fullData.split(".")[len(fullData.split("."))-1]
    trainFile = fullData[:-len(lang)] + "train." + lang
    tuningFile = fullData[:-len(lang)] + "tune." + lang
    testFile = fullData[:-len(lang)] + "test." + lang
    data = open(fullData, 'r')
    train = open(trainFile, 'w')
    tune = open(tuningFile, 'w')
    test = open(testFile, 'w')
    i = 0
    for l in data.readlines():
        if l.strip():
            if i < (datasize  - nbTuning - nbTesting):
                train.write(l)
            elif i < (datasize - nbTesting):
                tune.write(l)
            else:
                test.write(l)
            i += 1
    data.close()
    train.close()
    tune.close()
    test.close()
    return trainFile, tuningFile, testFile



def getLanguage(langcode):
    isostandard = minidom.parse(os.path.dirname(__file__)+"/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code') 
            and item.attributes[u'iso_639_1_code'].value == langcode):
                return item.attributes['name'].value
    raise RuntimeError("Language code '" + langcode + "' could not be related to a known language")
   
 

