# -*- coding: utf-8 -*- 

import os, json, copy, random, re
import utils, evaluation


rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
expDir = rootDir + "/experiments/"
moses_root = rootDir + "/moses" 
mgizapp_root = rootDir + "/mgizapp"
irstlm_root = rootDir + "/irstlm"
os.environ["IRSTLM"] = irstlm_root
defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"

class Experiment(object):
    
    executor = utils.CommandExecutor()
    
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
                self.settings["source_long"] = utils.getLanguage(sourceLang)
            if targetLang:
                self.settings["target"] = targetLang
                self.settings["target_long"] = utils.getLanguage(targetLang)
                
        self.recordState()
        print ("Experiment " + expName + " (" + self.settings["source"]  
               + "-" + self.settings["target"] + ") successfully started")
      
    
    
    def doWholeShibang(self, alignedData, lmData=None):
        if not lmData:
            lmData = alignedData + "." + self.settings["target"]
        trainStem, tuneStem, testStem, lmData = self.divideData(alignedData, lmData)
        self.trainLanguageModel(lmData)
        self.trainTranslationModel(trainStem)
        self.tuneTranslationModel(tuneStem)
        self.evaluateBLEU(testStem)
                    
    
    def trainLanguageModel(self, trainFile, ngram_order=3):
        lang = trainFile.split(".")[len(trainFile.split("."))-1]

        processedTrain = self.processRawData(trainFile)
        self.settings["lm"] = {"ngram_order":ngram_order}
        self.recordState()

        print "Building language model based on " + processedTrain["true"]
        
        sbFile = processedTrain["true"].replace(".true.", ".sb.")
        self.executor.run(irstlm_root + "/bin/add-start-end.sh", processedTrain["true"], sbFile)
        
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
        print "New binarised language model: " + utils.getsize(blmFile)   
        self.settings["lm"]["blm"] = blmFile

        self.recordState()
        os.remove(sbFile)
        os.remove(lmFile+ ".gz") 
        os.remove(arpaFile)
    
    
    def trainTranslationModel(self, trainStem, nbThreads=2, alignment=defaultAlignment, 
                              reordering=defaultReordering, preprocess=True):
           
        if preprocess:         
            trainStem = self.processAlignedData(trainStem)["clean"]
       
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + trainStem)

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self.getTrainScript(tmDir, trainStem, nbThreads, alignment, reordering)
        utils.resetDir(tmDir)
        result = self.executor.run(tmScript)
        if result:
            print "Finished building translation model in directory " + utils.getsize(tmDir)
            self.settings["tm"]=tmDir
            self.prunePhraseTable()
            self.recordState()
        else:
            print "Construction of translation model FAILED"
            
    
    def prunePhraseTable(self):
        
        if not self.settings.has_key("tm"):
            raise RuntimeError("Translation model is not yet constructed")
        
        phrasetable = self.settings["tm"]+"/model/phrase-table.gz"
        newtable = phrasetable.replace(".gz", ".reduced.gz")
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
        



    def getTrainScript(self ,tmDir, trainData, nbThreads, alignment, reordering):
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
                       

    def tuneTranslationModel(self, tuningStem, preprocess=True, nbThreads=2):
        
        if preprocess:         
            tuningStem = self.processAlignedData(tuningStem)["clean"]
        
        print ("Tuning translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " + tuningStem)
        
        tuneDir = self.settings["path"]+"/tunedmodel"
        tuningScript = self.getTuningScript(tuneDir, tuningStem, nbThreads)
        utils.resetDir(tuneDir)
        result = self.executor.run(tuningScript)
        if result:
            print "Finished tuning translation model in directory " + utils.getsize(tuneDir)
            self.settings["ttm"]=tuneDir
            self.recordState()
        else:
            print "Tuning of translation model FAILED"
        
    def getTuningScript(self, tuneDir, tuningStem, nbThreads):

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
        self.normaliseFile(rawFile, normFile)
        tokFile = normFile.replace(".norm.", ".tok.") 
        self.tokeniseFile(normFile, tokFile)
        
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
        os.remove(normFile)  
        os.remove(tokFile)
        return dataset  
    


    def binariseModel(self):
        print "Binarise translation model " + self.settings["source"] + " -> " + self.settings["target"]
        if not self.settings.has_key("ttm"):
            raise RuntimeError("Translation model has not yet been trained and tuned")
        
        binaDir = self.settings["path"]+"/binmodel"
        phraseTable = self.settings["tm"]+"/model/phrase-table.gz"
        reorderingTable = self.settings["tm"]+"/model/reordering-table.wbe-" + self.settings["reordering"] + ".gz"
        
        utils.resetDir(binaDir)
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
        
        with open(self.settings["ttm"]+"/moses.ini") as initConfig:
            with open(binaDir+"/moses.ini", 'w') as newConfig:
                for l in initConfig.readlines():
                    l = l.replace("PhraseDictionaryMemory", "PhraseDictionaryBinary")
                    l = l.replace(phraseTable, binaDir + "/phrase-table")
                    l = l.replace(reorderingTable, binaDir + "/reordering-table")
                    newConfig.write(l)
        
        self.settings["btm"] = binaDir
        self.recordState()
        print "Finished binarising the translation model in directory " + utils.getsize(binaDir)

      
   
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
            text = self.tokenise(text, self.settings["source"])
            text = self.truecase(text, self.settings["truecasing"][self.settings["source"]])

        transScript = self.getTranslateScript(initFile, nbThreads)

        return self.executor.run_output(transScript, stdin=text)
        
   
    def translateFile(self, infile, outfile, preprocess=True, filterModel=False, nbThreads=2):
           
        if filterModel:
            filterDir = self.getFilteredModel(infile)
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
            infile = self.processRawData(infile)["true"]

        transScript = self.getTranslateScript(initFile, nbThreads, inputFile=infile)
        
        result = self.executor.run(transScript, stdout=outfile)
        
        if result:
            if not self.settings.has_key("translations"):
                self.settings["translations"] = []
            translation = {"in":infile, "out":outfile}
            self.settings["translations"].append(translation)
        else:
            print "Translation of file " + infile + " FAILED"
        
        if filterDir:
            utils.rmDir(filterDir)
    
    
    def getTranslateScript(self, initFile, nbThreads, inputFile=None):
        script = (moses_root + "/bin/moses -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
        return script
                                     
    
    def evaluateBLEU(self, testData, preprocess=True):
 
        print ("Evaluating BLEU scores with test data: " + testData)
        
        testSource = testData + "." + self.settings["source"]
        testTarget = testData + "." + self.settings["target"]
        if not (os.path.exists(testSource) and os.path.exists(testTarget)):
            raise RuntimeError("Test data cannot be found")

        if preprocess:
            testSource = self.processRawData(testSource)["true"]
            testTarget = self.processRawData(testTarget)["true"]
        
        
        translationfile = testTarget.replace(".true.", ".translated.")
        self.translateFile(testSource, translationfile, filterModel=True,preprocess=False)
        
        bleuScript = moses_root + "/scripts/generic/multi-bleu.perl -lc " + testTarget
        result2 = utils.run_output(bleuScript, stdin=translationfile)
        print result2
        s = re.search("=\s(([0-9,\.])+)\,", result2)
        if s:
            score = s.group(1)
            self.settings["translations"][-1]["bleu"] = score
        

    def analyseErrors(self, testData, preprocess=True):
        print ("Perform error analysis with test data: " + testData)
        
        testSource = testData + "." + self.settings["source"]
        testTarget = testData + "." + self.settings["target"]
        if not (os.path.exists(testSource) and os.path.exists(testTarget)):
            raise RuntimeError("Test data cannot be found")

        if preprocess:
            testSource = self.processRawData(testSource)["true"]
            testTarget = self.processRawData(testTarget)["true"]
        
        filteredDir = self.getFilteredModel(testSource)
        
        translationfile = testTarget.replace(".true.", ".translated.")
        self.translateFile(testSource, translationfile, customModel=filteredDir,preprocess=False)
        
        evaluation.analyseShortAnswers(testTarget, translationfile)
        evaluation.analyseQuestions(testTarget, translationfile)
        evaluation.analyseBigErrors(testTarget, translationfile)



    def getFilteredModel(self, testSource):
        
        if self.settings.has_key("ttm"):
            initFile = self.settings["ttm"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet tuned")

        filteredDir = self.settings["path"]+ "/filteredmodel-" +  os.path.basename(testSource) 
        utils.rmDir(filteredDir)

        filterScript = (moses_root + "/scripts/training/filter-model-given-input.pl "
                        + filteredDir + " " + initFile + " "
                        + testSource + " -Binarizer "  + moses_root+"/bin/processPhraseTable")
        self.executor.run(filterScript)
        return filteredDir
            
    
    def recordState(self):
        dump = json.dumps(self.settings)
        with open(self.settings["path"]+"/settings.json", 'w') as jsonFile:
            jsonFile.write(dump)
           
    
    def reduceSize(self):
        if self.settings.has_key("tm"):
            utils.rmDir(self.settings["tm"]+"/corpus") 
            utils.rmDir(self.settings["tm"]+"/giza." + self.settings["source"] + "-" + self.settings["target"])
            utils.rmDir(self.settings["tm"]+"/giza." + self.settings["target"] + "-" + self.settings["source"])
            with open(self.settings["tm"]+"/model/moses.ini", 'r') as iniFile:
                iniContent = iniFile.read()
            for f in os.listdir(self.settings["tm"]+"/model"):
                if f not in iniContent and f != "moses.ini":
                    os.remove(self.settings["tm"]+"/model/" + f)
        
        if self.settings.has_key("ttm"):
            for f in os.listdir(self.settings["ttm"]):
                fi = self.settings["ttm"] + "/" + f
                if f != "moses.ini" and os.path.isfile(fi):
                    os.remove(fi)
                elif os.path.isdir(fi):
                    utils.rmDir(fi)
        print "Finished reducing the size of experiment directory " + self.settings["path"]
 
    
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

        print "New tokenised file: " + utils.getsize(outputFile)    
            
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
        return utils.run_output(tokScript, stdin=inputText).strip()
                
                
    def trainTruecasingModel(self, inputFile, modelFile):
        if not os.path.exists(inputFile):
            raise RuntimeError("tokenised file " + inputFile + " does not exist")
            
        print "Start building truecasing model based on " + inputFile
        truecaseModelScript = (moses_root + "/scripts/recaser/train-truecaser.perl" 
                               + " --model " + modelFile + " --corpus " + inputFile)
        result = self.executor.run(truecaseModelScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")

        print "New truecasing model: " + utils.getsize(modelFile)
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

        print "New truecased file: " + utils.getsize(outputFile)
        return outputFile
    
    
    def truecase(self, inputText, modelFile):
        if not os.path.exists(modelFile):
            raise RuntimeError("model file " + modelFile + " does not exist")
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        return utils.run_output(truecaseScript, stdin=inputText)
        
       
    def cutoffFiles(self, inputStem, outputStem, source, target, maxLength):
                   
        cleanScript = (moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                       inputStem + " " + source + " " + target + " " 
                       + outputStem + " 1 " + str(maxLength))
        result = self.executor.run(cleanScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")
        outputSource = outputStem+"."+source
        outputTarget = outputStem+"."+target
        print "New cleaned files: " + (utils.getsize(outputSource) + " and " + 
                                       utils.getsize(outputTarget))
        return outputSource, outputTarget
    
        
    def divideData(self, alignedData, lmData, nbTuning=1000, nbTesting=3000):
        
        fullSource = alignedData + "." + self.settings["source"]
        fullTarget = alignedData + "." + self.settings["target"]
        
        if not os.path.exists(fullSource) or not os.path.exists(fullTarget):
            raise RuntimeError("Data " + alignedData + " does not exist")
        
        nbLinesSource = utils.countNbLines(fullSource)
        nbLinesTarget = utils.countNbLines(fullTarget)
        if nbLinesSource != nbLinesTarget:
            raise RuntimeError("Number of lines for source and target are different")
        if nbLinesSource <= nbTuning + nbTesting:
            raise RuntimeError("Data " + alignedData + " is too small")
         
        fullSource = open(fullSource, 'r')
        fullTarget = open(fullTarget, 'r')
        trainStem = self.settings["path"] + "/" + os.path.basename(alignedData) + ".train"
        trainSource = open(trainStem + "." + self.settings["source"], 'w', 1000000)
        trainTarget = open(trainStem + "." + self.settings["target"], 'w', 1000000)
        tuneStem = self.settings["path"] + "/" + os.path.basename(alignedData) + ".tune"
        tuneSource = open(tuneStem + "." + self.settings["source"], 'w')
        tuneTarget = open(tuneStem + "." + self.settings["target"], 'w')
        testStem = self.settings["path"]+ "/" + os.path.basename(alignedData) + ".test"
        testSource = open(testStem + "." + self.settings["source"], 'w')
        testTarget = open(testStem + "." + self.settings["target"], 'w')
        
        tuningLines = set()
        while len(tuningLines) < nbTuning:
            choice = random.randrange(2, nbLinesSource)
            tuningLines.add(choice)
        
        testingLines = set()
        while len(testingLines) < nbTesting:
            choice = random.randrange(2, nbLinesSource)
            if choice not in tuningLines:
                testingLines.add(choice)
         
        print "Dividing source data..."
        sourceLines = fullSource.readlines()
        for i in range(0, len(sourceLines)):
            sourceLine = sourceLines[i]
            if i in tuningLines:
                tuneSource.write(sourceLine)
            elif i in testingLines:
                testSource.write(sourceLine)
            else:
                trainSource.write(sourceLine)
        
        print "Dividing target data..."
        targetLines = fullTarget.readlines()
        for i in range(0, len(targetLines)):
            targetLine = targetLines[i]
            if i in tuningLines:
                tuneTarget.write(targetLine)
            elif i in testingLines:
                testTarget.write(targetLine)
            else:
                trainTarget.write(targetLine)
         
        for f in [fullSource, fullTarget, trainSource, trainTarget,
                  tuneSource, tuneTarget, testSource, testTarget]:
            f.close()
                       
        print "Filtering language model to remove sentences from test set..."
        
        linesdict = {}
        for i in range(0, len(targetLines)):
            l = targetLines[i]
            if (i+2) in testingLines:
                curLine = {"i-2": l}
            if (i+1) in testingLines:
                curLine["i-1"] = l
            if i in testingLines:
                if l not in linesdict:
                    linesdict[l] = [curLine]
                else:
                    linesdict[l].append(curLine)

        inData = open(lmData, 'r')
        extension = lmData.split(".")[len(lmData.split("."))-1]
        newLmFile = (self.settings["path"] + "/" + os.path.basename(lmData[:-len(extension)])
                     + "wotest." + extension)        
        outData = open(newLmFile, 'w', 1000000)
                           
        prev2Line = None
        prevLine = None
        skippedLines = []
        for l in inData.readlines():
            toSkip = False
            if l in linesdict:
                for lineinfo in linesdict[l]:
                    if prev2Line == lineinfo["i-2"] and prevLine == lineinfo["i-1"]:
                        skippedLines.append(l)
                        toSkip = True
            if not toSkip:
                outData.write(l)                                
            prev2Line = prevLine
            prevLine = l
        
        inData.close()
        outData.close()
        print "Number of skipped lines in language model: " + str(len(skippedLines))
        return trainStem, tuneStem, testStem, newLmFile


            