# -*- coding: utf-8 -*- 

import os, shutil, json, time, re
import shellutils, slurmutils
from xml.dom import minidom

workingDir ="./experiments/"
defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"

class Experiment:
            
    def __init__(self, expName, sourceLang=None, targetLang=None):
        self.system = {}
        self.system["name"] = expName
        
        self.system["path"] = workingDir+self.system["name"]
        if not os.path.exists(self.system["path"]):
            os.makedirs(self.system["path"]) 
        elif os.path.exists(self.system["path"]+"/experiment.json"):
            self.system = json.loads(open(self.system["path"]+"/experiment.json").read())
            
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

                      
    
    
    # Try with SRILM instead?
    def trainLanguageModel(self, trainFile, ngram_order=3):
        lang = trainFile.split(".")[len(trainFile.split("."))-1]

        processedTrain = self.processRawData(trainFile)
        self.system["lm"] = {"ngram_order":ngram_order, "data":processedTrain}
        self.recordState()

        print "Building language model based on " + processedTrain["true"]
        
        sbFile = processedTrain["true"].replace(".true.", ".sb.")
        shellutils.run("./irstlm/bin/add-start-end.sh", processedTrain["true"], sbFile)
        self.system["lm"]["sb"] = sbFile
        self.recordState()
        
        lmFile = self.system["path"] + "/langmodel.lm." + lang
        lmScript = (("export IRSTLM=./irstlm; ./irstlm/bin/build-lm.sh -i %s" +
                    " -p -s improved-kneser-ney -o %s -n %i -t ./tmp"
                    )%(sbFile, lmFile, ngram_order))
        shellutils.run(lmScript)
        self.system["lm"]["lm"] = lmFile
        self.recordState()
                           
        arpaFile = self.system["path"] + "/langmodel.arpa." + lang
        arpaScript = ("./irstlm/bin/compile-lm  --text=yes %s %s"%(lmFile+".gz", arpaFile))
        shellutils.run(arpaScript)  
        self.system["lm"]["arpa"] = arpaFile
        self.recordState()

        blmFile = self.system["path"] + "/langmodel.blm." + lang
        blmScript = "./moses/bin/build_binary " + arpaFile + " " + blmFile
        shellutils.run(blmScript)
        print "New Binarised language model: " + getFileDescription(blmFile)   
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

        tmScript, tmDir = self.getTrainScript(nbThreads)
        shutil.rmtree(tmDir, ignore_errors=True)  
        os.makedirs(tmDir) 
        result = shellutils.run(tmScript)
        if result:
            print "Finished building translation model in directory " + getFileDescription(tmDir)
            self.system["tm"]["dir"]=tmDir
            self.recordState()
        else:
            print "Construction of translation model FAILED"
            shellutils.run("rm -rf " + tmDir)



    def getTrainScript(self ,nbThreads):
        if not self.system.has_key("lm") or not self.system["lm"].has_key("blm"): 
            raise RuntimeError("Language model for " + self.system["target_long"] + " is not yet trained")
        
        lmPath = os.popen("pwd").read().strip()+"/" + self.system["lm"]["blm"]
        tmDir = self.system["path"] + "/translationmodel"
        tmScript = ("./moses/scripts/training/train-model.perl "
                    + "--root-dir " + tmDir + " -corpus " +  self.system["tm"]["data"]["clean"]
                    + " -f " + self.system["source"] + " -e " + self.system["target"] 
                    + " -alignment " + self.system["alignment"] + " " 
                    + " -reordering " + self.system["reordering"] + " "
                    + " -lm 0:" +str(self.system["lm"]["ngram_order"])+":"+lmPath+":8" 
                    + " -external-bin-dir ./mgizapp/bin -cores %i -mgiza -mgiza-cpus %i"
                    + " -parallel -sort-buffer-size %iG -sort-batch-size 1021 " 
                    + " -sort-compress gzip -sort-parallel %i"
                    )%(nbThreads, nbThreads, 8, nbThreads)
        return tmScript, tmDir


                       

    def tuneTranslationModel(self, tuningStem=None, memoryGb=32, nbThreads=16):
        
        if tuningStem:         
            tuningData = self.processAlignedData(tuningStem)
            self.system["ttm"] = {"data":tuningData}
            self.recordState()
        elif not self.system.has_key("ttm") or not self.system["ttm"].has_key("data"):
            raise RuntimeError("Aligned tuning data is not yet processed")    
        
        print ("Tuning translation model " + self.system["source"] + "-" 
               + self.system["target"] + " with " + tuningData["clean"])
        
        tuningScript, tuneDir = self.getTuningScript(nbThreads)
        shellutils.run(tuningScript)
        print "Finished tuning translation model in directory " + getFileDescription(tuneDir)
        self.system["ttm"]["dir"]=tuneDir
        self.recordState()
        
        
    def getTuningScript(self, nbThreads):
        if not self.system.has_key("tm") or not self.system["tm"].has_key("dir"): 
            raise RuntimeError("Translation model is not yet trained")

        tuneDir = self.system["path"]+"/tunedmodel4"
        path= os.popen("pwd").read().strip()+"/"
        tuneScript = ("./moses/scripts/training/mert-moses.pl " 
                      + path+self.system["ttm"]["data"]["clean"] + "." + self.system["source"] + " " 
                      + path+self.system["ttm"]["data"]["clean"] + "." + self.system["target"] + " "
                      + path+"./moses/bin/moses " 
                      + path+self.system["tm"]["dir"] + "/model/moses.ini " 
                      + " --mertdir " + path + "./moses/bin/ "
                      + " --batch-mira "
                      + " --decoder-flags=\'-threads %i\' --working-dir " + path+tuneDir
                      )%(nbThreads)
        return tuneScript, tuneDir
        
                               

    def processAlignedData(self, dataStem):

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
        cleanFiles(trueStem, cleanStem, self.system["source"], self.system["target"])
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
    
    
    
    def translate(self, text):
        if not self.system.has_key("tm"):
            raise RuntimeError("No translation model has been built yet!")
        elif not self.system.has_key("ttm"):
            print "WARNING: translation model has not been tuned!"
            iniFile = self.system["tm"]["dir"]+"/model/moses.ini"
        else:
            iniFile = self.system["ttm"]["dir"]+"/moses.ini"
        print text
        transScript = u'echo \"%r\" | ./moses/bin/moses -f %s'%(text,iniFile)
        if len(text) < 500:
            shellutils.run(transScript)
        else:
            shellutils.run(transScript)
        
              
                                


    #def binariseModel(self, tm):
    #    print "Binarise translation model " + self.system["source"] + " -> " + self.system["target"]
    #    binaDir = tm["tmDir"].replace("model.", "binmodel.")
    #    binScript = ("mkdir " + binaDir + "; ./moses/bin/processPhraseTable -ttable 0 0 " + tuneDir + 
    #                  "/model/phrase-table.gz " + " -nscores 5 -out " + binaDir + "/phrase-table")
    #    shellutils.run(binScript)            
    #    binScript2 = ("./moses/bin/processLexicalTable -in " + tuneDir + "/reordering-table.wbe-msd-bidirectional-fe.gz " 
    #                  + " -out " + binaDir + "/reordering-table")
    #    shellutils.run(binScript2)
    #    print "Finished binarising the translation model in directory " + getFileDescription(binaDir)
            
            
    def recordState(self):
        dump = json.dumps(self.system)
        with open(self.system["path"]+"/experiment.json", 'w') as jsonFile:
            jsonFile.write(dump)



def getFileDescription(filename):
    desc = filename + " ("
    size = os.path.getsize(filename)
    if size > 1000000000:
        desc = desc + str(size/1000000000) + " Gb.)"
    elif size > 1000000:
        desc = desc + str(size/1000000) + " Mb.)"
    else:
        desc = desc + str(size/1000) + " Kb.)"
    return desc


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
    tokScript = "./moses/scripts/tokenizer/tokenizer.perl -l " + lang
    shellutils.run(tokScript, inputFile, outputFile)
    print "New tokenised file: " + getFileDescription(outputFile)            
    return outputFile

            
def trainTruecasingModel(inputFile, modelFile):
    if not os.path.exists(inputFile):
        raise RuntimeError("tokenised file " + inputFile + " does not exist")
        
    print "Start building truecasing model based on " + inputFile
    truecaseModelScript = ("./moses/scripts/recaser/train-truecaser.perl " 
                           "--model " + modelFile + " --corpus " + inputFile)
    shellutils.run(truecaseModelScript)
    print "New truecasing model: " + getFileDescription(modelFile)
    return modelFile
    
    
def truecaseFile(inputFile, outputFile, modelFile):
   
    if not os.path.exists(inputFile):
        raise RuntimeError("tokenised file " + inputFile + " does not exist")

    if not os.path.exists(modelFile):
        raise RuntimeError("model file " + modelFile + " does not exist")

    print "Start truecasing of file \"" + inputFile + "\""
    truecaseScript = "./moses/scripts/recaser/truecase.perl --model " + modelFile
    shellutils.run(truecaseScript, inputFile, outputFile)
    print "New truecased file: " + getFileDescription(outputFile)
    return outputFile

   
def cleanFiles(inputStem, outputStem, source, target, maxLength=80):
               
    cleanScript = ("./moses/scripts/training/clean-corpus-n.perl " + 
                   inputStem + " " + source + " " + target + " " + outputStem + " 1 " + str(maxLength))
    shellutils.run(cleanScript)
    outputSource = outputStem+"."+source
    outputTarget = outputStem+"."+target
    print "New cleaned files: " + (getFileDescription(outputSource) + " and " + 
                                   getFileDescription(outputTarget))
    return outputSource, outputTarget


   
 

