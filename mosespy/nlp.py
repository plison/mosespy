
from paths import Path 
from corpus import AlignedCorpus, TranslatedCorpus
rootDir = Path(__file__).getUp().getUp()
moses_root = rootDir + "/moses" 


class CorpusProcessor():
    
    def __init__(self, workPath, executor, nbThreads=2):
        self.workPath = workPath
        self.executor = executor
        self.tokeniser = Tokeniser(executor, nbThreads)
        self.truecaser = TrueCaser(executor, workPath+"/truecasingmodel")
        
    def processCorpus(self, corpus, maxLength=80):
        
        if not isinstance(corpus, AlignedCorpus):
            raise RuntimeError("aligned data must be of type AlignedCorpus")
   
        trueSource = self.processFile(corpus.getSourceFile())
        self.processFile(corpus.getTargetFile())
     
        trueCorpus = AlignedCorpus(trueSource.getStem(), corpus.sourceLang, corpus.targetLang)
        cleanStem = trueSource.getStem().changeProperty("clean")
        cleanCorpus = self.cutoffFiles(trueCorpus, cleanStem, maxLength)
        cleanCorpus.origin = corpus.origin
        return cleanCorpus


    def processFile(self, rawFile):
         
        rawFile = Path(rawFile)
        
        # STEP 1: tokenisation
        normFile = self.workPath + "/" + rawFile.basename().addProperty("norm")
        self.tokeniser.normaliseFile(rawFile, normFile)
        tokFile = normFile.changeProperty("tok")
        self.tokeniser.tokeniseFile(normFile, tokFile)
        
        # STEP 2: train truecaser if not already existing
        if not self.truecaser.isModelTrained(rawFile.getLang()):
            self.truecaser.trainModel(tokFile)
            
        # STEP 3: truecasing   
        trueFile = tokFile.changeProperty("true")
        self.truecaser.truecaseFile(tokFile, trueFile) 
        
        normFile.remove()
        tokFile.remove()
        return trueFile  
    
    
    def processText(self, text, lang):
        tokText = self.tokeniser.tokenise(text, lang)                 
        trueText = self.truecaser.truecase(tokText, lang)        
        return trueText
 
 
    def revertCorpus(self, corpus):
        if not isinstance(corpus, TranslatedCorpus):
            raise RuntimeError("aligned data must be of type TranslatedCorpus")
        
        revertedSource = self.revertFile(corpus.getSourceFile())
        self.revertFile(corpus.getTargetFile())
        translation = self.revertFile(corpus.getTranslationFile())
        newCorpus = TranslatedCorpus(revertedSource.getStem(), corpus.sourceLang, corpus.targetLang, translation)
        newCorpus.origin = corpus.origin
        return newCorpus
 
 
    def revertFile(self, processedFile):
        if not processedFile.exists():
            raise RuntimeError(processedFile + " does not exist")
        
        untokFile = self.workPath + "/" + processedFile.basename().addProperty("detok") 
        self.tokeniser.detokeniseFile(processedFile,untokFile)
         
        finalFile = untokFile.changeProperty("read")
        self.tokeniser.deescapeSpecialCharacters(untokFile, finalFile)
    
        return finalFile

   
    def cutoffFiles(self, inputCorpus, outputStem, maxLength):
                   
        cleanScript = (moses_root + "/scripts/training/clean-corpus-n.perl" + " " + 
                       inputCorpus.getStem() + " " + inputCorpus.sourceLang + " " + inputCorpus.targetLang + " " 
                       + outputStem + " 1 " + str(maxLength))
        result = self.executor.run(cleanScript)
        if not result:
            raise RuntimeError("Cleaning of aligned files has failed")
        outputSource = outputStem+"."+inputCorpus.sourceLang
        outputTarget = outputStem+"."+inputCorpus.targetLang
        print "New cleaned files: " + outputSource.getDescription() + " and " + outputTarget.getDescription()
        return AlignedCorpus(outputStem, inputCorpus.sourceLang, inputCorpus.targetLang)
  
         
class Tokeniser():
    
    def __init__(self, executor, nbThreads=2):
        self.executor = executor
        self.nbThreads = nbThreads
     
    def normaliseFile(self, inputFile, outputFile):
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        cleanScript = moses_root + "/scripts/tokenizer/normalize-punctuation.perl " + lang
        result = self.executor.run(cleanScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Normalisation of %s has failed"%(inputFile))
   
    
    def deescapeSpecialCharacters(self, inputFile, outputFile):
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        deescapeScript = moses_root + "/scripts/tokenizer/deescape-special-chars.perl "
        result = self.executor.run(deescapeScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Deescaping of special characters in %s has failed"%(inputFile))

      
    def detokeniseFile(self, inputFile, outputFile):
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        print "Start detokenisation of file \"" + inputFile + "\""
        detokScript = moses_root + "/scripts/tokenizer/detokenizer.perl -l " + lang
        result = self.executor.run(detokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Detokenisation of %s has failed"%(inputFile))

        print "New de_tokenised file: " + outputFile.getDescription() 

      
    def tokeniseFile(self, inputFile, outputFile):
        lang = inputFile.getLang()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        print "Start tokenisation of file \"" + inputFile + "\""
        tokScript = (moses_root + "/scripts/tokenizer/tokenizer.perl" 
                     + " -l " + lang + " -threads " + str(self.nbThreads))
        result = self.executor.run(tokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Tokenisation of %s has failed"%(inputFile))

        print "New _tokenised file: " + outputFile.getDescription() 
            
        return outputFile
    
    
    def tokenise(self, inputText, lang):
        tokScript = moses_root + "/scripts/tokenizer/tokenizer.perl" + " -l " + lang
        return self.executor.run_output(tokScript, stdin=inputText).strip()
                


class TrueCaser():
          
    def __init__(self, executor, modelStem):
        self.executor = executor
        self.modelStem = modelStem
               
    def trainModel(self, inputFile):
        if not inputFile.exists():
            raise RuntimeError("Tokenised file " + inputFile + " does not exist")
        
        modelFile = self.modelStem + "." + inputFile.getLang()
        print "Start building truecasing model based on " + inputFile
        truecaseModelScript = (moses_root + "/scripts/recaser/train-truecaser.perl" 
                               + " --model " + modelFile + " --corpus " + inputFile)
        result = self.executor.run(truecaseModelScript)
        if not result:
            raise RuntimeError("Training of truecasing model with %s has failed"%(inputFile))

        print "New truecasing model: " + modelFile.getDescription()
    
    
    def isModelTrained(self, lang):
        return Path(self.modelStem + "." + lang).exists()
        
            
    def truecaseFile(self, inputFile, outputFile):
       
        if not inputFile.exists():
            raise RuntimeError("_tokenised file " + inputFile + " does not exist")
    
        if not self.isModelTrained(inputFile.getLang()):
            raise RuntimeError("model file for " + inputFile.getLang() + " does not exist")
    
        modelFile = Path(self.modelStem + "." + inputFile.getLang())
        print "Start truecasing of file \"" + inputFile + "\""
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        result = self.executor.run(truecaseScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Truecasing of %s has failed"%(inputFile))

        print "New truecased file: " + outputFile.getDescription()
        return outputFile
    
    
    def truecase(self, inputText, lang):
        modelFile = Path(self.modelStem + "." + lang)
        if not modelFile.exists():
            raise RuntimeError("model file " + modelFile + " does not exist")
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + modelFile
        return self.executor.run_output(truecaseScript, stdin=inputText)
    

 
def extractNgrams(tokens, size):
    ngrams = []
    if len(tokens) < size:
        return ngrams   
    for i in range(size-1, len(tokens)):
        ngrams.append(" ".join(tokens[i-size+1:i+1]))
    return ngrams
    

def getBLEUScore(reference, actual, ngrams=4):
    if len(reference) != len(actual):
        raise RuntimeError("reference and actual translation lines have different lengths")
    for i in range(0, len(reference)):
        reftokens = reference[i].split()
        actualtokens = actual[i].split()
        bp = min(1, (len(reftokens)+0.0)/len(actualtokens))
        product = bp
        for j in range(1, ngrams+1):
            refNgrams = set(extractNgrams(reftokens, j))
            if len(refNgrams) == 0:
                break
            actNgrams = set(extractNgrams(actualtokens, j))
            correctNgrams = refNgrams.intersection(actNgrams)
            precision = (len(correctNgrams)+0.0)/len(refNgrams)
            product *= precision
    return product


def getWER(reference, actual):
    refTokens = reference.split()
    actualTokens = actual.split()
    if len(refTokens) == 0:
        return len(actualTokens)
    if len(refTokens) < len(actualTokens):
        return getWER(actual, reference)
 
    # len(refTokens) >= len(actualTokens)
    if len(actualTokens) == 0:
        return len(refTokens)
 
    previous_row = range(len(actualTokens) + 1)
    for i, c1 in enumerate(refTokens):
        current_row = [i + 1]
        for j, c2 in enumerate(actualTokens):
            insertions = previous_row[j + 1] + 1 
            deletions = current_row[j] + 1       
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
 
    return (previous_row[-1]+0.0)/len(refTokens)
  
