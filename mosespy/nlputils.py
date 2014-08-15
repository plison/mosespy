
from mosespy.pathutils import Path 
from xml.dom import minidom

rootDir = Path(__file__).getUp().getUp()
moses_root = rootDir + "/moses" 
  
class Tokeniser():
    
    def __init__(self, executor, nbThreads=2):
        self.executor = executor
        self.nbThreads = nbThreads
     
    def normaliseFile(self, inputFile, outputFile):
        lang = inputFile.getSuffix()
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
        lang = inputFile.getSuffix()
        if not inputFile.exists():
            raise RuntimeError("raw file " + inputFile + " does not exist")
                        
        print "Start detokenisation of file \"" + inputFile + "\""
        detokScript = moses_root + "/scripts/tokenizer/detokenizer.perl -l " + lang
        result = self.executor.run(detokScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Detokenisation of %s has failed"%(inputFile))

        print "New de_tokenised file: " + outputFile.getDescription() 

      
    def tokeniseFile(self, inputFile, outputFile):
        lang = inputFile.getSuffix()
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
          
    def __init__(self, executor, modelFile):
        self.executor = executor
        self.modelFile = modelFile
               
    def trainModel(self, inputFile):
        if not inputFile.exists():
            raise RuntimeError("Tokenised file " + inputFile + " does not exist")
        
        print "Start building truecasing model based on " + inputFile
        truecaseModelScript = (moses_root + "/scripts/recaser/train-truecaser.perl" 
                               + " --model " + self.modelFile + " --corpus " + inputFile)
        result = self.executor.run(truecaseModelScript)
        if not result:
            raise RuntimeError("Training of truecasing model with %s has failed"%(inputFile))

        print "New truecasing model: " + self.modelFile.getDescription()
    
    
    def isModelTrained(self):
        return self.modelFile.exist()
        
            
    def truecaseFile(self, inputFile, outputFile):
       
        if not inputFile.exists():
            raise RuntimeError("_tokenised file " + inputFile + " does not exist")
    
        if not self.isModelTrained():
            raise RuntimeError("model file " + self.modelFile + " does not exist")
    
        print "Start truecasing of file \"" + inputFile + "\""
        truecaseScript = moses_root + "/scripts/recaser/truecase.perl" + " --model " + self.modelFile
        result = self.executor.run(truecaseScript, inputFile, outputFile)
        if not result:
            raise RuntimeError("Truecasing of %s has failed"%(inputFile))

        print "New truecased file: " + outputFile.getDescription()
        return outputFile
    
    
    def truecase(self, inputText, modelFile):
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
        


def getLanguage(langcode):
    isostandard = minidom.parse(rootDir+"/data/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code') 
            and item.attributes[u'iso_639_1_code'].value == langcode):
            return item.attributes['name'].value
    raise RuntimeError("Language code '" + langcode + "' could not be related to a known language")


