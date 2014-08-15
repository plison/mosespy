import os, shutil

from xml.dom import minidom
    
class Path(str):

    def getStem(self):
        lang = self.getLang()
        return Path(self[:-len(lang)-1]) if lang else self
        
    def removeProperty(self):
        curProp = self.getProperty()
        if curProp:
            return Path(self.replace("."+curProp, ""))
        else:
            return self
        
    def getLang(self):
        if  "." in self:
            langcode = self.split(".")[len(self.split("."))-1]
            return langcode if isLanguage(langcode) else None
        else:
            return None        
    
    def getProperty(self):
        stem = self.getStem()
        if "." in stem:
            return stem.split(".")[len(stem.split("."))-1]
        return None
    
    def setLang(self, lang):
        if not lang in languages:
            raise RuntimeError("language code " + lang + " is not valid")
        return self.getStem() + "." + lang
 
    def changeProperty(self, newProperty):
        stem = self.getStem()
        stem = stem.removeProperty()
        newPath = stem + "." + newProperty
        if self.getLang():
            newPath += "." + self.getLang()
        return newPath
    
    def addProperty(self, newProperty):
        stem = self.getStem()
        newPath = stem + "." + newProperty
        if self.getLang():
            newPath += "." + self.getLang()
        return newPath
         
    
    def getPath(self):
        return Path(os.path.dirname(self))

    def exists(self):
        return os.path.exists(self)
     
    def basename(self):
        return Path(os.path.basename(self))    

    def remove(self):
        if os.path.isfile(self):
            os.remove(self)
        elif os.path.isdir(self):
            shutil.rmtree(self, ignore_errors=True)
       

    def reset(self):
        self.remove()
        os.makedirs(self)
        
            
    def make(self):
        if not os.path.exists(self):
            os.makedirs(self)
            
    def __add__(self, other):
        return Path(str.__add__(self, other))


    def getDescription(self):
        if os.path.isfile(self):
            size = os.path.getsize(self)
            if size > 1000000000:
                return self +  " (" + str(size/1000000000) + "G)"
            elif size > 1000000:
                return self +  " ("+str(size/1000000) + "M)"
            else:
                return self + " ("+str(size/1000) + "K)"     
        elif os.path.isdir(self):
            return self + " (" + os.popen('du -sh ' + self).read().split(" ")[0] + ")"
        return "(not found)"
       
    
    def countNbLines(self):
        if not self.exists():
            return RuntimeError("File does not exist")
        return int(os.popen("wc -l " + self).read().split()[0])
    
    
    def getUp(self):
        if os.path.exists(self): 
            return Path(os.path.realpath(os.path.dirname(self)))
        else:
            raise RuntimeError(self + " does not exist")
        
    def listdir(self):
        if os.path.isdir(self):
            result = []
            for i in os.listdir(self):
                result.append(Path(i))
            return result
        else:
            raise RuntimeError(self + " not a directory")
        
        
    def readlines(self):
        if os.path.isfile(self):
            with open(self, 'r') as fileD:
                lines = fileD.readlines()
            return lines
        else:
            raise RuntimeError(self + " not an existing file")
 
    def writelines(self, lines):
        with open(self, 'w') as fileD:
            fileD.writelines(lines)
    

def convertToPaths(element):
    if isinstance(element, basestring):
        element = Path(element)
    elif isinstance(element, dict):
        for k in element.keys():
            element[k] = convertToPaths(element[k])
    elif isinstance(element, list):
        for i in range(0, len(element)):
            element[i] = convertToPaths(element[i])
    return element


def extractLanguages():
    isostandard = minidom.parse(Path(__file__).getUp().getUp()+"/data/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    languagesdict = {}
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code')):
            langcode = item.attributes[u'iso_639_1_code'].value
            language = item.attributes['name'].value
            languagesdict[langcode] = language
    return languagesdict


def getLanguage(langcode):
    if languages.has_key(langcode):
        return languages[langcode]
    else:
        raise RuntimeError("cannot find language with code " + str(langcode))
    
def isLanguage(langcode):
    return languages.has_key(langcode)
   
languages = extractLanguages()

