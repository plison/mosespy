import re
from system import Path

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
                end = len(lines)-1
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
    
    
    
        
        
            
        
            
