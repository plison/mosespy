import os, shutil

class Path(str):

    def getStem(self, fullPath=True):
        extension = self.getSuffix()
        if  fullPath:
            return Path(self[:-len(extension)-1])
        else:
            return Path(os.path.basename(self)[:-len(extension)-1])
    
    def getSuffix(self):
        if  "." in self:
            return self.split(".")[len(self.split("."))-1]
        else:
            raise RuntimeError("file " + self + " does not have a suffix")
    
    def getPath(self):
        return Path(os.path.dirname(self))
            

    def getInfix(self):
        if  self.count(".") >= 2:
            return self.split(".")[len(self.split("."))-2]
        else:
            return ""

    def exists(self):
        return os.path.exists(self)
     
    def changePath(self, newPath):
        if newPath[-1] == "/":
            newPath = newPath[:-1]
        return Path(newPath + "/" + os.path.basename(self))
    
    
    def setInfix(self, infix):
        if self.count(".") == 0:
            return Path(self + "." + infix)
        elif self.count(".") == 1:
            extension = self.getSuffix()
            return Path(self[:-len(extension)] + infix + "." + extension)
        else:
            existingInfix = self.split(".")[len(self.split("."))-2]
            return Path(self.replace("."+existingInfix+".", "."+infix+"."))

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


