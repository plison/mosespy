
import os, subprocess, shutil, time, random
from datetime import datetime
from xml.dom import minidom


class Path(str):

    def getStem(self, fullPath=True):
        extension = self.getFileExtension(1)
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
            raise RuntimeError("file " + self + " does not have an infix")

    
    def exists(self):
        return os.path.exists(self)
     
    def replacePath(self, newPath):
        if newPath[-1] == "/":
            newPath = newPath[:-1]
        return Path(newPath + "/" + os.path.basename(self))
    
    
    def setInfix(self, infix):
        if self.count(".") == 0:
            return Path(self + "." + infix)
        elif self.count(".") == 1:
            extension = self.getFileExtension(1)
            return Path(self[:-len(extension)] + infix + "." + extension)
        else:
            existingInfix = self.getFileExtension(2)
            return Path(self.replace("."+existingInfix+".", "."+infix+"."))

    def remove(self):
        if os.path.isfile(self):
            os.remove(self)
        elif os.path.isdir(self):
            shutil.rmtree(self, ignore_errors=True)
       

    def reset(self):
        self.remove()
        if os.path.isdir(self):
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

    

class CommandExecutor(object):
    
    def run(self, script, stdin=None, stdout=None):
        global callincr
        callincr = callincr + 1 if 'callincr' in globals() else 1
        print "[" + str(callincr) + "] Running " + script + \
                (" < " + stdin if isinstance(stdin, basestring) else "") + \
              (" > " + stdout if isinstance(stdout, basestring) else "")
                  
        if os.path.exists(str(stdin)):
            stdin_popen = file(stdin, 'r')
        elif isinstance(stdin, basestring):
            stdin_popen = subprocess.PIPE
        else:
            stdin_popen = None
            
        if os.path.exists(os.path.dirname(str(stdout))):
            stdout_popen = file(stdout, 'w')
        elif stdout is not None and not stdout:
            stdout_popen = subprocess.PIPE
        else:
            stdout_popen = None
        
        inittime = datetime.now()
        p = subprocess.Popen(script, shell=True, stdin=stdin_popen, stdout=stdout_popen)
        out_popen = p.communicate(stdin)[0]
        
        print "Task [" + str(callincr) + "] " + ("successful" if not p.returncode else "FAILED")
        print "Execution time: " + (str(datetime.now() - inittime)).split(".")[0]
        if stdout_popen == subprocess.PIPE:
            return out_popen
        else:
            return not p.returncode
        

    
    def run_output(self, script, stdin=None):
        return self.run(script, stdin, stdout=False)
               
 
 
def run(script, stdin=None, stdout=None):
    return CommandExecutor().run(script, stdin, stdout)
  
def run_output(script, stdin=None):
    return CommandExecutor().run_output(script, stdin)

   
def existsExecutable(command):
    paths = os.popen("echo $PATH").read().strip()
    for path in paths.split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, command)
            if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
                return True
    return False



def getLanguage(langcode):
    rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    isostandard = minidom.parse(rootDir+"/data/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code') 
            and item.attributes[u'iso_639_1_code'].value == langcode):
                return item.attributes['name'].value
    raise RuntimeError("Language code '" + langcode + "' could not be related to a known language")


def drawRandomNumbers(start, end, number, exclusion=set()):
    numbers = set()
    while len(numbers) < number:
        choice = random.randrange(start, end)
        if choice not in exclusion:
            numbers.add(choice)
    return numbers

    
def waitForCompletion(jobs):
    print "Parallel run of " + str(len(jobs)) + " processes"
    time.sleep(0.1)
    for counter in range(0, 10000):
        running = [t for t in jobs if t.is_alive()]
        if len(running) > 0:
            time.sleep(1)
            if not (counter % 60):
                print "Number of running processes: " + str(len(running))
        else:
            break
    print "Parallel processes completed"  


def splitData(inputFile, outputDir, nbSplits):

    if isinstance(inputFile, basestring) and os.path.exists(inputFile):  
        extension = "." + Path(inputFile).getSuffix()
        fullFile = open(inputFile, 'r')
        lines = fullFile.readlines()
        fullFile.close()
    else:
        raise RuntimeError("cannot split the content for data " + str(inputFile))
        
    totalLines = len(lines) 
    nbSplits = min(nbSplits, totalLines)
    print "Splitting " + str(totalLines)  + " with " + str(nbSplits)
    filenames = []
    curSplit = 0
    filename = outputDir + "/" + str(curSplit) + extension
    filenames.append(filename)
    curFile = open(filename, 'w')
    nbLines = 0
    for l in lines:
        curFile.write(l)
        nbLines += 1
        if nbLines >= (totalLines / nbSplits + 1):
            nbLines = 0
            curFile.close()
            curSplit += 1
            filename = outputDir + "/" + str(curSplit) + extension
            curFile = open(filename, 'w')
            filenames.append(filename)
    curFile.close()
    return filenames

 
