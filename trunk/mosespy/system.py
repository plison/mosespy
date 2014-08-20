# -*- coding: utf-8 -*-

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date$"    


import os, shutil, subprocess, time, Queue, threading, copy
from datetime import datetime
from xml.dom import minidom

class CommandExecutor(object):
    
    def __init__(self, quiet=False):
        self.callincr = 0
        self.tmpdir = None
        self.quiet = quiet
        
        if "/usr/local/bin" not in os.environ["PATH"]:
            os.environ["PATH"] = "/usr/local/bin:" + os.environ["PATH"]


    def run(self, script, stdin=None, stdout=None):
            
        self.callincr += 1
        curcall = int(self.callincr)
        str_stdin = ""
        if os.path.exists(str(stdin)):
            stdin_popen = open(stdin, 'r')
            str_stdin = " < " + stdin
        elif isinstance(stdin, basestring):
            stdin_popen = subprocess.PIPE
            str_stdin = " <<< \"" + stdin + "\""
        else:
            stdin_popen = None
        
        str_stdout = ""
        if os.path.exists(os.path.dirname(str(stdout))):
            stdout_popen = open(stdout, 'w')
            str_stdout = " > " + stdout
        elif stdout is not None and not stdout:
            stdout_popen = subprocess.PIPE
        else:
            stdout_popen = None
        
        if not self.quiet:     
            print "[" + str(curcall) + "] Running " + script + str_stdin + str_stdout
        
        inittime = datetime.now()
        p = subprocess.Popen(script, shell=True, stdin=stdin_popen, stdout=stdout_popen)
        out_popen = p.communicate(stdin)[0]
        
        if not self.quiet:     
            print "Task [" + str(curcall) + "] " + ("successful" if not p.returncode else "FAILED")
            print "Execution time: " + (str(datetime.now() - inittime)).split(".")[0]
            
        if stdout_popen == subprocess.PIPE:
            return out_popen.strip()
        else:
            return not p.returncode
    
    
    def run_output(self, script, stdin=None):
        return self.run(script, stdin, stdout=False)

    
    def run_parallel(self, script, jobArgs, stdins=None, stdouts=None): 
         
        resultQueues = []
        for i in range(0, len(jobArgs)):
            time.sleep(0.1)
            jobArg = jobArgs[i]
            filledScript = script%(jobArg)
            stdin = stdins[i] if stdins else None
            stdout = stdouts[i] if stdouts else None
            resultQueue = Queue.Queue()
            t = threading.Thread(target=self._run_queue, args=(filledScript, resultQueue, stdin, stdout))
            resultQueues.append(resultQueue)
            t.start()
            
        time.sleep(0.1)
        print str(len(resultQueues)) + " processes started..."
        for counter in range(0, 10000):
            stillRunning = []
            for q in resultQueues:
                if not q.empty():
                    if not q.get():
                        print "One parallel task failed, aborting"
                        return False
                else:
                    stillRunning.append(q)
            resultQueues = stillRunning 
            if len(resultQueues) > 0:
                time.sleep(1)
                if not (counter % 60):
                    print "Number of running processes after %i mins: %i"%(counter/60, len(resultQueues))
            else:
                break
        print "Parallel processes successfully completed" 
        return True
        
    
    def _run_queue(self, script, resultQueue, stdin=None, stdout=None):
        result = self.run(script, stdin, stdout)
        resultQueue.put(result)


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
            return langcode if langcode in languages else None
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
         
    def getAbsolute(self):
        return Path(os.path.abspath(self))
    
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
        
    
    def move(self, newLoc):
        if self.exists():
            shutil.move(self, newLoc)
            
    def printlines(self):
        if os.path.isfile(self):
            with open(self, 'r') as fileD:
                for line in fileD.readlines():
                    print line.strip()
        else:
            raise RuntimeError(self + " not an existing file")
         
            
    def make(self):
        if not os.path.exists(self):
            os.makedirs(self)
            
    def __add__(self, other):
        return Path(str.__add__(self, other))



    def getSize(self):
        if os.path.isfile(self):
            return os.path.getsize(self)
        elif os.path.isdir(self):
            sizeStr = os.popen('du -sh ' + self).read().split("\t")[0]
            number = float(sizeStr[:-1])
            if sizeStr[-1]=="K":
                return number*1000
            elif sizeStr[-1]=="M":
                return number*1000000
            elif sizeStr[-1]=="G":
                return  number*1000000000
            else:
                return number
        else:
            return None       


    def getDescription(self):
        if self.exists():
            size = self.getSize()                  
            if size > 1000000000:
                sizeStr = str(size/1000000000) + "G"
            elif size > 1000000:
                sizeStr = str(size/1000000) + "M"
            else:
                sizeStr = str(size/1000) + "K"  
            return self + " ("+sizeStr+")"   
        else:
            return "not found"
       
    
    def countNbLines(self):
        if not self.exists():
            return RuntimeError("File does not exist")
        return int(os.popen("wc -l " + self).read().split()[0])
    
    
    def getUp(self):
        return Path(os.path.realpath(os.path.dirname(self)))
        
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

 
def run(script, stdin=None, stdout=None):
    return CommandExecutor(True).run(script, stdin, stdout)
  
def run_output(script, stdin=None):
    return CommandExecutor(True).run_output(script, stdin)

   
def existsExecutable(command):
    paths = os.popen("echo $PATH").read().strip()
    for path in paths.split(os.pathsep):
        path = path.strip('"')
        exe_file = os.path.join(path, command)
        if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
            return True
    return False

 
def setEnv(variable, value, override=True):
    if override:
        os.environ[variable] = value
    else:
        os.environ[variable] = value + ":" + os.environ[variable]
        
        
def getEnv():
    return copy.deepcopy(os.environ)

def delEnv(key):
    del os.environ[key]

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
       
languages = extractLanguages()

