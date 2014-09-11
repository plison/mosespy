# -*- coding: utf-8 -*-

# =================================================================                                                                   
# Copyright (C) 2014-2017 Pierre Lison (plison@ifi.uio.no)
                                                                            
# Permission is hereby granted, free of charge, to any person 
# obtaining a copy of this software and associated documentation 
# files (the "Software"), to deal in the Software without restriction, 
# including without limitation the rights to use, copy, modify, merge, 
# publish, distribute, sublicense, and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, 
# subject to the following conditions:

# The above copyright notice and this permission notice shall be 
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# =================================================================  


"""Module for interface MosesPy to the file system.  The module
contains class and methods for executing shell commands, creating
and manipulating file paths, and changing environment variables.

"""
__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"


import os, shutil, subprocess, time, Queue, threading, copy, re
from datetime import datetime
from xml.dom import minidom

class Path(str):
    """Representation of a file or directory path (which may or may not
    currently exist.  The class provides a variety of methods for creating,
    moving, deleting files and directories, as well as more specific methods
    for handling file formats used in machine translation.
    
    For a file name such as corpus.en, the part 'corpus' is called the stem 
    of the file, while the extension 'en' represents the language of the 
    corpus (i.e. English). A file name can also encode a series of additional
    optional flags, such as corpus.tok.en, which specifies that the corpus 
    has been tokenised.
    
    """
    

    def getStem(self):
        """Returns the stem of the file path (i.e. without the extension).
        
        """
        lang = self.getLang()
        return Path(self[:self.rfind("."+lang)]) if lang else self
        
    def removeFlags(self):
        """Remove all flags in the path and returns the modified path.
        
        """
        curProp = self.getFlags()
        if curProp:
            return Path(self.replace("."+curProp, ""))
        else:
            return self
        
    def getLang(self):
        """Returns the language code in the file extension (if any).
        
        """
        if  "." in self:
            langcode = self.split(".")[len(self.split("."))-1]
            langcode = re.sub(r"\d+", "", langcode)
            return langcode if langcode in languages else None
        else:
            return None        
    
    def getFlags(self):
        """Returns the list of flags in the file name.
        
        """
        stem = self.getStem()
        if "." in stem:
            return stem.split(".")[len(stem.split("."))-1]
        return None
     
    def setLang(self, lang):
        """Sets the language of the path and returns the modified path.
        
        """
        if not lang in languages:
            raise RuntimeError("language code " + lang + " is not valid")
        return self.getStem() + "." + lang
 
    def changeFlag(self, newFlag):
        """Changes the current flag for the file path and returns the new path.
        
        """
        stem = self.getStem()
        stem = stem.removeFlags()
        newPath = stem + "." + newFlag
        if self.getLang():
            newPath += "." + self.getLang()
        return newPath
    
    def addFlag(self, newFlag, reverseOrder=False):
        """Adds a new flag to the file path and returns the new path. If 
        reverseOrder is set to True, the flag is inserted before existing flags.
        
        """
        stem = self.getStem()
        if not reverseOrder or not self.getFlags():
            newPath = stem + "." + newFlag
        else:
            curProp = self.getFlags()
            newPath = stem.removeFlags() + "." + newFlag + "." + curProp
        if self.getLang():
            newPath += self.replace(stem, "")
        return newPath
         
    def getAbsolute(self):
        """Returns the absolute path.
        
        """
        return Path(os.path.abspath(self))
    
    def exists(self):
        """Returns True if the file exists and False otherwise.
        
        """
        return os.path.exists(self)
     
    def basename(self):
        """Returns the basename of the path.
        
        """
        return Path(os.path.basename(self))    

    def remove(self):
        """Removes the file or directory referred to by the path.
        
        """
        if os.path.isfile(self):
            os.remove(self)
        elif os.path.isdir(self):
            shutil.rmtree(self, ignore_errors=True)
       

    def resetdir(self):
        """Remove the current path and replaces it by an empty
        directory.
        
        """
        self.getAbsolute().remove()
        os.makedirs(self)
        

    def resetfile(self):
        """Remove the current path and replaces it by an empty
        file.
        
        """
        self.getAbsolute().remove()
        with open(self, 'a'):
            os.utime(self, None) 
    
    def move(self, newLoc):
        """Moves the file or directory to a new location.
        
        """
        if self.exists():
            shutil.move(self, newLoc)
            
    def rename(self, newname):
        """Renames the file or directory.
        
        """
        if self.exists():
            if Path(newname).exists():
                Path(newname).remove()
            os.rename(self, newname)
            return Path(newname)
            
            
    def copy(self, newLoc):
        """Copies the file or directory in a new location.
        
        """
        if self.exists():
            shutil.copy(self, newLoc)
            return Path(newLoc + "/" + self.basename())
             
    def __add__(self, other):
        """Concatenates the path with a string (or another path).
        
        """
        return Path(str.__add__(self, other))


    def getSize(self):
        """Returns the size of the file or directory.
        
        """
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
            print "cannot find file " + self
            return -1     


    def getDescription(self):
        """Returns a string of the format '{path} ({size})'.
        
        """
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
        """Returns the number of lines in the file.
        
        """
        if not self.exists() or not os.path.isfile(self):
            return RuntimeError("File does not exist")
        return int(os.popen("wc -l " + self).read().split()[0])
    
    
    def getUp(self):
        """Returns the upper directory for the path.
        
        """
        return Path(os.path.realpath(os.path.dirname(self)))
        
    def listdir(self):
        """Returns the list of files in the directory (provided
        the path refers to a directory).
        
        """
        if os.path.isdir(self):
            result = []
            for i in os.listdir(self):
                result.append(Path(i))
            return result
        else:
            raise RuntimeError(self + " not a directory")
        
        
    def readlines(self):
        """Reads the lines in the file and returns the result.
        
        """
        if os.path.isfile(self):
            with open(self, 'r') as fileD:
                lines = fileD.readlines()
            return lines
        else:
            raise RuntimeError(self + " not an existing file")

    def read(self):
        """Reads the content of the file and returns the result
        as a single string.
        
        """
        if os.path.isfile(self):
            with open(self, 'r') as fileD:
                data = fileD.read()
            return data
        else:
            raise RuntimeError(self + " not an existing file")

    def write(self, text):
        """Write the text into the file.
        
        """
        with open(self, 'w') as fileD:
            fileD.write(text)
        return self
 
    def writelines(self, lines):
        """Write the lines into the file.
        
        """
        with open(self, 'w') as fileD:
            fileD.writelines(lines)
        return self
   

class ShellExecutor(object):
    """Executor of commands through the shell.  The commands
    can be executor either sequentially or in parallel.
    
    """
    
    def __init__(self, quiet=False):
        """Creates a new executor.  If quiet is set to True,
        the executor does not print any command on the standard
        output.
        
        """
        self.callincr = 0
        self.quiet = quiet
        

    def run(self, script, stdin=None, stdout=None):
        """Runs a new script on the shell.  
        
        Args:
            script (str): the command to execute
            stdin: the standard input, which can be a file,
                a text input, or nothing (None).
            stdout: the standard output, which can be a file,
                nothing (None), or the boolean True (in which case
                the standard output is returned by the method).
        
        Returns:
            the standard output if stdout==True, or the
            return code of the script execution otherwise.
        
        """
        self.callincr += 1
        curcall = int(self.callincr)
           
        cmd_str = "[%i] Running %s "%(curcall, script)

        stdin_popen = None
        stdout_popen = None
        callInput = None
        if os.path.exists(str(stdin)):
            stdin_popen = open(stdin, 'r')
            cmd_str += "< " + stdin
        elif isinstance(stdin, basestring):
            stdin_popen = subprocess.PIPE
            callInput = stdin
            cmd_str += "<<< \"" + stdin if len(stdin)< 50 else (stdin[0:50]+"...")+"\""
        
        if os.path.exists(os.path.dirname(str(stdout))):
            stdout_popen = open(stdout, 'w')
            cmd_str += " > " + stdout
        elif stdout is not None and stdout==True:
            stdout_popen = subprocess.PIPE
                    
        if not self.quiet:
            print cmd_str
        
        inittime = datetime.now()
        p = subprocess.Popen(script, shell=True, stdin=stdin_popen, stdout=stdout_popen)
        callOutput = p.communicate(callInput)[0]
      
        if not self.quiet:     
            print "Task [%i] %s"%(curcall,"successful" if not p.returncode else "FAILED")
            print "Execution time: " + (str(datetime.now() - inittime)).split(".")[0]
            
        if stdout_popen == subprocess.PIPE:
            return callOutput.strip()
        else:
            return not p.returncode
    
    
    def run_output(self, script, stdin=None):
        """"Runs the script and return the output. 
        
        """
        return self.run(script, stdin, stdout=True)

    
    def run_parallel(self, scripts, stdins=None, stdouts=None): 
        """Runs a set of scripts in parallel, where each script is 
        executed in a separate thread.
        
        Args:
            scripts (list): the commands to execute
            stdin: the standard inputs, which can a list of files, a list
                of text inputs, or nothing (None).
            stdout: the standard output, which can a list of files, nothing 
                (None) or the boolean True, in which case the outputs are 
                returned by the method.
                
        Returns:
            if stdout is set to True, the method returns a list of strings 
            representing the scripts outputs. Else, the method returns True
            if all scripts were successfully executed, and False otherwise.
        
        """
        resultQueues = []
        for i in range(0, len(scripts)):
            time.sleep(0.1)
            script = scripts[i]
            stdin = stdins[i] if stdins else None
            stdout = stdouts[i] if stdouts else None
            resultQueue = Queue.Queue()
            t = threading.Thread(target=self._run_queue, 
                                 args=(script, resultQueue, stdin, stdout))
            resultQueues.append(resultQueue)
            t.start()
            
        time.sleep(0.1)
        print str(len(resultQueues)) + " processes started..."
        results = {}
        for counter in range(0, 10000):
            for rqi in range(0, len(resultQueues)):
                q = resultQueues[rqi]
                if not results.has_key(rqi) and not q.empty():
                    val = q.get()
                    if stdouts == None and val == False:
                        print "One parallel task failed, aborting"
                        return False
                    results[rqi] = val
            
            if len(results) < len(resultQueues):
                time.sleep(1)
                if not (counter % 60):
                    print ("Nb. of running processes after %i mins: %i"
                           %(counter/60, len(resultQueues) - len(results)))
            else:
                break
        
        print "Parallel processes successfully completed" 
        return True if stdout==None else [v for (_,v) in sorted(results.items())]
        


    def run_parallel_function(self, function, jobArgs, stdins=None, stdouts=None):
        """Runs in parallel a Python function, where each instance is executed
        with particular arguments.
        
        Args:
            function (function): the Python function to execute
            jobArgs (list): a list of lists, where the top list represents a
                particular instantiation, and the nested list the set of arguments
                to provide the Python function for this instance.
            stdins, stdouts: like for the run_parallel method
            
        """
        if not hasattr(function, '__call__'):
            raise RuntimeError("function must be a python function")
        scripts = []
        for arg in jobArgs:
            argStr = ",".join(["'"+x+"'" if isinstance(x,basestring) else str(x) for x in arg])
            fullFunction = function.__module__+"."+function.__name__
            script = ("python -u -c \"import %s ;"%(function.__module__)
                      + " %s(%s)\""%(fullFunction, argStr))
            scripts.append(script)
        return self.run_parallel(scripts, stdins, stdouts)
        
    
    def _run_queue(self, script, resultQueue, stdin=None, stdout=None):
        """runs a particular scripts and add the result to the queue object.
        
        """
        result = self.run(script, stdin, stdout)
        resultQueue.put(result)

 

def run(script, stdin=None, stdout=None):
    """Runs the script through the shell executor.
    
    """
    return ShellExecutor(quiet=True).run(script, stdin, stdout)
  
def run_output(script, stdin=None):
    """Runs the script through the shell executor and returns
    the result.
    
    """
    return ShellExecutor(quiet=True).run_output(script, stdin)

   
def existsExecutable(command):
    """Returns true if the command is an executable that is found
    in the PATH environment variable.
    
    """
    paths = os.popen("echo $PATH").read().strip()
    for path in paths.split(os.pathsep):
        path = path.strip('"')
        exe_file = os.path.join(path, command)
        if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
            return True
    return False

 
def setEnv(variable, value, override=True):
    """Sets or modifies the environment variable.
    
    Args:
        variable (str): the environment variable to change
        value (str): the value to set or add
        override (bool): whether to override existing values
        
    """
    if override or not os.environ.has_key(variable):
        os.environ[variable] = value
    elif not os.environ[variable].startswith(value):
        os.environ[variable] = value + ":" + os.environ[variable]
        
         
def getEnv():
    """Returns a copy of the environment variables.
    
    """
    return copy.deepcopy(os.environ)

def delEnv(key):
    """Deletes the current content of the environment variable.
    
    """
    del os.environ[key]

def extractLanguages():
    """Extracts possible language codes following the ISO standard.
    
    """
    isostandard = minidom.parse(Path(__file__).getUp()+"/data/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    languagesdict = {}
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code')):
            langcode = item.attributes[u'iso_639_1_code'].value
            language = item.attributes['name'].value
            languagesdict[langcode] = language
    return languagesdict


def getLanguage(langcode):
    """Returns the full name of the language referred to by its two-letters
    language code.
    
    """
    if languages.has_key(langcode):
        return languages[langcode]
    else:
        raise RuntimeError("cannot find language with code " + str(langcode))
       
languages = extractLanguages()

