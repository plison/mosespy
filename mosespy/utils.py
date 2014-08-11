
import os, subprocess, shutil, sys, threading, time
from datetime import datetime
from xml.dom import minidom


class CommandExecutor(object):
    
    def run(self, script, stdin=None, stdout=None):
        global callincr
        callincr = callincr + 1 if 'callincr' in globals() else 1
        sys.stderr.write("[" + str(callincr) + "] Running " + script + \
                (" < " + stdin if isinstance(stdin, basestring) else "") + \
              (" > " + stdout if isinstance(stdout, basestring) else "")+"\n")
                  
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
        
        sys.stderr.write("Task [" + str(callincr) + "] " + ("successful" if not p.returncode 
                                                            else "FAILED")+"\n")
        sys.stderr.write("Execution time: " + (str(datetime.now() - inittime)).split(".")[0]+"\n")
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


def rmDir(dirName):
    shutil.rmtree(dirName, ignore_errors=True)   
        

def resetDir(dirName):
    rmDir(dirName)  
    os.makedirs(dirName)


def getsize(filename):
    if os.path.isfile(filename):
        size = os.path.getsize(filename)
        if size > 1000000000:
            return filename +  " (" + str(size/1000000000) + "G)"
        elif size > 1000000:
            return filename +  " ("+str(size/1000000) + "M)"
        else:
            return filename + " ("+str(size/1000) + "K)"     
    elif os.path.isdir(filename):
        return filename + " (" + os.popen('du -sh ' + filename).read().split(" ")[0] + ")"
    return "(not found)"



def countNbLines(filename):
    if not os.path.exists(filename):
        return RuntimeError("File does not exist")
    return int(run_output("wc -l " + filename).split()[0])


def getLanguage(langcode):
    rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    isostandard = minidom.parse(rootDir+"/data/iso639.xml")
    itemlist = isostandard.getElementsByTagName('iso_639_entry') 
    for item in itemlist :
        if (item.attributes.has_key('iso_639_1_code') 
            and item.attributes[u'iso_639_1_code'].value == langcode):
                return item.attributes['name'].value
    raise RuntimeError("Language code '" + langcode + "' could not be related to a known language")




def splitData(data, outputDir, nbSplits):
    
    if isinstance(data, list):
        extension = ""
        lines = data
    elif isinstance(data, basestring) and os.path.exists(data):  
        extension = "." + data.split(".")[len(data.split("."))-1]
        fullFile = open(data, 'r')
        lines = fullFile.readlines()
        fullFile.close()
        lines = data.split()
    else:
        raise RuntimeError("cannot split the content for data " + str(data))
        
    totalLines = len(lines) 
    nbSplits = min(nbSplits, totalLines)
    
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

 
