
import os, subprocess, shutil


class CommandExecutor(object):
    
    def run(self, script, infile=None, outfile=None, return_output=False):
        global callincr
        callincr = callincr + 1 if 'callincr' in globals() else 1
        print "[" + str(callincr) + "] Running " + script + \
                (" < " + infile if infile is not None else "") + \
              (" > " + outfile if outfile is not None else "")
                 
        stdin=open(infile) if infile is not None else None
        stdout=open(outfile, 'w') if outfile is not None else None
    
        if return_output:
            return os.popen(script + " < " + infile if infile else script).read()
        else:
            result = subprocess.call(script, stdin=stdin, stdout=stdout, shell=True)
           
            if not result:
                print "Task [" + str(callincr) + "] successful"
                return True
            else:
                print "!!! Task [" + str(callincr) + "] FAILED"
                return False
        
 
 
def run(script, infile=None, outfile=None, return_output=False):
    return CommandExecutor().run(script, infile, outfile, return_output)
  
   
def existsExecutable(command):
    paths = os.popen("echo $PATH").read().strip()
    for path in paths.split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, command)
            if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
                return True
    return False
        

def resetDir(dirName):
    shutil.rmtree(dirName, ignore_errors=True)   
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

