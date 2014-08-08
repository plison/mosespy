
import os, subprocess, shutil



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
            
        p = subprocess.Popen(script, shell=True, stdin=stdin_popen, stdout=stdout_popen)
        out_popen = p.communicate(stdin)[0]
        
        print "Task [" + str(callincr) + "] " + ("successful" if not p.returncode else "FAILED")
            
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

