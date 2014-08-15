
import os, subprocess, time
from datetime import datetime


class CommandExecutor(object):
    
    def __init__(self):
        self.callincr = 0
        
    def run(self, script, stdin=None, stdout=None):
        self.callincr += 1
        print "[" + str(self.callincr) + "] Running " + script + \
                (" < " + stdin if isinstance(stdin, basestring) else "") + \
              (" > " + stdout if isinstance(stdout, basestring) else "")
                  
        if os.path.exists(str(stdin)):
            stdin_popen = open(stdin, 'r')
        elif isinstance(stdin, basestring):
            stdin_popen = subprocess.PIPE
        else:
            stdin_popen = None
            
        if os.path.exists(os.path.dirname(str(stdout))):
            stdout_popen = open(stdout, 'w')
        elif stdout is not None and not stdout:
            stdout_popen = subprocess.PIPE
        else:
            stdout_popen = None
        
        inittime = datetime.now()
        p = subprocess.Popen(script, shell=True, stdin=stdin_popen, stdout=stdout_popen)
        out_popen = p.communicate(stdin)[0]
        
        print "Task [" + str(self.callincr) + "] " + ("successful" if not p.returncode else "FAILED")
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

