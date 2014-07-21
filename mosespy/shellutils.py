
import os, subprocess
  

initialCmds = ""



def run(script, infile=None, outfile=None, return_output=False):
    global callincr
    callincr = callincr + 1 if 'callincr' in globals() else 1
    print "[" + str(callincr) + "] Running " + script + \
            (" < " + infile if infile is not None else "") + \
          (" > " + outfile if outfile is not None else "")
             
    stdin=open(infile) if infile is not None else None
    stdout=open(outfile, 'w') if outfile is not None else None

    if initialCmds:
        script = initialCmds + " ; " + script
        
    if return_output:
        return subprocess.check_output(script, stdin=stdin, shell=True)
    else:
        result = subprocess.call(script, stdin=stdin, stdout=stdout, shell=True)
       
        if not result:
            print "\tTask [" + str(callincr) + "] successful"
            return True
        else:
            print "\t!!! Task [" + str(callincr) + "] FAILED"
            return False
    
    
def existsExecutable(command):
    paths = os.popen(initialCmds + " ; echo $PATH").read().strip()
    for path in paths.split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, command)
            if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
                return True
    return False
        



def getsize(filename):
    desc = filename + " ("
    if os.path.isfile(filename):
        size = os.path.getsize(filename)
        if size > 1000000000:
            size = str(size/1000000000) + " G"
        elif size > 1000000:
            size = str(size/1000000) + " M"
        else:
            size = str(size/1000) + " K"     
    elif os.path.isdir(filename):
        size = os.popen('du -sh ' + filename).read()
    desc += size + ")"
    return desc

