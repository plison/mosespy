
import os, subprocess
  
my_env = os.environ
my_env["LD_LIBRARY_PATH"] = ("/cluster/home/plison/libs/boost_1_55_0/lib64" 
                             + ":/cluster/home/plison/libs/gperftools-2.2.1/lib/" 
                             + ":/cluster/software/VERSIONS/openmpi.intel-1.8/lib"
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/compiler/lib/intel64" 
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/mkl/lib/intel64" 
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/ipp/lib/intel64" 
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/tbb/lib/intel64" 
                             + ":" + os.popen("echo $LD_LIBRARY_PATH").read().strip('\n'))

my_env["PATH"] = ("/opt/rocks/bin:" + os.popen("echo $PATH").read().strip('\n'))


def run(script, infile=None, outfile=None):
    global callincr
    callincr = callincr + 1 if 'callincr' in globals() else 1
    print "[" + str(callincr) + "] Running " + script + \
            (" < " + infile if infile is not None else "") + \
          (" > " + outfile if outfile is not None else "")
             
    stdin=open(infile) if infile is not None else None
    stdout=open(outfile, 'w') if outfile is not None else None
      
    result = subprocess.call(script, stdin=stdin, stdout=stdout, shell=True, env=my_env)
       
    if not result:
        print "\tTask [" + str(callincr) + "] successful"
        return True
    else:
        print "\t!!! Task [" + str(callincr) + "] FAILED"
        return False
    
    
def existsExecutable(command):
    for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, command)
            if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
                return True
    return False
        

