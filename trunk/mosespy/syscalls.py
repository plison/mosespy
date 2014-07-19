
import os, subprocess, shutil, time, textwrap
       
my_env = os.environ
my_env["LD_LIBRARY_PATH"] = ("/cluster/home/plison/libs/boost_1_55_0/lib64" 
                             + ":/cluster/home/plison/libs/gperftools-2.2.1/lib/" 
                             + ":/cluster/software/VERSIONS/openmpi.intel-1.8/lib"
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/compiler/lib/intel64" 
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/mkl/lib/intel64" 
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/ipp/lib/intel64" 
                             + ":/cluster/software/VERSIONS/intel-2013.sp1.3/tbb/lib/intel64" 
                             + ":" + os.popen("echo $LD_LIBRARY_PATH").read().strip('\n'))

my_env["PATH"] = ("/opt/rocks/bin:" 
                  + os.popen("echo $PATH").read().strip('\n'))

def run(script, infile=None, outfile=None):
    global callincr
    callincr = callincr + 1 if 'callincr' in globals() else 1
    print "\t[" + str(callincr) + "] Running " + script + \
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
        
     
def run_batch(pythonFile, nbNodes=1):
    global callincr
    callincr = callincr + 1 if 'callincr' in globals() else 1
    print ("\t[" + str(callincr) + "] Creating batch file for python file " + pythonFile)

    batchFile = createBatchFile("python " + pythonFile, nbNodes=nbNodes, name="Main")
    run("sbatch " + batchFile, outfile="logs/out.txt")
    with open('logs/out.txt') as out:
        text = out.read().strip('\n')
        if "Submitted batch job" in text:
            jobid = text.replace("Submitted batch job ", "")
            print "Waiting for job " + jobid + " to start..."
            jobfile = "slurm-"+jobid+".out"
            while not os.path.exists(jobfile):
                time.sleep(5)
            print "Job " + jobid + " has now started"
            with open(jobfile) as slurm:
                while True:
                    where = slurm.tell()
                    line = slurm.readline()
                    if not line:
                        time.sleep(1)
                        slurm.seek(where)
                    elif "Job " + jobid in line and "completed" in line:
                        break
                    else:
                        print line,  
            shutil.move(jobfile, "./logs/"+jobfile) 
        else:
            print "Cannot start batch script: " + text
            return False
    return True

   
def createBatchFile(script, infile=None, outfile=None, nbNodes=1, name=None, memoryGb=60):
      
    if not name:               
        name = script.split(' ')[0].split("/")[len(script.split(' ')[0].split("/"))-1]
    script = script + (" < " + infile if infile else "")  + (" > " + outfile if outfile else "")
    if memoryGb > 60:
        memoryStr = str(max(1, memoryGb)) + "G --partition=hugemem"
    else:
        memoryStr = str(max(1, memoryGb)) + "G"
    batchFile = "logs/"+name.replace("$","")+".sh"
    batch = textwrap.dedent("""\
                            #!/bin/bash
                            #SBATCH --job-name=%s
                            #SBATCH --account=nn9106k
                            #SBATCH --time=05:00:00
                            #SBATCH --exclusive
                            #SBATCH --ntasks=1
                            #SBATCH --mem-per-cpu=%s
                            #SBATCH --nodes=%i

                            source /cluster/bin/jobsetup  
                            module load openmpi.intel
                            export LD_LIBRARY_PATH=%s
                                   
                            %s""" %(name, "50G", nbNodes, my_env["LD_LIBRARY_PATH"], script))   
    with open(batchFile, 'w') as f:
        f.write(batch)
    return batchFile
    
    