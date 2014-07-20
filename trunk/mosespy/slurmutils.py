
import time, shutil, os, shellutils, textwrap, re
     
def run_batch(pythonFile, nbNodes=1):
    
    print "Creating batch file for python file " + pythonFile
    batchFile = createBatchFile("python -u " + pythonFile, nbNodes=nbNodes, name="Main")
    shellutils.run("sbatch " + batchFile, outfile="logs/out.txt")
    
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

   
def createBatchFile(script, nbNodes=1, memoryGb=60, name=None):
      
    if not name:
        name = script.split(' ')[0].split("/")[len(script.split(' ')[0].split("/"))-1]
    #script = script + (" < " + infile if infile else "")  + (" > " + outfile if outfile else "")
    if memoryGb > 60:
        memoryStr = str(max(1, memoryGb)) + "G --partition=hugemem"
    else:
        memoryStr = str(max(1, memoryGb)) + "G"
    batchFile = "logs/"+name.replace("$","")+".sh"
    libraryPath = ("$LD_LIBRARY_PATH:/cluster/home/plison/libs/boost_1_55_0/lib64" 
                   + ":/cluster/home/plison/libs/gperftools-2.2.1/lib/")
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
                            module load intel
                            module load openmpi.intel
                            export LD_LIBRARY_PATH=%s                                   
                            %s""" %(name, memoryStr, nbNodes, libraryPath, script))   
    with open(batchFile, 'w') as f:
        f.write(batch)
    return batchFile




def trainInSplits(baseScript, nbSplits):
        
    tmDir = (re.search("--root-dir\s+((\S)+)", baseScript)).group(1)
    trainData = (re.search("-corpus\s+((\S)+)", baseScript)).group(1)
    alignment = (re.search("-alignment\s+((\S)+)", baseScript)).group(1)
    source = (re.search("-f\s+((\S)+)", baseScript)).group(1)
    target = (re.search("-e\s+((\S)+)", baseScript)).group(1)

    outputDir = os.path.dirname(tmDir) + "/splits"
    shutil.rmtree(outputDir)
    os.makedirs(outputDir)

    splits = splitData(trainData + "." + source, outputDir, nbSplits)
    splitData(trainData + "." + target, outputDir, nbSplits)
              
    for split in splits:
            shutil.rmtree(split, ignore_errors=True)

    paramScript = baseScript.replace(tmDir, outputDir + "/" + "$TASK_ID")\
                            .replace(trainData, outputDir + "/" +"$TASK_ID")
    batchFile = createBatchFile(paramScript, name="split-$TASK_ID")
    
    shellutils.run("arrayrun 0-%i --job-name=\"split\"  %s &"%(nbSplits-1, batchFile), 
                 outfile="./logs/out-split.txt")
    time.sleep(1)
    jobs = set()
    with open('./logs/out-split.txt') as out:
        for l in out.readlines():
            if "Submitted batch job" in l:
                jobid = l.split(" ")[len(l.split(" "))-1].strip("\n")
                jobs.add(jobid)
    time.sleep(1)
    while True:
        queue = os.popen("squeue -u plison").read()
        if len(set(queue.split()).intersection(jobs)) == 0:
            break
        print "Unfinished jobs: " + str(list(jobs))
        time.sleep(10)
       
    if not os.path.exists(tmDir+"/model"):
        os.makedirs(tmDir+"/model")
    with open(tmDir+"/model/aligned."+alignment, 'w') as al:
        for split in splits:
            with open(split+"/model/aligned."+alignment) as part:
                al.write(part.read())
                                           
    return shellutils.run(baseScript + " --first-step 4")

    


def splitData(dataFile, outputDir, nbSplits):
        
    extension = dataFile.split(".")[len(dataFile.split("."))-1]
    totalLines = int(os.popen("wc -l " + dataFile).read().split(" ")[0])
    shellutils.run("split -d -l %i -a %i %s %s"%(totalLines / nbSplits + 1, nbSplits, 
                                               dataFile, outputDir+"/"+ extension +"." ))
    
    digits = []
    for f in os.listdir(outputDir):
        if f.startswith(extension+".") and f.split(".")[1].isdigit():
            digit = f.split(".")[1]
            shutil.move(outputDir+"/"+ extension+ "."+digit, outputDir+"/"+str(int(digit))+"."+extension)
            digits.append(outputDir+"/"+str(int(digit)))
    digits.sort()
    return digits
  
    