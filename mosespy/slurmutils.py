
import time, shutil, os, shellutils, textwrap, re, sys
import mosespy
from mosespy import Experiment 
     
     
class SlurmExperiment(Experiment):
    
    def __init__(self, expName, sourceLang=None, targetLang=None):
            
        if "--no-sbatch" in sys.argv:
            Experiment.__init__(self, expName, sourceLang, targetLang)
            return
        
        print "Starting " + sys.argv[0] + " using sbatch"
            
        if not os.path.exists(sys.argv[0]):
            raise RuntimeError(sys.argv[0] + " must be a python file") 
    
        pythonFile = sys.argv[0].strip()
        nbNodes = 1
        for arg in sys.argv:
            if "--nodes=" in arg:
                nbNodes = int(arg.replace("--nodes=", "").strip())
                    
        batchFile = createBatchFile("python -u " + pythonFile +" --no-sbatch", nbNodes=nbNodes, name="Main")
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
                print "Cannot start batch script, aborting"
                exit()
                
    
  
    def trainTranslationModel(self, trainStem=None, nbSplits=1, nbThreads=16):
        
        if nbSplits == 1:
            Experiment.trainTranslationModel(self, trainStem, nbThreads)
            return
       
        if trainStem:         
            trainData = self.processAlignedData(trainStem)
            self.system["tm"] = {"data": trainData}
            self.recordState()        
        elif not self.system.has_key("tm") or not self.system["tm"].has_key("data"):
            raise RuntimeError("Aligned training data is not yet processed")    
        
        print ("Building translation model " + self.system["source"] + "-" 
               + self.system["target"] + " with " + trainData["clean"] 
               + " with " + str(nbSplits) + " splits")

        tmScript, tmDir = self.getTrainScript(nbThreads)
    
        outputDir = os.path.dirname(tmDir) + "/splits"
        shutil.rmtree(outputDir)
        os.makedirs(outputDir)
    
        splits = splitData(trainData["clean"] + "." + self.system["source"], outputDir, nbSplits)
        splitData(trainData["clean"] + "." + self.system["target"], outputDir, nbSplits)
                      
        paramScript = tmScript.replace(tmDir, outputDir + "/" + "$TASK_ID")\
                                .replace(trainData["clean"], outputDir + "/" +"$TASK_ID")
        arrayrun(paramScript, nbSplits)
           
        if not os.path.exists(tmDir+"/model"):
            os.makedirs(tmDir+"/model")
        with open(tmDir+"/model/aligned."+self.system["alignment"], 'w') as al:
            for split in splits:
                with open(split+"/model/aligned."+self.system["alignment"]) as part:
                    al.write(part.read())
                                               
        result = shellutils.run(tmScript + " --first-step 4")

        if result:
            print "Finished building translation model in directory " + mosespy.getFileDescription(tmDir)
            self.system["tm"]["dir"]=tmDir
            self.recordState()
        else:
            print "Construction of translation model FAILED"
            shellutils.run("rm -rf " + tmDir)



def arrayrun(paramScript, nbSplits):
    
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
        queue = os.popen("squeue -u " + os.popen("whoami").read()).read()
        if len(set(queue.split()).intersection(jobs)) == 0:
            break
        print "Unfinished jobs: " + str(list(jobs))
        time.sleep(10)
      
         

   
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
  
    