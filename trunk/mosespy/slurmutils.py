
import time, shutil, os, shellutils, textwrap, re
from mosespy import Experiment 
  
  

class SlurmExecutor(object):
    
    def __init__(self, account, time="3:00:00", memory="10G", nbThreads=6):
        self.account = account
        self.time = time
        self.memory = memory
        self.nbThreads = nbThreads
        
    def run(self, script, infile=None, outfile=None, return_output=False):
        srun_cmd = ("srun --account=" + self.account
                + " --mem-per-cpu=" + self.memory
                +" --exclusive"
                + " --cpus-per-task=" + str(self.nbThreads)
                + " --time=" + self.time
                + " " + script)
        shellutils.run(srun_cmd, infile, outfile, return_output)
    
       
class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None):
        Experiment.__init__(self, expName, sourceLang, targetLang)
  
        if not shellutils.existsExecutable("sbatch"):
            print "SLURM system not present, some methods might be unavailable"
            return
        elif not account:
            account = getDefaultSlurmAccount()
        if account:
            self.system["slurm_account"] = account
      
        os.environ["LD_LIBRARY_PATH"] = (os.popen("module load intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + os.popen("module load openmpi.intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                                         + "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        os.environ["PATH"] = "/opt/rocks/bin:" + os.environ["PATH"]
        self.executor = SlurmExecutor(account)

        
    def trainTranslationModel(self, trainStem=None, nbSplits=1, nbThreads=16):
        
        if nbSplits == 1:
            Experiment.trainTranslationModel(self, trainStem, nbThreads)
            return
        
        if not self.system.has_key("slurm_account"):
            raise RuntimeError("SLURM system not present, cannot split model training")
       
        if trainStem:         
            trainData = self.processAlignedData(trainStem)
            self.system["tm"] = {"data": trainData}
            self.recordState()        
        elif not self.system.has_key("tm") or not self.system["tm"].has_key("data"):
            raise RuntimeError("Aligned training data is not yet processed")  
        
        cleanData = self.system["tm"]["data"]["clean"]         
        print ("Building translation model " + self.system["source"] + "-" 
               + self.system["target"] + " with " +  cleanData
               + " with " + str(nbSplits) + " splits")
    
        splitDir = self.system["path"] + "/splits"
        shutil.rmtree(splitDir, ignore_errors=True)
        os.makedirs(splitDir)
        splitData(cleanData + "." + self.system["source"], splitDir, nbSplits)
        splitData(cleanData + "." + self.system["target"], splitDir, nbSplits)

        tmDir = self.system["path"] + "/translationmodel"
        tmScript = self.getTrainScript(nbThreads, tmDir)
        paramScript = (tmScript.replace(tmDir, splitDir + "/" + "$TASK_ID")\
                                .replace(cleanData, splitDir + "/" +"$TASK_ID")
                                + " --last-step 3")
        self.arrayrun(paramScript, nbSplits)
        
        shutil.rmtree(tmDir, ignore_errors=True)   
        os.makedirs(tmDir+"/model")
        alignFile = tmDir+"/model/aligned."+self.system["alignment"]
        with open(alignFile, 'w') as align:
            for split in range(0, nbSplits):
                splitFile = splitDir+ "/" + str(split)+"/model/aligned."+self.system["alignment"]
                with open(splitFile) as part:
                    for partline in part.readlines():
                        if partline.strip():
                            align.write(partline.strip('\n') + '\n')
                            
        result = shellutils.run(tmScript + " --first-step 4")

        if result:
            print "Finished building translation model in: " + shellutils.getsize(tmDir)
            self.system["tm"]["dir"]=tmDir
            self.recordState()
        else:
            shellutils.run("rm -rf " + tmDir)


    def arrayrun(self, paramScript, nbSplits, time="2:00:00", memory="10G"):
    
        shellutils.run("arrayrun 0-" + str(nbSplits-1)
                       + " --account " + self.system["slurm_account"]
                       + " --time " + time 
                       + " --cpus-per-task " + 16
                       + " --mem-per-cpu " + memory
                       + " \"" + paramScript + "\"")       
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
            print "Unfinished array jobs: " + str(list(jobs))
            time.sleep(60)
        print "SLURM array run completed."
        for job in jobs:
            shutil.move("slurm-"+job+".out", "logs/slurm-"+job+".out")
            
    
   
    def createBatchFile(self, script, time="6:00:00", nbTasks=1, memoryGb=60, name=None):
          
        if not name:
            name = script.split(' ')[0].split("/")[len(script.split(' ')[0].split("/"))-1]
        if memoryGb > 60:
            memoryStr = str(max(1, memoryGb)) + "G --partition=hugemem"
        else:
            memoryStr = str(max(1, memoryGb)) + "G"
        batchFile = "logs/"+name.replace("$","")+".sh"
     
        batch = textwrap.dedent("""\
                                #!/bin/bash
                                #SBATCH --job-name=%s
                                #SBATCH --account=%s
                                #SBATCH --time=%s
                                #SBATCH --ntasks=%i
                                #SBATCH --mem-per-cpu=%s
    
                                source /cluster/bin/jobsetup  
                                %s 
                                """%(name, self.system["slurm_account"], time, 
                                     nbTasks, memoryStr, script))   
        with open(batchFile, 'w') as f:
            f.write(batch)
        return batchFile

   


def splitData(dataFile, outputDir, nbSplits):
        
    extension = dataFile.split(".")[len(dataFile.split("."))-1]
    totalLines = int(os.popen("wc -l " + dataFile).read().strip().split(" ")[0])

    with open(dataFile) as fullFile:
        curSplit = 0
        curFile = open(outputDir + "/" + str(curSplit) + "." + extension, 'w')
        nbLines = 0
        for l in fullFile.readlines():
            curFile.write(l)
            nbLines += 1
            if nbLines >= (totalLines / nbSplits + 1):
                nbLines = 0
                curFile.close()
                curSplit += 1
                curFile = open(outputDir + "/" + str(curSplit) + "." + extension, 'w')
        curFile.close()



def getDefaultSlurmAccount():
    user = (os.popen("whoami")).read().strip()
    result = (os.popen("sacctmgr show User "+user + " -p")).read()
    s = re.search(user+"\|((\S)+?)\|", result)
    if s:
        account = s.group(1)
        print "Using SLURM account \"" + account + "\"..."
        return account
    return None




  
    
