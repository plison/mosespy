
import time, shutil, os, shellutils, textwrap, re, sys
import mosespy
from mosespy import Experiment 
     
initialCmds = ""
     
class SlurmExperiment(Experiment):
    
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None):
        Experiment.__init__(self, expName, sourceLang, targetLang)
        if not shellutils.existsExecutable("sbatch"):
            print "SLURM system not present, some methods might be unavailable"
        elif not account:
            account = getDefaultSlurmAccount()
        if account:
            self.system["slurm_account"] = account
    
  
    def trainTranslationModel(self, trainStem=None, nbSplits=1, nbThreads=16):
        
        if nbSplits == 1:
            Experiment.trainTranslationModel(self, trainStem, nbThreads)
            return
        
        if not self.system.has_key("slurm_account"):
            print "SLURM system not present, cannot split model training"
            Experiment.trainTranslationModel(self, trainStem, nbThreads)
            return            
       
        if trainStem:         
            trainData = self.processAlignedData(trainStem)
            self.system["tm"] = {"data": trainData}
            self.recordState()        
        elif not self.system.has_key("tm") or not self.system["tm"].has_key("data"):
            raise RuntimeError("Aligned training data is not yet processed")    
        
        trainData = self.system["tm"]["data"]
        print ("Building translation model " + self.system["source"] + "-" 
               + self.system["target"] + " with " + trainData["clean"] 
               + " with " + str(nbSplits) + " splits")

        tmScript, tmDir = self.getTrainScript(nbThreads)
    
        outputDir = os.path.dirname(tmDir) + "/splits"
        
        
        shutil.rmtree(tmDir, ignore_errors=True)   
        os.makedirs(tmDir+"/model")
        with open(tmDir+"/model/aligned."+self.system["alignment"], 'w') as al:
            for split in range(0, nbSplits):
                with open(outputDir+ "/" + str(split)+"/model/aligned."+self.system["alignment"]) as part:
                    for partline in part.readlines():
                        if partline.strip():
                            al.write(partline)
                            if '\n' not in partline:
                                al.write('\n')
            
        result = shellutils.run(tmScript + " --first-step 4")

        if result:
            print "Finished building translation model in directory " + mosespy.getFileDescription(tmDir)
            self.system["tm"]["dir"]=tmDir
            self.recordState()
        else:
            print "Construction of translation model FAILED"
            shellutils.run("rm -rf " + tmDir)


 
    def translate(self, text):
        if self.system.has_key("btm"):
            initFile = self.system["btm"]["dir"] + "/moses.ini"
        elif self.system.has_key("ttm"):
            print "Warning: translation model is not yet binarised"
            initFile = self.system["ttm"]["dir"] + "/moses.ini"
        elif self.system.has_key("tm"):
            print "Warning: translation model is not yet tuned!"
            initFile = self.system["tm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print text
        transScript = u'echo \"%r\" | mpirun ./moses/bin/moses -f %s'%(text,initFile)
        result = shellutils.run(transScript, return_output=True)
        print result
        return result
        


def arrayrun(paramScript, account, nbSplits):
    
    batchFile = createBatchFile(paramScript, account, name="split-$TASK_ID", nbTasks=1)

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
        time.sleep(60)
    print "SLURM array run completed."
    for job in jobs:
        shutil.move("slurm-"+job+".out", "logs/slurm-"+job+".out")


def sbatch(pythonFile, account=None, nbTasks=1, memoryGb=60):

    print "Starting " + pythonFile + " using sbatch"
        
    if not account:
        account = getDefaultSlurmAccount()
    if not account:
        print "could not identify SLURM account for user, switching back to normal mode"
        shellutils.run("python -u " + pythonFile)
        
    if not os.path.exists(pythonFile):
        raise RuntimeError(pythonFile + " must be a python file") 
           
    batchFile = createBatchFile("python -u " + pythonFile, account, nbTasks=nbTasks, 
                                name=pythonFile, memoryGb=memoryGb)
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

  
     

   
def createBatchFile(script, account, time="5:00:00", nbTasks=1, memoryGb=60, name=None):
      
    if not name:
        name = script.split(' ')[0].split("/")[len(script.split(' ')[0].split("/"))-1]
    #script = script + (" < " + infile if infile else "")  + (" > " + outfile if outfile else "")
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
                            %s 
                            """%(name, account, time, nbTasks, memoryStr, 
                                 shellutils.initialCmds, script))   
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




  
    
