
import os, re, uuid, threading, time
import utils, experiment, moses_parallel
from experiment import Experiment 
  
nodeMemory=62000
nodeCpus = 16
nodeTime = "8:00:00"

class SlurmExecutor(utils.CommandExecutor):
        
    def __init__(self, account=None):
        if not account:
            account = getDefaultSlurmAccount()
        self.account = account
        os.environ["LD_LIBRARY_PATH"] = (os.popen("module load intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + os.popen("module load openmpi.intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                                         + "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        os.environ["PATH"] = "/opt/rocks/bin:" + os.popen("module load openmpi.intel ; echo $PATH").read().strip('\n')

        
    def run(self, script, stdin=None, stdout=None):
        
        name = str(uuid.uuid4())[0:5]
        srun = ("srun --account=" + self.account
                + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                +" --job-name=" + name
                + " --cpus-per-task=" + str(nodeCpus)
                + " --time=" + nodeTime)
        
        cmd = srun + " " + script 
        return super(SlurmExecutor,self).run(cmd, stdin, stdout)
       
        
    def runs(self, scripts, stdins=None, stdouts=None):
        jobnames = []
        i = 0
        for script in scripts:
            name = str(uuid.uuid4())[0:5]
            srun_cmd = ("srun --account=" + self.account
                        + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                        +"  --job-name=" + name
                        + " --cpus-per-task=" + str(nodeCpus)
                        + " --time=" + nodeTime
                        + " " + script )
            stdin = stdins[i] if isinstance(stdins, list) else None
            stdout = stdouts[i] if isinstance(stdouts, list) else None
    
            t = threading.Thread(target=super(SlurmExecutor,self).run, 
                                 args=(srun_cmd, stdin, stdout))
            t.start()
            jobnames.append(name)
            i += 1
            
        time.sleep(1)
        while True:
            queue = os.popen("squeue -u " + os.popen("whoami").read()).read()
            if len(set(queue.split()).intersection(jobnames)) == 0:
                break
            print "Unfinished jobs: " + str(set(queue.split()).intersection(jobnames))
            time.sleep(60)
        print "SLURM parallel run completed."
  
        
class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, 
                 account=None, nbJobs=4):
        Experiment.__init__(self, expName, sourceLang, targetLang)
  
        if not utils.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
    
        self.executor = SlurmExecutor(account)
        self.nbJobs = nbJobs
        self.decoder = str(moses_parallel.__file__).replace("pyc", "py")

        
    def trainTranslationModel(self, trainStem, preprocess=True,
                              alignment=experiment.defaultAlignment, 
                              reordering=experiment.defaultReordering):
        
        if self.nbJobs == 1:
            Experiment.trainTranslationModel(self, trainStem, nodeCpus)
            return
             
        if preprocess:         
            trainStem = self.processAlignedData(trainStem)["clean"]
        
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " +  trainStem
               + " with " + str(self.nbJobs) + " splits")
    
        splitDir = self.settings["path"] + "/splits"
        utils.resetDir(splitDir)
        utils.splitData(trainStem + "." + self.settings["source"], splitDir, self.nbJobs)
        utils.splitData(trainStem + "." + self.settings["target"], splitDir, self.nbJobs)

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self.getTrainScript(tmDir, trainStem, nodeCpus, alignment, reordering)
        scripts = []
        for i in range(0, self.nbJobs):
            scripts.append((tmScript.replace(tmDir, splitDir + "/" + str(i))\
                                .replace(trainStem, splitDir + "/" +str(i))
                                + " --last-step 3"))
        self.executor.runs(scripts)
        
        utils.resetDir(tmDir)
        os.makedirs(tmDir+"/model")
        alignFile = tmDir+"/model/aligned."+alignment
        with open(alignFile, 'w') as align:
            for split in range(0, self.nbJobs):
                splitFile = splitDir+ "/" + str(split)+"/model/aligned."+alignment
                with open(splitFile) as part:
                    for partline in part.readlines():
                        if partline.strip():
                            align.write(partline.strip('\n') + '\n')
                            
        tmScript +=  (" -sort-buffer-size " + str(nodeMemory/4) + "M " 
                      + "-sort-batch-size 1024 " 
                    + " -sort-compress gzip -sort-parallel " + str(nodeCpus))              
        result = self.executor.run(tmScript + " --first-step 4")
        utils.rmDir(splitDir)

        if result:
            print "Finished building translation model in: " + utils.getsize(tmDir)
            self.settings["tm"]=tmDir
            self.recordState()
        else:
            print "Construction of translation model FAILED"
    
                
    def tokeniseFile(self, inputFile, outputFile):
        Experiment.tokeniseFile(self, inputFile, outputFile, nbThreads=nodeCpus)
          

    def getTuningScript(self, tuneDir, tuningStem, nbThreads):
        script = super(SlurmExperiment, self).getTuningScript(tuneDir, tuningStem, nodeCpus)
        script = script.replace("--decoder-flags=\'", 
                                "--decoder-flags=\'-njobs " + str(self.nbJobs) + " ")
        return script


    def translate(self, text, preprocess=True, customModel=None, nbThreads=2):
        return super(SlurmExperiment, self).translate(text, preprocess, customModel, nodeCpus)
    
    
    def translateFile(self, infile, outfile, preprocess=True, customModel=None, nbThreads=2):
        return super(SlurmExperiment, self).translateFile(infile, outfile, preprocess, 
                                                          customModel, nodeCpus)
    


def getDefaultSlurmAccount():
    user = (os.popen("whoami")).read().strip()
    result = (os.popen("sacctmgr show User "+user + " -p")).read()
    s = re.search(user+"\|((\S)+?)\|", result)
    if s:
        account = s.group(1)
        print "Using SLURM account \"" + account + "\"..."
        return account
    raise RuntimeError("cannot find default SLURM account")




  
    
