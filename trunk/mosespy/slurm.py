
import os, re, uuid, threading, time, sys, copy
import utils, experiment
from experiment import Experiment 
  
nodeMemory=62000
nodeCpus = 16
nodeTime = "6:00:00"

class SlurmExecutor(utils.CommandExecutor):
        
    def __init__(self, account=None):
        if not account:
            account = getDefaultSlurmAccount()
        if not account:
            raise RuntimeError("cannot find default SLURM account")
        self.account = account
        os.environ["LD_LIBRARY_PATH"] = (os.popen("module load intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + os.popen("module load openmpi.intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                                         + "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        os.environ["PATH"] = "/opt/rocks/bin:" + os.popen("module load openmpi.intel ; echo $PATH").read().strip('\n')

    
    def getScript(self, script):
        name = str(uuid.uuid4())[0:5]
        script = ("srun --account=" + self.account
                + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                +" --job-name=" + name
                + " --cpus-per-task=" + str(nodeCpus)
                + " --time=" + nodeTime 
                + " " + script)
        return script
        
    
    def run(self, script, stdin=None, stdout=None):  
        return super(SlurmExecutor,self).run(self.getScript(script), stdin, stdout)
    
            
    def runs(self, scripts, stdins=None, stdouts=None):
        
        jobs = []  
        for script in scripts:
            script = self.getScript(script)
            stdin = stdins[len(jobs)] if isinstance(stdins, list) else None
            stdout = stdouts[len(jobs)] if isinstance(stdouts, list) else None
    
            t = threading.Thread(target=super(SlurmExecutor,self).run, 
                                 args=(script, stdin, stdout))
            jobs.append(t)
            t.start()
            
        utils.waitForCompletion(jobs)
  
        
class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, 
                 account=None, nbJobs=4):
        Experiment.__init__(self, expName, sourceLang, targetLang)
  
        if not utils.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
    
        self.settings["account"] = account
        self.settings["nbJobs"] = nbJobs
        self.executor = SlurmExecutor(account)
        self.decoder = experiment.rootDir + "/mosespy/moses_parallel.py"

    
    def copy(self, nexExpName):
        newexp = SlurmExperiment(nexExpName, self.settings["source"], self.settings["target"], self.settings["account"])
        settingscopy = copy.deepcopy(self.settings)
        for k in settingscopy.keys():
            if k != "name" and k!= "path":
                newexp.settings[k] = settingscopy[k]
        newexp.recordState()
        newexp.decoder = self.decoder
        return newexp
    
    
    def trainTranslationModel(self, trainStem, preprocess=True,
                              alignment=experiment.defaultAlignment, 
                              reordering=experiment.defaultReordering):
        
        if self.settings["nbJobs"] == 1:
            Experiment.trainTranslationModel(self, trainStem, nodeCpus)
            return
             
        if preprocess:         
            trainStem = self.processAlignedData(trainStem)["clean"]
        
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " +  trainStem
               + " with " + str(self.settings["nbJobs"]) + " splits")
    
        splitDir = self.settings["path"] + "/splits"
        utils.resetDir(splitDir)
        utils.splitData(trainStem + "." + self.settings["source"], splitDir, self.settings["nbJobs"])
        utils.splitData(trainStem + "." + self.settings["target"], splitDir, self.settings["nbJobs"])

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self.getTrainScript(tmDir, trainStem, nodeCpus, alignment, reordering)
        scripts = []
        for i in range(0, self.settings["nbJobs"]):
            scripts.append((tmScript.replace(tmDir, splitDir + "/" + str(i))\
                                .replace(trainStem, splitDir + "/" +str(i))
                                + " --last-step 3"))
        self.executor.runs(scripts)
        
        utils.resetDir(tmDir)
        os.makedirs(tmDir+"/model")
        alignFile = tmDir+"/model/aligned."+alignment
        with open(alignFile, 'w') as align:
            for split in range(0, self.settings["nbJobs"]):
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
       
    
    def tuneTranslationModel(self, tuningStem, preprocess=True, nbThreads=nodeCpus):
        Experiment.tuneTranslationModel(self, tuningStem, preprocess, nbThreads)
        with open(self.settings["ttm"] + "/moses.ini", 'r') as iniFile:
            config = iniFile.read()
        print config
        with open(self.settings["ttm"] + "/moses.ini", 'w') as iniFile:
            iniFile.write(config.replace("[jobs]\n"+str(self.settings["nbJobs"]), ""))
        
    

    def getTuningScript(self, tuneDir, tuningStem, nbThreads):
        script = Experiment.getTuningScript(self, tuneDir, tuningStem, nodeCpus)
        return script.replace("--decoder-flags=\'", 
                              "--decoder-flags=\'-jobs " + str(self.settings["nbJobs"]) + " ")


    def translate(self, text, preprocess=True, customModel=None, nbThreads=2):
        return Experiment.translate(self, text, preprocess, customModel, nodeCpus)
    
    
    def translateFile(self, infile, outfile, preprocess=True, customModel=None, nbThreads=2):
        return Experiment.translateFile(self, infile, outfile, preprocess, 
                                        customModel, nodeCpus)
    


def getDefaultSlurmAccount():
    user = (os.popen("whoami")).read().strip()
    result = (os.popen("sacctmgr show User "+user + " -p")).read()
    s = re.search(user+"\|((\S)+?)\|", result)
    if s:
        account = s.group(1)
        return account
    return None




  
    
