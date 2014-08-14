
import os, re, uuid, threading, copy
import utils, experiment
from experiment import Experiment 
from utils import Path
  
nodeMemory=62000
nodeCpus = 16
nodeTime = "4:00:00"

class SlurmExecutor(utils.CommandExecutor):
        
    def __init__(self, account=None):
        if not account:
            account = _getDefaultSlurmAccount()
        if not account:
            print "Warning: no SLURM account found, switching to normal execution"
        self.account = account
        os.environ["LD_LIBRARY_PATH"] = (os.popen("module load intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + os.popen("module load openmpi.intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                                         + "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        os.environ["PATH"] = "/opt/rocks/bin:" + os.popen("module load openmpi.intel ; echo $PATH").read().strip('\n')

    
    def _getScript(self, script, allowForks):    
        if self.account and (allowForks or "SLURM" not in str(os.environ.keys())):
            name = str(uuid.uuid4())[0:5]
            script = ("srun --account=" + self.account
                      + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                      +" --job-name=" + name
                      + " --cpus-per-task=" + str(nodeCpus)
                      + " --time=" + nodeTime 
                      + " " + script)  
            for k in list(os.environ):
                if "SLURM" in k and k in os.environ.keys():
                    del os.environ[k]     
        return script
        
    
    def run(self, script, stdin=None, stdout=None, allowForks=False):
        script = self._getScript(script, allowForks)  
        return super(SlurmExecutor,self).run(script, stdin, stdout)
    
              
  
        
class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None, nbJobs=4):
        
        Experiment.__init__(self, expName, sourceLang, targetLang)
  
        if not utils.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
    
        self.settings["account"] = account
        self.settings["nbjobs"] = nbJobs
        self.executor = SlurmExecutor(account)

    
    def copy(self, nexExpName):
        newexp = SlurmExperiment(nexExpName, self.settings["source"], self.settings["target"], self.settings["account"])
        settingscopy = copy.deepcopy(self.settings)
        for k in settingscopy.keys():
            if k != "name" and k!= "path":
                newexp.settings[k] = settingscopy[k]
        newexp._recordState()
        return newexp
    
    
    def trainTranslationModel(self, trainStem, preprocess=True,
                              alignment=experiment.defaultAlignment, 
                              reordering=experiment.defaultReordering):
        
        if self.settings["nbjobs"] == 1:
            Experiment.trainTranslationModel(self, trainStem, nodeCpus)
            return
             
        if preprocess:         
            trainStem = self._processAlignedData(trainStem)["clean"]
        
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " +  trainStem
               + " with " + str(self.settings["nbjobs"]) + " splits")
    
        splitDir = self.settings["path"] + "/splits"
        splitDir.reset()
        utils.splitData(trainStem + "." + self.settings["source"], splitDir, self.settings["nbjobs"])
        utils.splitData(trainStem + "." + self.settings["target"], splitDir, self.settings["nbjobs"])

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self._getTrainScript(tmDir, trainStem, nodeCpus, alignment, reordering)
       
        jobs = []
        for i in range(0, self.settings["nbjobs"]):
            script = (tmScript.replace(tmDir, splitDir + "/" + str(i))\
                                .replace(trainStem, splitDir + "/" +str(i))
                                + " --last-step 3")
            t = threading.Thread(target=self.executor.run, args=(script, None, None, True))
            jobs.append(t)
            t.start()
        utils.waitForCompletion(jobs)
         
        tmDir.reset()
        Path(tmDir+"/model").make()
        alignFile = tmDir+"/model/aligned."+alignment
        with open(alignFile, 'w') as align:
            for split in range(0, self.settings["nbjobs"]):
                splitFile = splitDir+ "/" + str(split)+"/model/aligned."+alignment
                with open(splitFile) as part:
                    for partline in part.readlines():
                        if partline.strip():
                            align.write(partline.strip('\n') + '\n')
                            
        tmScript +=  (" -sort-buffer-size " + str(nodeMemory/4) + "M " 
                      + "-sort-batch-size 1024 " 
                    + " -sort-compress gzip -sort-parallel " + str(nodeCpus))              
        result = self.executor.run(tmScript + " --first-step 4")
        splitDir.remove()

        if result:
            print "Finished building translation model in: " + tmDir.getDescription()
            self.settings["tm"]=tmDir
            self._prunePhraseTable()
            self._recordState()
        else:
            print "Construction of translation model FAILED"
 
    
                
    def tokeniseFile(self, inputFile, outputFile):
        Experiment._tokeniseFile(self, inputFile, outputFile, nbThreads=nodeCpus)
       
    
    def tuneTranslationModel(self, tuningStem, preprocess=True, nbThreads=nodeCpus):
        Experiment.tuneTranslationModel(self, tuningStem, preprocess, nbThreads)
        if self.settings.has_key("ttm"):
            with open(self.settings["ttm"] + "/moses.ini", 'r') as iniFile:
                config = iniFile.read()
            with open(self.settings["ttm"] + "/moses.ini", 'w') as iniFile:
                iniFile.write(re.sub("\[jobs\]\n(\d)+", "", config))


    def _getNbDecodingJobs(self, sourceFile):
        nblines = sourceFile.countNbLines()
        return min(self.settings["nbjobs"], max(1,nblines/1000))


    def getTuningScript(self, tuneDir, tuningStem, nbThreads):
        nbDecodingJobs = self._getNbDecodingJobs(tuningStem + "." + self.settings["source"])
        tuneScript = (experiment.moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + tuningStem + "." + self.settings["source"] + " " 
                      + tuningStem + "." + self.settings["target"] + " "
                      + experiment.rootDir + "/mosespy/moses_parallel.py "
                      + self.settings["tm"] + "/model/moses.ini " 
                      + " --mertdir " + experiment.moses_root + "/bin/"
                      + " --batch-mira "
                      + " --decoder-flags=\'-jobs %i -threads %i -v 0' "
                      + " --working-dir " + tuneDir
                      )%(nbDecodingJobs, nbThreads)
        return tuneScript


    def translate(self, text, preprocess=True, nbThreads=nodeCpus):
        return Experiment.translate(self, text, preprocess, nbThreads)
    
    
    def translateFile(self, infile, outfile, preprocess=True, filterModel=False, nbThreads=nodeCpus):
        return Experiment.translateFile(self, infile, outfile, preprocess, 
                                        filterModel, nbThreads)
        
 
    def _getTranslateScript(self, initFile, nbThreads, inputFile=None):
        script = (experiment.rootDir + "/mosespy/moses_parallel.py -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
            nbJobs = self._getNbDecodingJobs(inputFile)
            script += " -jobs " + str(nbJobs)
        return script  


def _getDefaultSlurmAccount():
    user = (os.popen("whoami")).read().strip()
    result = (os.popen("sacctmgr show User "+user + " -p")).read()
    s = re.search(user+"\|((\S)+?)\|", result)
    if s:
        account = s.group(1)
        return account
    return None




  
    