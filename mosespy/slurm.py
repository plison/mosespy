
import re, uuid, threading, copy
import experiment, system
from experiment import Experiment 
from mosespy.processing import CorpusProcessor
from corpus import AlignedCorpus
from system import CommandExecutor
from config import MosesConfig

nodeMemory=60000
nodeCpus = 16
nodeTime = "5:00:00"

class SlurmExecutor(CommandExecutor):
        
    def __init__(self, account=None):
        CommandExecutor.__init__(self)
        if not account:
            account = _getDefaultSlurmAccount()
        if not account:
            print "Warning: no SLURM account found, switching to normal execution"
        self.account = account
        
        # System-dependent settings for the Abel cluster, change it to suit your needs
        modScript = "module load intel openmpi.intel ; echo $LD_LIBRARY_PATH"
        system.setEnv("LD_LIBRARY_PATH", system.run_output(modScript) + ":"
                       + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                   +   "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        system.setEnv("PATH", "/opt/rocks/bin", override=False)
        

    def _getScript(self, script):    
        if self.account:
            name = str(uuid.uuid4())[0:5]
            script = ("srun --account=" + self.account
                      + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                      +" --job-name=" + name
                      + " --cpus-per-task=" + str(nodeCpus)
                      + " --time=" + nodeTime
                      + " --propagate=NONE " 
                      + " " + script)  
        return script
        
    
    def run(self, script, stdin=None, stdout=None):
        if not "SLURM" in str(system.getEnv().keys()):
            script = self._getScript(script)  
        return super(SlurmExecutor,self).run(script, stdin, stdout)
    
    
    def run_parallel(self, script, jobArgs, stdins=None, stdouts=None):  
        jobs = []
        for i in range(0, len(jobArgs)):
            jobArg = jobArgs[i]
            filledScript = script%(jobArg)
            stdin = stdins[i] if stdins else None
            stdout = stdouts[i] if stdouts else None
            t = threading.Thread(target=self.run, args=(filledScript, stdin, stdout))
            jobs.append(t)
            t.start()
        system.waitForCompletion(jobs)
    
               
        
class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None, maxJobs=4):
        
        Experiment.__init__(self, expName, sourceLang, targetLang)
  
        if not system.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
    
        self.settings["account"] = account
        self.maxJobs = maxJobs
        self.executor = SlurmExecutor(account)
        self.processor = CorpusProcessor(self.settings["path"], self.executor, nodeCpus)
        
    
    def copy(self, nexExpName):
        newexp = SlurmExperiment(nexExpName, self.settings["source"], self.settings["target"], self.settings["account"])
        settingscopy = copy.deepcopy(self.settings)
        for k in settingscopy.keys():
            if k != "name" and k!= "path":
                newexp.settings[k] = settingscopy[k]
        return newexp
    
    
    def trainTranslationModel(self, trainStem, preprocess=True,alignment=experiment.defaultAlignment, 
                              reordering=experiment.defaultReordering, nbThreads=nodeCpus):
        
        if self.maxJobs == 1:
            Experiment.trainTranslationModel(self, trainStem, nbThreads)
            return
             
        train = AlignedCorpus(trainStem, self.settings["source"], self.settings["target"])
        if preprocess:         
            train = self.processor.processCorpus(train)
        
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " +  train.getStem()
               + " with " + str(self.maxJobs) + " splits")
    
        splitDir = self.settings["path"] + "/splits"
        splitStems = train.splitData(splitDir, self.maxJobs/2)
 
        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self._getTrainScript(tmDir, train.getStem(), nbThreads, alignment, reordering)
           
        slotScript = tmScript.replace(tmDir, "%s").replace(train.getStem(), "%s") + " %s"
  
        jobArgs = [(stem, stem, " --first-step 3 --last-step 3") for stem in splitStems]
        self.executor.run_parallel(slotScript, jobArgs)
         
        tmDir.reset()
        (tmDir+"/model").make()
        alignFile = tmDir+"/model/aligned."+alignment
        with open(alignFile, 'w') as align:
            for split in range(0, self.maxJobs/2):
                splitFile = splitDir+ "/" + str(split)+"/model/aligned."+alignment
                with open(splitFile) as part:
                    for partline in part.readlines():
                        if partline.strip():
                            align.write(partline.strip('\n') + '\n')
        splitDir.remove()
                            
        tmScript +=  (" -sort-buffer-size " + str(nodeMemory/4) + "M " 
                      + "-sort-batch-size 1024 " 
                    + " -sort-compress gzip -sort-parallel " + str(nodeCpus))              
        result = self.executor.run(tmScript + " --first-step 4")
        
        if result:
            print "Finished building translation model in: " + tmDir.getDescription()
            self.settings["tm"]=tmDir
       #     self._prunePhraseTable()
            self._recordState()
        else:
            print "Construction of translation model FAILED"
 
      
    def tuneTranslationModel(self, tuningStem, preprocess=True, nbThreads=nodeCpus):
        Experiment.tuneTranslationModel(self, tuningStem, preprocess, nbThreads)
        config = MosesConfig(self.settings["ttm"]+"/moses.ini")
        config.removePart("jobs")


    def _getTuningScript(self, tuneDir, tuningStem, nbThreads):
        nbDecodingJobs = self._getNbDecodingJobs(tuningStem + "." + self.settings["source"])
        tuneScript = (experiment.moses_root + "/scripts/training/mert-moses.pl" + " " 
                      + tuningStem + "." + self.settings["source"] + " " 
                      + tuningStem + "." + self.settings["target"] + " "
                      + experiment.rootDir + "/mosespy/moses_parallel.py "
                      + self.settings["tm"] + "/model/moses.ini " 
                      + " --mertdir " + experiment.moses_root + "/bin/"
                      + " --decoder-flags=\'-jobs %i -threads %i -v 0' "
                      + " --working-dir " + tuneDir
                      )%(nbDecodingJobs, nbThreads)
        return tuneScript


    def translate(self, text, preprocess=True, nbThreads=nodeCpus):
        return Experiment.translate(self, text, preprocess, nbThreads)
    
    
    def translateFile(self, infile, outfile, preprocess=True, filterModel=False, nbThreads=nodeCpus):
        return Experiment.translateFile(self, infile, outfile, preprocess, 
                                        filterModel, nbThreads)
        
 
    def _getNbDecodingJobs(self, sourceFile):
        nblines = sourceFile.countNbLines()
        return min(self.maxJobs, max(1,nblines/1000))


    def _getTranslateScript(self, initFile, nbThreads, inputFile=None):
        script = (experiment.rootDir + "/mosespy/moses_parallel.py -f " + initFile.encode('utf-8') 
                + " -v 0 -threads " + str(nbThreads))
        if inputFile:
            script += " -input-file "+ inputFile
            maxJobs = self._getNbDecodingJobs(inputFile)
            script += " -jobs " + str(maxJobs)
        return script  


def _getDefaultSlurmAccount():
    user = system.run_output("whoami")
    result = system.run_output("sacctmgr show User "+user + " -p")
    s = re.search(user+r"\|((\S)+?)\|", result)
    if s:
        account = s.group(1)
        return account
    return None




  
    
