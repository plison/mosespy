# -*- coding: utf-8 -*-

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

import re, uuid, copy
import mosespy.experiment as experiment
import mosespy.system as system
from mosespy.experiment import Experiment 
from mosespy.corpus import AlignedCorpus, CorpusProcessor
from mosespy.system import CommandExecutor, Path

nodeMemory=60000
nodeCpus = 16
nodeTime = "5:00:00"

class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None, maxJobs=4):
        
        Experiment.__init__(self, expName, sourceLang, targetLang)
        self.maxJobs = maxJobs
  
        if not system.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
        
        self.executor = SlurmExecutor(account)
        self.nbThreads = nodeCpus
        self.processor = CorpusProcessor(self.expPath, self.executor, nodeCpus)
        self.decoder = Path(__file__).getUp().getAbsolute() + "/moses_parallel.py"
        
    
    def copy(self, nexExpName):
        newexp = SlurmExperiment(nexExpName, self.sourceLang, 
                                 self.targetLang, self.executor.account, self.maxJobs)
        newexp.lm = self.lm
        newexp.tm = self.tm
        newexp.ngram_order = self.ngram_order
        newexp.iniFile = self.iniFile
        newexp.sourceLang = self.sourceLang
        newexp.targetLang = self.targetLang
        newexp.test = self.test
        newexp.maxJobs = self.maxJobs
        return newexp
    
    
    def trainTranslationModel(self, trainStem, alignment=experiment.defaultAlignment, 
                              reordering=experiment.defaultReordering, preprocess=True, 
                              pruning=True):
        
        if self.maxJobs == 1:
            return Experiment.trainTranslationModel(self, trainStem, alignment, reordering, 
                                                    preprocess, pruning)
             
        train = AlignedCorpus(trainStem, self.sourceLang, self.targetLang)
        if preprocess:         
            train = self.processor.processCorpus(train)
        
        print ("Building translation model " + self.sourceLang + "-" 
               + self.targetLang + " with " +  train.getStem()
               + " with " + str(self.maxJobs/2) + " splits")
    
        splitDir = self.expPath + "/splits"
        splitDir.reset()
        
        splitStems = CorpusProcessor(splitDir, self.executor, nodeCpus).splitData(train, self.maxJobs/2)
        print "Split data: " + str(splitStems)
        tmDir = self.expPath + "/translationmodel"
        tmScript = self._getTrainScript(tmDir, train.getStem(), alignment, reordering)
           
        slotScript = tmScript.replace(tmDir, "%s").replace(train.getStem(), "%s") + " %s"
        jobArgs1 = [(stem, stem, " --last-step 1") for stem in splitStems]
        r1 = self.executor.run_parallel(slotScript, jobArgs1)
        if not r1:
            raise RuntimeError("Construction of translation model FAILED (step 1)")
  
        jobArgs2 = [(stem, stem, " --first-step 2 --last-step 2 --direction 1") for stem in splitStems]
        jobArgs3 = [(stem, stem, " --first-step 2 --last-step 2 --direction 2") for stem in splitStems]
        r2 = self.executor.run_parallel(slotScript, jobArgs2 + jobArgs3)
        if not r2:
            raise RuntimeError("Construction of translation model FAILED (step 2)")

        jobArgs4 = [(stem, stem, " --first-step 3 --last-step 3") for stem in splitStems]
        r3 = self.executor.run_parallel(slotScript, jobArgs4)
        if not r3:
            raise RuntimeError("Construction of translation model FAILED (step 3)")
                 
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
        #              + "-sort-batch-size 1024 " 
                    + " -sort-compress gzip -sort-parallel " + str(nodeCpus))              
        result = self.executor.run(tmScript + " --first-step 4")
        
        if result:
            print "Finished building translation model in: " + tmDir.getDescription()
            self.tm=tmDir + "/model"
            self.iniFile = self.tm + "/moses.ini"
            if pruning:
                self.prunePhraseTable()
            self._recordState()
        else:
            raise RuntimeError("Construction of translation model FAILED (step 4)")
 


class SlurmExecutor(CommandExecutor):
        
    def __init__(self, account=None):
        
        CommandExecutor.__init__(self)
        self.account = _getDefaultSlurmAccount() if not account else account
        if not self.account:
            print "Warning: cannot use SLURM bindings"
            return
        correctSlurmEnv()
        

    def run(self, script, stdin=None, stdout=None):
        if not "SLURM" in str(system.getEnv().keys()) and self.account:
            name = str(uuid.uuid4())[0:5]
            script = ("srun --account=" + self.account
                      + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                      +" --job-name=" + name
                      + " --cpus-per-task=" + str(nodeCpus)
                      + " --time=" + nodeTime
                      + " " + script) 
        return CommandExecutor.run(self, script, stdin, stdout)
    
    
    def run_parallel(self, script, jobArgs, stdins=None, stdouts=None):
        if len(jobArgs) == 1:
            stdin = stdins[0] if isinstance(stdins,list) else None
            stdout = stdouts[0] if isinstance(stdouts,list) else None
            result = self.run(script%(jobArgs[0]), stdin, stdout) 
            return [result] if stdouts else result
        for k in system.getEnv():
            if "SLURM" in k:
                system.delEnv(k)
        return CommandExecutor.run_parallel(self, script, jobArgs, stdins, stdouts)



def correctSlurmEnv():
    # System-dependent settings for the Abel cluster, change it to suit your needs
    if system.existsExecutable("srun"):
        modScript = "module load intel ; echo $LD_LIBRARY_PATH"
        system.setEnv("LD_LIBRARY_PATH", system.run_output(modScript) + ":"
                      + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                      +   "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        system.setEnv("PATH", "/opt/rocks/bin", override=False)
               
       
def _getDefaultSlurmAccount():
    if system.existsExecutable("sacctmgr"):
        user = system.run_output("whoami")
        result = system.run_output("sacctmgr show User "+user + " -p")
        s = re.search(user+r"\|((\S)+?)\|", result)
        if s:
            account = s.group(1)
            return account
    return None




  
    
