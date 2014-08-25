# -*- coding: utf-8 -*-

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

import re, uuid, copy
import mosespy.system as system
from mosespy.experiment import Experiment 
from mosespy.corpus import CorpusProcessor
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
         
    
    def _constructTranslationModel(self, trainCorpus, alignment, reordering):
        
        splitDir = self.expPath + "/splits"
        splitDir.reset()
        
        splitStems = self.processor.splitData(trainCorpus, self.maxJobs/2, splitDir)
        tmDir = self.expPath + "/translationmodel"
           
        scripts1 = [self._getTrainScript(stem, stem, alignment, reordering, 1, 1) 
                    for stem in splitStems]
        r1 = self.executor.run_parallel(scripts1)
        if not r1:
            raise RuntimeError("Construction of translation model FAILED (step 1)")

        scripts2 = [self._getTrainScript(stem, stem, alignment, reordering, 2, 2, direct) 
                    for stem in splitStems for direct in [1,2]]
        r2 = self.executor.run_parallel(scripts2)
        if not r2:
            raise RuntimeError("Construction of translation model FAILED (step 2)")

        scripts3 = [self._getTrainScript(stem, stem, alignment, reordering, 3, 3)
                    for stem in splitStems]
        r3 = self.executor.run_parallel(scripts3)
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
                    
        script4 = self._getTrainScript(tmDir, trainCorpus.getStem(), alignment, reordering, 4) 
        r4 = self.executor.run(script4)
        if not r4:
            raise RuntimeError("Construction of translation model FAILED (step 4)")
        return tmDir

 


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
    
    
    def run_parallel(self, scripts, stdins=None, stdouts=None):
        if len(scripts) == 1:
            stdin = stdins[0] if isinstance(stdins,list) else None
            stdout = stdouts[0] if isinstance(stdouts,list) else None
            result = self.run(scripts[0], stdin, stdout) 
            return [result] if stdouts else result
        
        currentEnv = copy.deepcopy(system.getEnv())
        for k in system.getEnv():
            if "SLURM" in k:
                system.delEnv(k)
        result = CommandExecutor.run_parallel(self, scripts, stdins, stdouts)
        for k in currentEnv:
            system.setEnv(k, currentEnv[k])
        return result



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




  
    
