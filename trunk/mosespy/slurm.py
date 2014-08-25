# -*- coding: utf-8 -*-

"""The slurm module provides functionalities to make use of
high-performance, SLURM-based computer clusters to train, tune
and evaluate machine translation models.

The central element of this module is the SlurmExperiment class
which extends the Experiment class (in module experiment) in 
order to run processes through SLURM instead of on the shell.

"""

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

import re, uuid, copy
import mosespy.system as system
from mosespy.experiment import Experiment 
from mosespy.corpus import CorpusProcessor
from mosespy.system import ShellExecutor, Path

# Total memory per node
nodeMemory=60000

# Number of CPUs per node
nodeCpus = 16

# Walltime for each command
nodeTime = "5:00:00"

class SlurmExperiment(Experiment):
    """Extension of the Experiment class (in module experiment) to run processes 
    through SLURM commands instead of on the shell. Training and decoding can also 
    make use of multiple parallel jobs to speed up the experiments. 
    
    """
            
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None, 
                 maxJobs=4):
        """Creates a new experiment with the given name.  If an experiment of 
        same name already exists, its state is reloaded (based on the JSON
        file that records the experiment state). 
        
        Args: 
            sourceLang (str): language code for the source language
            targetLang (str): language code for the target language
            account (string): SLURM account
            maxJobs (int): maximum number of SLURM jobs to run in parallel
            
        """
        self.executor = SlurmExecutor(account)
        Experiment.__init__(self, expName, sourceLang, targetLang)
        self.maxJobs = maxJobs
  
        if not system.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
        
        self.nbThreads = nodeCpus
        self.processor = CorpusProcessor(self.expPath, self.executor, nodeCpus)
        self.decoder = Path(__file__).getUp().getAbsolute() + "/moses_parallel.py"
        
    
    def copy(self, nexExpName):
        """Copies the experiment with another name.
        
        """
        newexp = SlurmExperiment(nexExpName, self.sourceLang, 
                                 self.targetLang, self.executor.account, self.maxJobs)
        newexp.lm = self.lm
        newexp.tm = self.tm
        newexp.ngram_order = self.ngram_order
        newexp.iniFile = self.iniFile
        newexp.sourceLang = self.sourceLang
        newexp.targetLang = self.targetLang
        newexp.results = self.results
        newexp.maxJobs = self.maxJobs
        return newexp
         
    
    def _constructTranslationModel(self, trainCorpus, alignment, reordering):
        """Constructs the translation model in a distributed fashion, by splitting
        the training data into chunks that are aligned and processed independently
        (from step 1 to step 3). The method returns the directory containing the 
        resulting model data.
        
        The method should not be called directly, please use trainTranslationModel(...) 
        instead.
        
        """     
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
        (tmDir+"/model").reset()
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

 


class SlurmExecutor(ShellExecutor):
    """Executor of commands through SLURM calls. The class extends the
    ShellExecutor to run command through 'srun'. 
    
    """
        
    def __init__(self, account=None):
        """Creates a new executor.  If no account if provided, the method
        tries to extract the default account.
        
        """        
        ShellExecutor.__init__(self)
        self.account = _getDefaultSlurmAccount() if not account else account
        if not self.account:
            print "Warning: cannot use SLURM bindings"
            return
        correctSlurmEnv()
        

    def run(self, script, stdin=None, stdout=None):
        """Runs the script through 'srun', if the current process is not
        already running through SLURM.  Else, simply executes a shell call.
        
        Args:
            script (str): the command to execute
            stdin: the standard input, which can be either a file,
                a text input, or nothing (None).
            stdout: the standard output, which can be either a file, 
                nothing (None), or the boolean 'True' (in which case 
                the output is returned by the method).
        
        """
        if not "SLURM" in str(system.getEnv().keys()) and self.account:
            name = str(uuid.uuid4())[0:5]
            script = ("srun --account=" + self.account
                      + " --mem-per-cpu=" + str(nodeMemory/nodeCpus) + "M"
                      +" --job-name=" + name
                      + " --cpus-per-task=" + str(nodeCpus)
                      + " --time=" + nodeTime
                      + " " + script) 
        return ShellExecutor.run(self, script, stdin, stdout)
    
    
    def run_parallel(self, scripts, stdins=None, stdouts=None):
        """Runs a set of scripts in parallel through 'srun', each script
        being run on a separate node.
        
        Args:
            scripts (list): the commands to execute
            stdin: the standard inputs, which can a list of files, a list
                of text inputs, or nothing (None).
            stdout: the standard output, which can a list of files, nothing 
                (None) or the boolean True, in which case the outputs are 
                returned by the method.
        
        """
        if len(scripts) == 1:
            stdin = stdins[0] if isinstance(stdins,list) else stdins
            stdout = stdouts[0] if isinstance(stdouts,list) else stdouts
            result = self.run(scripts[0], stdin, stdout) 
            return [result] if stdouts else result
        
        currentEnv = copy.deepcopy(system.getEnv())
        for k in system.getEnv():
            if "SLURM" in k:
                system.delEnv(k)
        result = ShellExecutor.run_parallel(self, scripts, stdins, stdouts)
        for k in currentEnv:
            system.setEnv(k, currentEnv[k])
        return result

               
       
def _getDefaultSlurmAccount():
    """Returns the default Slurm account for the current user.
    
    """
    if system.existsExecutable("sacctmgr"):
        user = system.run_output("whoami")
        result = system.run_output("sacctmgr show User "+user + " -p")
        s = re.search(user+r"\|((\S)+?)\|", result)
        if s:
            account = s.group(1)
            return account
    return None




def correctSlurmEnv():
    """Corrects the environment variables for the Abel cluster.
    
    """
    # System-dependent settings for the Abel cluster, change it to suit your needs
    if system.existsExecutable("srun"):
        modScript = "module load intel ; echo $LD_LIBRARY_PATH"
        system.setEnv("LD_LIBRARY_PATH", system.run_output(modScript) + ":"
                      + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                      +   "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        system.setEnv("PATH", "/opt/rocks/bin", override=False)


  
    
