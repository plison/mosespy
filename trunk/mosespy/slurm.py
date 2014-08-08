
import time, os, shellutils, textwrap, re, uuid
from mosespy import moseswrapper
from mosespy.moseswrapper import Experiment 
  
  
decoder = moseswrapper.moses_root + "/bin/moses -f"

class SlurmExecutor(shellutils.CommandExecutor):
        
    def __init__(self, account, time="6:00:00", memory=3936, nbThreads=16):
        self.account = account
        self.time = time
        self.memory = memory
        self.nbThreads = nbThreads
        
    def run(self, script, stdin=None, stdout=None):
        
        name = str(uuid.uuid4())[0:5]
        srun = ("srun --account=" + self.account
                + " --mem-per-cpu=" + str(self.memory) + "M"
                +" --job-name=" + name
                + " --cpus-per-task=" + str(self.nbThreads)
                + " --time=" + self.time)
        
        cmd = srun + " " + script 
        return super(SlurmExecutor,self).run(cmd, stdin, stdout)
       
        
    def runs(self, scripts, stdins=None, stdouts=None):
        jobnames = []
        i = 0
        print stdins
        print stdouts
        for script in scripts:
            name = str(uuid.uuid4())[0:5]
            srun_cmd = ("srun --account=" + self.account
                        + " --mem-per-cpu=" + str(self.memory) + "M"
                        +"  --job-name=" + name
                        + " --cpus-per-task=" + str(self.nbThreads)
                        + " --time=" + self.time
                        + " " + script + " &")
            stdin = stdins[i] if isinstance(stdins, list) else None
            stdout = stdouts[i] if isinstance(stdouts, list) else None
            
            super(SlurmExecutor,self).run(srun_cmd, stdin=stdin, stdout=stdout)
            jobnames.append(name)
            i += 1
            
        time.sleep(1)
        while True:
            queue = os.popen("squeue -u " + os.popen("whoami").read()).read()
            if len(set(queue.split()).intersection(jobnames)) == 0:
                break
            print "Unfinished jobs: " + str(list(jobnames))
            time.sleep(60)
        print "SLURM parallel run completed."
  
        
class SlurmExperiment(Experiment):
            
    def __init__(self, expName, sourceLang=None, targetLang=None, account=None):
        Experiment.__init__(self, expName, sourceLang, targetLang)
  
        if not shellutils.existsExecutable("srun"):
            print "SLURM system not present, switching back to standard setup"
            return
        elif not account:
            account = getDefaultSlurmAccount()
        if account:
            self.settings["slurm_account"] = account
      
        os.environ["LD_LIBRARY_PATH"] = (os.popen("module load intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + os.popen("module load openmpi.intel ; echo $LD_LIBRARY_PATH")
                                         .read().strip('\n') + ":"
                                         + "/cluster/home/plison/libs/boost_1_55_0/lib64:" 
                                         + "/cluster/home/plison/libs/gperftools-2.2.1/lib/")
        os.environ["PATH"] = "/opt/rocks/bin:" + os.popen("module load openmpi.intel ; echo $PATH").read().strip('\n')
        self.executor = SlurmExecutor(account)

        
    def trainTranslationModel(self, trainStem=None, nbSplits=1, nbThreads=16, 
                              alignment=moseswrapper.defaultAlignment, 
                              reordering=moseswrapper.defaultReordering):
        
        if nbSplits == 1:
            Experiment.trainTranslationModel(self, trainStem, nbThreads)
            return
        
        if not self.settings.has_key("slurm_account"):
            raise RuntimeError("SLURM system not present, cannot split model training")
       
        if trainStem:         
            trainData = self.processAlignedData(trainStem)
            self.settings["tm"] = {"data": trainData}
            self.recordState()        
        elif not self.settings.has_key("tm") or not self.settings["tm"].has_key("data"):
            raise RuntimeError("Aligned training data is not yet processed")  
        
        cleanData = self.settings["tm"]["data"]["clean"]         
        print ("Building translation model " + self.settings["source"] + "-" 
               + self.settings["target"] + " with " +  cleanData
               + " with " + str(nbSplits) + " splits")
    
        splitDir = self.settings["path"] + "/splits"
        shellutils.resetDir(splitDir)
        splitData(cleanData + "." + self.settings["source"], splitDir, nbSplits)
        splitData(cleanData + "." + self.settings["target"], splitDir, nbSplits)

        tmDir = self.settings["path"] + "/translationmodel"
        tmScript = self.getTrainScript(tmDir, nbThreads, alignment, reordering)
        scripts = []
        for i in range(0, nbSplits):
            scripts.append((tmScript(tmDir, splitDir + "/" + str(i))\
                                .replace(cleanData, splitDir + "/" +str(i))
                                + " --last-step 3"))
        self.executor.runs(scripts)
        
        shellutils.resetDir(tmDir)
        os.makedirs(tmDir+"/model")
        alignFile = tmDir+"/model/aligned."+alignment
        with open(alignFile, 'w') as align:
            for split in range(0, nbSplits):
                splitFile = splitDir+ "/" + str(split)+"/model/aligned."+alignment
                with open(splitFile) as part:
                    for partline in part.readlines():
                        if partline.strip():
                            align.write(partline.strip('\n') + '\n')
                            
        tmScript +=  (" -sort-buffer-size 10G -sort-batch-size 1024 " 
                    + " -sort-compress gzip -sort-parallel " + nbThreads)              
        result = self.executor.run(tmScript + " --first-step 4")
        shellutils.rmDir(splitDir)

        if result:
            print "Finished building translation model in: " + shellutils.getsize(tmDir)
            self.settings["tm"]["dir"]=tmDir
            self.recordState()
        else:
            self.executor.run("rm -rf " + tmDir)
            
            
    def tokeniseFile(self, inputFile, outputFile, nbThreads=16):
        Experiment.tokeniseFile(self, inputFile, outputFile, nbThreads=nbThreads)
          
      
    def translateFile(self, infile, outfile, preprocess=True, customModel=None, nbSplits=4):
        if customModel:
            if not os.path.exists(customModel+"/moses.ini"):
                raise RuntimeError("Custom model " + customModel + " does not exist")
            initFile = customModel+"/moses.ini"
        elif self.settings.has_key("btm"):
            initFile = self.settings["btm"]["dir"] + "/moses.ini"
        elif self.settings.has_key("ttm"):
            print "Warning: translation model is not yet binarised"
            initFile = self.settings["ttm"]["dir"] + "/moses.ini"
        else:
            raise RuntimeError("Translation model is not yet trained!")
        print ("Translating file \"" + infile + "\" from " + 
               self.settings["source"] + " to " + self.settings["target"])

        if preprocess:
            infile = self.processRawData(infile)["true"]
        
        splitDir = self.settings["path"] + "/splits-"+os.path.basename(infile)
        shellutils.resetDir(splitDir)
        infiles = splitData(infile, splitDir, nbSplits)
        outfiles = [splitDir + "/" + str(i) + "." + self.settings["target"] 
                    for i in range(0, nbSplits)]
        
        transScript = (moseswrapper.moses_root + "/bin/moses" + " -f " + initFile.encode('utf-8'))
        self.executor.runs([transScript]*nbSplits, infiles, outfiles)
     
        with open(outfile, 'w') as out:
            for outfile_part in outfiles:
                with open(outfile_part, 'r') as part:
                    for partline in part.readlines():
                        if partline.strip():
                            out.write(partline.strip('\n') + '\n')
 



def splitData(dataFile, outputDir, nbSplits):
        
    extension = dataFile.split(".")[len(dataFile.split("."))-1]
    totalLines = int(os.popen("wc -l " + dataFile).read().strip().split(" ")[0])

    filenames = []
    with open(dataFile) as fullFile:
        curSplit = 0
        filename = outputDir + "/" + str(curSplit) + "." + extension
        curFile = open(filename, 'w')
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
        filenames.append(filename)
    return filenames



def getDefaultSlurmAccount():
    user = (os.popen("whoami")).read().strip()
    result = (os.popen("sacctmgr show User "+user + " -p")).read()
    s = re.search(user+"\|((\S)+?)\|", result)
    if s:
        account = s.group(1)
        print "Using SLURM account \"" + account + "\"..."
        return account
    return None




  
    
