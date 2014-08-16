#!/usr/bin/env python

import sys, os, uuid, select, threading
import process, slurm, corpus
from paths import Path

moses_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/moses" 
decoder = moses_root + "/bin/moses "

executor = slurm.SlurmExecutor()

def getInput():
    lines = []
    for i in range(1, len(sys.argv)):
        if "-input-file" in sys.argv[i-1]:
            return Path(sys.argv[i].strip())
       
    while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
        line = sys.stdin.readline()
        if line.strip():
            lines.append(line)
        else:
            break  
          
    if len(lines) > 0:
        print "Number of input lines: " + str(len(lines))
        tmpInputFile = "./tmp" + str(uuid.uuid4())[0:6] + ".source"
        with open(tmpInputFile, 'w') as tmpInput:
            tmpInput.writelines(lines)
        return Path(tmpInputFile)
    else:
        return None


def getNbJobs():
    nbJobs = 4
    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        arg = arg if " " not in arg else "\'" + arg + "\'"
        if "-jobs" in sys.argv[i-1]:
            nbJobs = int(arg)
    return nbJobs


def getMosesArguments():
    argsToRemove = ["-jobs", "-input-file"]
    arguments = []
    for i in range(1, len(sys.argv)):
        curArg = sys.argv[i].strip()
        curArg = curArg if " " not in curArg else "\'" + curArg + "\'"
        prevArg = sys.argv[i-1].strip()
        if not curArg in argsToRemove and not prevArg in argsToRemove:
            arguments.append(curArg)
    
    arguments = " ".join(arguments)
    return arguments



def mergeOutFiles(outfiles, outStream):
    for outfile_part in outfiles:
        with open(outfile_part, 'r') as part:
            for partline in part.readlines():
                if partline.strip():
                    outStream.write(partline.strip('\n') + '\n')
    outStream.close()
       
       
def getArgumentValue(args, key):
    split = args.split()
    for i in range(0, len(split)):
        if i > 0 and key == split[i-1].strip():
            return split[i].strip()
    return None

def mergeNbestOutFiles(nbestOutPartFiles, nbestOutFile):
    print "Merging nbest files " + str(nbestOutPartFiles) + " into " + str(nbestOutFile) 
    localCount = 0
    globalCount = 0
    with open(nbestOutFile, 'w') as nbestout_full:
        for nbestOutPartFile in nbestOutPartFiles:
            with open(nbestOutPartFile, 'r') as nbestout_part:
                for partline in nbestout_part.readlines():
                    if partline.strip():
                        newCount = int(partline.split(" ")[0])
                        if newCount == localCount + 1:
                            localCount += 1
                            globalCount += 1
                        partline = partline.replace(str(newCount), str(globalCount), 1).strip("\n")+"\n"
                        nbestout_full.write(partline)
            localCount = 0
            globalCount += 1
   

def splitDecoding(inputFile, mosesArgs, nbJobs):
    splitDir = Path("./tmp" + str(uuid.uuid4())[0:6])
    splitDir.reset()
    infiles = corpus.splitData(inputFile, splitDir, nbJobs)  
    print "Data split in " + str(len(infiles))
    
    splits = []
    for i in range(0, len(infiles)):
        infile = Path(infiles[i])
        outfile = Path(splitDir + "/" + str(i) + ".translated")
            
        newArgs = str(mosesArgs)
        nbestout = getArgumentValue(mosesArgs, "-n-best-list")
        if nbestout:
            newArgs = newArgs.replace(nbestout, splitDir + "/" + str(i) + ".nbest" )    
        splits.append({"in": infile, "out":outfile, "args":newArgs})
    return splits
    
        
        
def runParallelMoses(inputFile, mosesArgs, outStream, nbJobs):
                
    if not inputFile:
        print "Running decoder: " + decoder + mosesArgs
        executor.run(decoder + mosesArgs, stdout=outStream)
        
    elif nbJobs == 1 or os.path.getsize(inputFile) < 1000:
        print "Running decoder: " + decoder + mosesArgs + " < " + inputFile
        executor.run(decoder + mosesArgs, stdin=inputFile, stdout=outStream)
    else:
        
        splits = splitDecoding(inputFile, mosesArgs, nbJobs)
        executor.allowForks(True)
        for split in splits:
            threadArgs = (split["in"], split["args"], split["out"], 1)
            t = threading.Thread(target=runParallelMoses, args=threadArgs)
            t.start()
            split["thread"] = t
            
        process.waitForCompletion([split["thread"] for split in splits])
        executor.allowForks(False)
        mergeOutFiles([split["out"] for split in splits], outStream)
        
        if "-n-best-list" in mosesArgs:
            mergeNbestOutFiles([getArgumentValue(split["args"], "-n-best-list") for split in splits], 
                               getArgumentValue(mosesArgs, "-n-best-list"))
     
        splits[0]["in"].getUp().remove()
                         

def main():      
   
    stdout = sys.stdout
    sys.stdout = sys.stderr

    nbJobs = getNbJobs()
    arguments = getMosesArguments()
    inputFile = getInput()
    
    runParallelMoses(inputFile, arguments, stdout, nbJobs)
    
    if inputFile and "tmp" in inputFile:
        os.remove(inputFile)



if __name__ == "__main__":
    main()
