#!/usr/bin/env python

import sys, utils,os, uuid, slurm, select, threading, time


moses_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/moses" 

def getInput():
    lines = []
    for i in range(1, len(sys.argv)):
        if "-input-file" in sys.argv[i-1]:
            return sys.argv[i].strip()
       
    while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
        line = sys.stdin.readline()
        if line.strip():
            print "New line: " + line
            lines.append(line)
        else:
            break  
          
    if len(lines) > 0:
        print "Number of input lines: " + str(len(lines))
        tmpInputFile = "./tmp" + str(uuid.uuid4())[0:6] + ".source"
        with open(tmpInputFile, 'w') as tmpInput:
            tmpInput.writelines(lines)
        return tmpInputFile
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
    argumentsToAvoid = ["-jobs", "-input-file"]
    arguments = []
    for i in range(1, len(sys.argv)):
        curArg = sys.argv[i].strip()
        curArg = curArg if " " not in curArg else "\'" + curArg + "\'"
        prevArg = sys.argv[i-1].strip()
        if not curArg in argumentsToAvoid and not prevArg in argumentsToAvoid:
            arguments.append(curArg)
    
    arguments = " ".join(arguments)
    print "Running moses with following arguments: " + str(arguments)
    return arguments


def getNbestOut():
    nbestout = None
    for i in range(1, len(sys.argv)):
        if "-n-best-list" in sys.argv[i-1]:
            nbestout = sys.argv[i]
    return nbestout
    

def mergeOutFiles(outfiles, outStream):
    for outfile_part in outfiles:
            with open(outfile_part, 'r') as part:
                for partline in part.readlines():
                    if partline.strip():
                        outStream.write(partline.strip('\n') + '\n')
    outStream.close()
    

def mergeNbestOutFiles(nbestOutPartFiles, nbestOutFile):
           
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
        
        
def runParallelMoses(inputFile, basicArgs, outStream, nbestOutFile, nbJobs):
    
    try:
        executor = slurm.SlurmExecutor()
    except RuntimeError:
        executor = utils.CommandExecutor()
        
    command = moses_root + "/bin/moses " + basicArgs
    command += (" -n-best-list " + nbestOutFile) if nbestOutFile else ""
    
    for k in list(os.environ):
        if "SLURM" in k:
            del os.environ[k] 

    if not inputFile:
        executor.run(command, stdout=outStream)
        
    elif nbJobs == 1:
        executor.run(command, stdin=inputFile, stdout=outStream)
    else:
        print "Splitting data into %i jobs"%(nbJobs)
        splitDir = "./tmp" + str(uuid.uuid4())[0:6]
        utils.resetDir(splitDir)
        
        infiles = utils.splitData(inputFile, splitDir, nbJobs)  
        jobs = {}     
        for i in range(0, len(infiles)):
            infile = infiles[i]
            outfile = splitDir + "/" + str(i) + ".translated"
            nbestOutFile2 = splitDir + "/" + str(i) + ".nbest" if nbestOutFile else None
            t = threading.Thread(target=runParallelMoses, args=(infile, basicArgs, outfile, nbestOutFile2, 1))
            t.start()
            jobs[t.ident] = {"thread":t, "in":infile, "out":outfile, "nbestout":nbestOutFile2}
            
        utils.waitForCompletion([jobs[k]["thread"] for k in jobs])
        mergeOutFiles([jobs[k]["out"] for k in jobs], outStream)
        if nbestOutFile:
            mergeNbestOutFiles([jobs[k]["nbestout"] for k in jobs], nbestOutFile)
     
        utils.rmDir(splitDir)
    
    
                    
           

def main():      
   
    stdout = sys.stdout
    sys.stdout = sys.stderr

    nbJobs = getNbJobs()
    arguments = getMosesArguments()
    inputFile = getInput()
    
    runParallelMoses(inputFile, arguments, stdout, getNbestOut(), nbJobs)
    
    if inputFile and "tmp" in inputFile:
        os.remove(inputFile)



if __name__ == "__main__":
    main()
