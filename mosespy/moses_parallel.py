#!/usr/bin/env python

import sys, utils,os, uuid, slurm, select

def main():      

    rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    moses_root = rootDir + "/moses" 
     
    stdout = sys.stdout
    sys.stdout = sys.stderr

    nbJobs = 4
    arguments = []
    lines = []
    
    nbestout = None
    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        arg = arg if " " not in arg else "\'" + arg + "\'"
        if "-jobs" in sys.argv[i-1]:
            nbJobs = int(arg)
        elif "-input-file" in sys.argv[i-1]:
            f = open(arg, 'r')
            for line in f.readlines():
                if line.strip():
                    lines.append(line)
        elif not "-jobs" in arg and not "-input-file" in arg:
            arguments.append(arg)
            
        if "-n-best-list" in sys.argv[i-1]:
            nbestout = arg
    
    arguments = " ".join(arguments)
    print "Running moses with following arguments: " + str(arguments)
    
    while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
        line = sys.stdin.readline()
        if line.strip():
            lines.append(line)
        else:
            break
    
    transScript = moses_root + "/bin/moses " + arguments
    executor = slurm.SlurmExecutor()
    
    for k in list(os.environ):
        if "SLURM" in k:
            del os.environ[k] 

    if not lines:
        executor.run(transScript)
    elif nbJobs == 1:
        tmpInputFile = "./tmp" + str(uuid.uuid4())[0:5]
        with open(tmpInputFile, 'w') as tmpInput:
            for line in lines:
                tmpInput.write(line.strip("\n") + "\n")
        executor.run(transScript, tmpInputFile)
    else:
        print "Number of input lines: " + str(len(lines))
        print "Splitting data into %i jobs"%(nbJobs)
        splitDir = "./tmp" + str(uuid.uuid4())[0:5]
        utils.resetDir(splitDir)
        
        infiles = utils.splitData(lines, splitDir, nbJobs)
        
        outfiles = [splitDir + "/" + str(i) + ".translated" for i in range(0, len(infiles))]
        
        transScripts = []
        for i in range(0, len(infiles)):
            if nbestout:    
                nbestout2 = splitDir + "/" + str(i) + ".nbest" 
                transScripts.append(transScript.replace(nbestout, nbestout2))
            else:
                transScripts.append(transScript)
                
        executor.runs(transScripts, infiles, outfiles)
            
        for outfile_part in outfiles:
            with open(outfile_part, 'r') as part:
                for partline in part.readlines():
                    if partline.strip():
                        stdout.write(partline.strip('\n') + '\n')
        stdout.close()
        
        localCount = 0
        globalCount = 0
        if nbestout:
            with open(nbestout, 'w') as nbestout_full:
                for i in range(0, len(infiles)):
                    with open(splitDir + "/" + str(i) + ".nbest", 'r') as nbestout_part:
                        for bestline in nbestout_part.readlines():
                            if bestline.strip():
                                newCount = int(bestline.split(" ")[0])
                                if newCount == localCount + 1:
                                    localCount += 1
                                    globalCount += 1
                                bestline = bestline.replace(newCount, globalCount, 1).strip("\n")+"\n"
                                nbestout_full.write(bestline)
                    localCount = 0
                    globalCount += 1
        utils.rmDir(splitDir)
                    
           


if __name__ == "__main__":
    main()
