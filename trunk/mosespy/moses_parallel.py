#!/usr/bin/env python

import sys, utils,os, uuid, slurm, select

def main():      

    rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    moses_root = rootDir + "/moses" 

    nbJobs = 4
    arguments = []
    lines = []
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
    
    arguments = " ".join(arguments)
    sys.stderr.write("Running moses with following arguments: " + str(arguments)+"\n")
    
    while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
        line = sys.stdin.readline()
        if line.strip():
            lines.append(line)
        else:
            break
    
    transScript = moses_root + "/bin/moses " + arguments
    
    executor = slurm.SlurmExecutor()
 
    for k in os.environ:
        if "SLURM" in k:
            del os.environ[k] 

    if not lines:
        sys.stderr.write("(no input provided)\n")
        executor.run(transScript)
    else:
        sys.stderr.write("Number of input lines: " + str(len(lines))+"\n")
        sys.stderr.write("Splitting data into %i jobs"%(nbJobs)+"\n")
        splitDir = "./tmp" + str(uuid.uuid4())[0:5]
        utils.resetDir(splitDir)
        
        infiles = utils.splitData(lines, splitDir, nbJobs)
        
        outfiles = [splitDir + "/" + str(i) + ".translated" for i in range(0, len(infiles))]
        executor.runs([transScript]*len(infiles), infiles, outfiles)
            
        for outfile_part in outfiles:
            with open(outfile_part, 'r') as part:
                for partline in part.readlines():
                    if partline.strip():
                        sys.stdout.write(partline.strip('\n') + '\n')
        utils.rmDir(splitDir)


if __name__ == "__main__":
    main()
