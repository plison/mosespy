#!/usr/bin/env python

import sys, utils,os, uuid, slurm 

def main():      
    rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    moses_root = rootDir + "/moses" 
        
    nbJobs = 4
    arguments = []
    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        if "-jobs" in sys.argv[i-1]:
            nbJobs = int(arg)
        elif not "-jobs" in arg:
            arguments.append(arg)
    
    arguments = " ".join(arguments)
    sys.stderr.write("Running moses with %i jobs and following arguments: %s"%(nbJobs, arguments))
    
    transScript = moses_root + "/bin/moses " + arguments
    if "-show-weights" not in arguments:
        splitDir = "./tmp" + str(uuid.uuid4())[0:5]
        utils.resetDir(splitDir)
        
        infiles = utils.splitData(sys.stdin, splitDir, nbJobs)
        
        outfiles = [splitDir + "/" + str(i) + ".translated" for i in range(0, len(infiles))]
                
        slurm.SlurmExecutor().runs([transScript]*len(infiles), infiles, outfiles)
            
        for outfile_part in outfiles:
            with open(outfile_part, 'r') as part:
                for partline in part.readlines():
                    if partline.strip():
                        sys.stdout.write(partline.strip('\n') + '\n')
        utils.rmDir(splitDir)
    else:
        slurm.SlurmExecutor.run(transScript)


if __name__ == "__main__":
    main()
