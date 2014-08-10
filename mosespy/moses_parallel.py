#!/usr/bin/env python

import sys, utils,os, uuid, slurm 
nbSplits = 4

rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
moses_root = rootDir + "/moses" 

arguments = " ".join(sys.argv[1:])
print "Arguments: " + arguments

splitDir = "./tmp" + str(uuid.uuid4())[0:5]
utils.resetDir(splitDir)

infiles = utils.splitData(sys.stdin, splitDir, nbSplits)

outfiles = [splitDir + "/" + str(i) + ".translated" for i in range(0, len(infiles))]
        
transScript = moses_root + "/bin/moses " + arguments
slurm.SlurmExecutor().runs([transScript]*len(infiles), infiles, outfiles)
    
for outfile_part in outfiles:
    with open(outfile_part, 'r') as part:
        for partline in part.readlines():
            if partline.strip():
                sys.stdout.write(partline.strip('\n') + '\n')
utils.rmDir(splitDir)
