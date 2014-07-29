# -*- coding: utf-8 -*- 

import sys
from mosespy import slurmutils, shellutils
from mosespy.slurmutils import SlurmExperiment
from mosespy.mosespy import Experiment

shellutils.initialCmds = ("module load intel ; module load openmpi.intel ; " 
                          +  "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:" 
                          + "/cluster/home/plison/libs/boost_1_55_0/lib64" 
                          + ":/cluster/home/plison/libs/gperftools-2.2.1/lib/ ;" 
                          + "export PATH=/opt/rocks/bin:$PATH")

if "--batch" in sys.argv:
    slurmutils.sbatch(__file__, nbTasks=32)
    exit()

trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/newstest/newssyscomb2009"

exp = SlurmExperiment("32-tasks", "fr", "en")
exp.trainLanguageModel(trainData+".en")
exp.trainTranslationModel(trainData, nbSplits=4)
exp.tuneTranslationModel(tuningData)
exp.binariseModel()
print exp.translate("Faire revenir les militants sur le terrain et convaincre que le vote est utile.")
