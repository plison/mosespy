# -*- coding: utf-8 -*- 

import sys
from mosespy import slurmutils
from mosespy.slurmutils import SlurmExperiment
from mosespy.mosespy import Experiment

slurmutils.initialCmds = ("module load intel ; module load openmpi.intel ; " 
                          +  "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:" 
                          + "/cluster/home/plison/libs/boost_1_55_0/lib64" 
                          + ":/cluster/home/plison/libs/gperftools-2.2.1/lib/")

if "--batch" in sys.argv:
    slurmutils.sbatch(__file__, nbTasks=10)
    exit()

trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/newstest/newssyscomb2009"

exp = SlurmExperiment("thirdexp", "en", "fr")
#exp.trainLanguageModel(trainData+".en")
#exp.trainTranslationModel(trainData, nbSplits=4)
exp.tuneTranslationModel(tuningData)

#exp.tuneTranslationModel(tuningData)
#exp.translate("faire revenir les militants sur le terrain et convaincre que le vote est utile .")