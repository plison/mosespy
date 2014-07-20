# -*- coding: utf-8 -*- 

import sys
from mosespy import slurmutils
from mosespy.slurmutils import SlurmExperiment
from mosespy.mosespy import Experiment


if "--batch" in sys.argv:
    slurmutils.sbatch(__file__)


trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/news-dev/newssyscomb2009"

exp = SlurmExperiment("exp-parallel", "en", "fr")
#exp.trainLanguageModel(trainData+".en")
exp.trainTranslationModel(nbSplits=4)


#exp.tuneTranslationModel(tuningData)
#exp.translate("faire revenir les militants sur le terrain et convaincre que le vote est utile .")