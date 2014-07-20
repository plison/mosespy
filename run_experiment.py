# -*- coding: utf-8 -*- 

import sys, os, time
from mosespy.mosespy import Experiment
from mosespy import slurmutils

if "-batch" in sys.argv or "--batch" in sys.argv:
    slurmutils.run_batch(os.path.basename(__file__))
    exit()

trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/news-dev/newssyscomb2009"

exp = Experiment("exp-parallel", "fr", "en")
exp.trainLanguageModel(trainData+".en")
exp.trainTranslationModel(trainData)


#exp.tuneTranslationModel(tuningData)
#exp.translate("faire revenir les militants sur le terrain et convaincre que le vote est utile .")