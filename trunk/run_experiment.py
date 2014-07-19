# -*- coding: utf-8 -*- 

import sys, os
from mosespy.mosespy import Experiment
from mosespy import syscalls

if "-batch" in sys.argv or "--batch" in sys.argv:
    syscalls.run_batch(os.path.basename(__file__))
    exit()

trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/news-dev/newssyscomb2009"

exp = Experiment("exp-parallel", "fr", "en")
exp.trainLanguageModel(trainData+".en")
exp.trainTranslationModel(trainData)


#exp.tuneTranslationModel(tuningData)
#exp.translate("faire revenir les militants sur le terrain et convaincre que le vote est utile .")