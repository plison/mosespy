# -*- coding: utf-8 -*- 

import sys
from mosespy import slurmutils, shellutils
from mosespy.slurmutils import SlurmExperiment
from mosespy.mosespy import Experiment

trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/newstest/newssyscomb2009"

exp = SlurmExperiment("basic", "fr", "en")
exp.trainLanguageModel(trainData+".en")
exp.trainTranslationModel(trainData, nbSplits=4)
exp.tuneTranslationModel(tuningData)
exp.binariseModel()
print exp.translate("Faire revenir les militants sur le terrain et convaincre que le vote est utile.")
