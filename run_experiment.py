# -*- coding: utf-8 -*- 

import sys
from mosespy.slurm import SlurmExperiment
from mosespy.moseswrapper import Experiment

trainData = "./data/news-commentary/news-commentary-v8.fr-en";
tuningData = "./data/newstest/newssyscomb2009"
testData = "./data/newstest/newstest2011"

exp = SlurmExperiment("basic", "fr", "en")
#exp.trainLanguageModel(trainData+".en")
#exp.trainTranslationModel(trainData, nbSplits=4)
#exp.tuneTranslationModel(tuningData)
#exp.binariseModel()
#print exp.translate("Faire revenir les militants sur le terrain et convaincre que le vote est utile.")
exp.evaluateBLEU(testData)
