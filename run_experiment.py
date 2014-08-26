# -*- coding: utf-8 -*- 

import sys
from mosespy.experiment import Experiment

trainData = "./data/news-commentary/news-commentary-v8.fr-en"
tuningData = "./data/newstest/news-test2008"
testData = "./data/newstest/newstest2011"

exp = Experiment("first_test", "fr", "en")
exp.trainLanguageModel(trainData+".en")
exp.trainTranslationModel(trainData)
exp.tuneTranslationModel(tuningData)
exp.evaluateBLEU(testData)

