# -*- coding: utf-8 -*- 

import sys
from mosespy.slurm import SlurmExperiment
from mosespy.moseswrapper import Experiment

lmData = "./data/opensubtitles/OpenSubtitles2013.en"
trainData = "./data/opensubtitles/OpenSubtitles2013.en-fr.train"
tuningData ="./data/opensubtitles/OpenSubtitles2013.en-fr.tune"
testData = "./data/opensubtitles/OpenSubtitles2013.en-fr.test"

exp = SlurmExperiment("opensub1", "fr", "en")
exp.trainLanguageModel(lmData)
exp.trainTranslationModel(trainData, nbSplits=8)
exp.tuneTranslationModel(tuningData)
#exp.binariseModel()
print exp.translate("Faire revenir les militants sur le terrain et convaincre que le vote est utile.")
exp.evaluateBLEU(testData)
