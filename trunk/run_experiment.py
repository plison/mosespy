# -*- coding: utf-8 -*- 

import sys
from mosespy.slurm import SlurmExperiment
from mosespy.experiment import Experiment
import mosespy.experiment

trainData = "./data/news-commentary/news-commentary-v8.fr-en"

exp = Experiment("test", "fr", "en")
print exp.analyseShortWords("true.en", "actual.en")

#exp.doWholeShibang(trainData)

#exp2 = exp.copy("opensub2")
#exp.trainLanguageModel(lmData)
#exp2.trainTranslationModel(trainData)
#exp.tuneTranslationModel(tuningData)
#exp.binariseModel()
#print exp2.translate("Faire revenir les militants sur le terrain et convaincre que le vote est utile.")
#exp.evaluateBLEU(testData)
