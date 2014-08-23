# -*- coding: utf-8 -*- 

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"

import sys
import unittest
import uuid
import os
import shutil 
import mosespy.experiment as experiment
import mosespy.slurm as slurm
import mosespy.system as system
from mosespy.system import Path, CommandExecutor
from mosespy.corpus import BasicCorpus, AlignedCorpus, CorpusProcessor
from mosespy.experiment import Experiment, MosesConfig
from mosespy.slurm import SlurmExperiment
import mosespy.datadivision as datadivision

slurm.correctSlurmEnv()

class Pipeline(unittest.TestCase):

    def setUp(self):
        self.tmpdir = Path(__file__).getUp().getUp() + "/tmp" + str(uuid.uuid4())[0:8]
        os.makedirs(self.tmpdir)
        self.inFile = (Path(__file__).getUp().getUp()+"/data/tests/subtitles.fr").copy(self.tmpdir)
        self.outFile = (Path(__file__).getUp().getUp()+"/data/tests/subtitles.en").copy(self.tmpdir)
        self.duplicatesFile = (Path(__file__).getUp().getUp()+"/data/tests/withduplicates.txt").copy(self.tmpdir)
        
    def preprocessing(self):
        
        processor = CorpusProcessor(self.tmpdir, CommandExecutor())
        trueFile = processor.processFile(self.inFile)
        trueLines = trueFile.readlines()
        self.assertIn("bien occupé .\n", trueLines)
        self.assertIn("comment va Janet ?\n", trueLines)
        self.assertIn("non , j&apos; ai complètement compris .\n", trueLines)
        self.assertTrue(processor.truecaser.isModelTrained("fr"))
        self.assertFalse(processor.truecaser.isModelTrained("en"))
        self.assertIn(self.inFile.basename().addProperty("true"), os.listdir(self.tmpdir))
        
        revertFile = processor.revertFile(trueFile)
        revertLines = revertFile.readlines()
        self.assertIn("non, j'ai complètement compris.\n", revertLines)
        
        corpus2 = processor.processCorpus(AlignedCorpus(self.inFile.getStem(), "fr", "en"))
        self.assertIsInstance(corpus2, AlignedCorpus)
        self.assertEqual(corpus2.getStem(), self.tmpdir + "/" + self.inFile.basename().getStem().addProperty("clean"))
        self.assertIn("non , j&apos; ai complètement compris .\n", corpus2.getSourceFile().readlines())
        self.assertEqual(corpus2.sourceLang, "fr")
        self.assertEqual(corpus2.targetLang, "en")
        self.assertIn(self.inFile.basename().addProperty("clean"), os.listdir(self.tmpdir))
        self.assertIn(self.outFile.basename().addProperty("clean"), os.listdir(self.tmpdir))
        self.assertEquals(len(os.listdir(self.tmpdir)), 8)


    def division(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        _, _, _, test = datadivision.divideData(acorpus, 10, 0, 10)
        self.assertTrue(os.path.exists(test.getStem()+".indices"))
        testlines = (test.getStem()+".en").readlines()
        indlines = (test.getStem()+".indices").readlines()
        self.assertEquals(indlines[0].strip(), self.inFile.getStem())
        targetlines = self.outFile.readlines()
        for i in range(0, len(testlines)):
            testLine = testlines[i]
            originIndex = int(indlines[i+1].strip())
            correspondingLine = targetlines[originIndex]
            self.assertEquals(testLine, correspondingLine)
        
        occurrences = test.getTargetCorpus().getOccurrences()
        histories = test.getTargetCorpus().getHistories()
        self.assertGreater(len(occurrences), 8)

        targetlines = [line.strip() for line in targetlines]
        testlines = [line.strip() for line in testlines]
        
        for i in range(0, len(testlines)):
            testLine = testlines[i]
            occ = occurrences[testLine]
            self.assertGreaterEqual(len(occ), 1)
            self.assertIn(i, occ)
            oindices = [k for k, x in enumerate(targetlines) if x == testLine]
            self.assertEquals(testLine, targetlines[oindices[0]])
            self.assertTrue(any([histories[i]==targetlines[max(0,q-2):q] for q in oindices]))
        
        newFile = datadivision.filterOutLines( BasicCorpus(self.outFile), test.getTargetCorpus())
        newlines = Path(newFile).readlines()
        intersect = set(testlines).intersection(set(newlines))
        self.assertTrue(all([targetlines.count(i) > 1 for i in intersect]))
        
        alignments = test.getAlignments(addHistory=True)
        self.assertEqual(len(alignments), 10)
        for i in range(0, len(testlines)):
            align = alignments[i]
            self.assertEqual(align["target"],testlines[i].strip())
            oindices = [k for k, x in enumerate(targetlines) if x == testlines[i]]
            self.assertTrue(any([not q or align["previoustarget"]==targetlines[q-1] for q in oindices]))
        
    
    def split(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        splitStems = CorpusProcessor(self.tmpdir).splitData(acorpus, 2)
        self.assertTrue(Path(self.tmpdir + "/0.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/1.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/0.en").exists())
        self.assertTrue(Path(self.tmpdir + "/1.en").exists())
        self.assertEquals(len(Path(self.tmpdir + "/0.fr").readlines()), 50)
        self.assertEquals(len(Path(self.tmpdir + "/1.fr").readlines()), 50)
        self.assertEquals(len(Path(self.tmpdir + "/0.en").readlines()), 50)
        self.assertEquals(len(Path(self.tmpdir + "/1.en").readlines()), 50)

        self.assertSetEqual(set(splitStems), set([Path(self.tmpdir + "/0"), Path(self.tmpdir + "/1")]))
        self.assertEquals(Path(self.tmpdir + "/0.fr").readlines()[0], self.inFile.readlines()[0])
        self.assertEquals(Path(self.tmpdir + "/0.en").readlines()[0], self.outFile.readlines()[0])
        self.assertEquals(Path(self.tmpdir + "/1.fr").readlines()[0], self.inFile.readlines()[50])
        self.assertEquals(Path(self.tmpdir + "/1.en").readlines()[0], self.outFile.readlines()[50])
    
        splitStems = CorpusProcessor(self.tmpdir).splitData(acorpus, 3)
        self.assertTrue(Path(self.tmpdir + "/0.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/1.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/2.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/0.en").exists())
        self.assertTrue(Path(self.tmpdir + "/1.en").exists())
        self.assertTrue(Path(self.tmpdir + "/2.en").exists())
        self.assertEquals(len(Path(self.tmpdir + "/0.fr").readlines()), 33)
        self.assertEquals(len(Path(self.tmpdir + "/1.fr").readlines()), 33)
        self.assertEquals(len(Path(self.tmpdir + "/2.fr").readlines()), 34)
        self.assertEquals(len(Path(self.tmpdir + "/0.en").readlines()), 33)
        self.assertEquals(len(Path(self.tmpdir + "/1.en").readlines()), 33)
        self.assertEquals(len(Path(self.tmpdir + "/2.en").readlines()), 34)

        
    def langmodel(self):
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        result1 = exp.queryLanguageModel("Where is it ?")
        self.assertAlmostEqual(result1["logprob"], -6.29391, 3)
        result2 = exp.queryLanguageModel("unrelated phrase I find here")
        self.assertAlmostEqual(result2["logprob"], -8.35604, 3)
        self.assertEqual(result2["OOVs"], 3)
        self.assertTrue(exp.lm)
        
    
    def translationmodel(self):
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(self.inFile.getStem(), pruning=False)
        self.assertTrue(exp.tm)
        exp.executor.run("gunzip " + exp.tm+"/phrase-table.gz")
        lines = Path(exp.tm + "/phrase-table").readlines()
        self.assertIn("veux te donner ||| want to give ||| 1 0.0451128 1 1 "
                      + "||| 0-0 1-1 2-2 ||| 1 1 1 ||| |||\n", lines)
        exp.executor.run("gzip " + exp.tm + "/phrase-table")
        config = MosesConfig(exp.tm + "/moses.ini")
        self.assertEqual(config.getPhraseTable(), exp.tm + "/phrase-table.gz")
        initSize = config.getPhraseTable().getSize()
        exp.prunePhraseTable()
        self.assertEqual(config.getPhraseTable(), exp.tm + "/phrase-table.reduced.gz")        
        newSize = config.getPhraseTable().getSize()
        self.assertLess(newSize, initSize)
        
    
    def tuning(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, tune, _, _ = datadivision.divideData(acorpus, 10, 0, 10, randomPick=False)
        tuneSourceLines = tune.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        tuneTargetLines = tune.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(tune.getStem() + "2.fr").writelines(tuneSourceLines)
        Path(tune.getStem() + "2.en").writelines(tuneTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem(), pruning=False)
        exp.tuneTranslationModel(tune.getStem() + "2")
        self.assertIn("tunedmodel", exp.iniFile)
        self.assertTrue(Path(exp.iniFile.getUp()+"/run3.out").exists())
        self.assertTrue(Path(exp.iniFile).exists())
        
        exp.reduceSize()
        self.assertSetEqual(set(os.listdir(exp.expPath)), 
                            set(['langmodel.blm.en', 'settings.json', 'translationmodel', 
                                  'truecasingmodel.en', 'truecasingmodel.fr', 'tunedmodel']))
        self.assertSetEqual(set(os.listdir(exp.tm.getUp())), set(["model"]))
        self.assertSetEqual(set(os.listdir(exp.tm + "")), 
                            set(["phrase-table.gz", "moses.ini", 
                                 "reordering-table.wbe-msd-bidirectional-fe.gz"]))
        self.assertSetEqual(set(os.listdir(exp.iniFile.getUp())), set(["moses.ini"]))

    def paths(self):
        p = Path(self.tmpdir + "/blabla.en")
        self.assertFalse(p.exists())
        self.assertEqual(p.getAbsolute().getUp().getUp(), Path(__file__).getUp().getUp())
        self.assertEquals(p.addProperty("prop"), self.tmpdir + "/blabla.prop.en")
        self.assertEquals(p.addProperty("prop").getProperty(), "prop")
        self.assertEquals(p.getLang(), "en")
        self.assertEquals(p.getStem(), self.tmpdir + "/blabla")
        self.assertEquals(p.addProperty("prop").removeProperty(), self.tmpdir + "/blabla.en")
        self.assertEquals(p.basename(), "blabla.en")
        p.writelines(["line1\n", "line2"])
        self.assertTrue(p.exists())
        self.assertListEqual(p.readlines(), ["line1\n", "line2"])
        self.assertEquals(p.addProperty("prop").addProperty("blob"), self.tmpdir + "/blabla.prop.blob.en")
        self.assertEquals(p.addProperty("prop").changeProperty("blob"), self.tmpdir + "/blabla.blob.en")
        self.assertEquals(p.getSize(), 11)
        self.assertEquals(p.getDescription(), p + " (0K)")
    
    
    def translate(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, _, _, test = datadivision.divideData(acorpus, 10, 0, 10, randomPick=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        bleu = exp.evaluateBLEU(test.getStem()+"2")
        self.assertTrue(exp.test)
        self.assertTrue(exp.test["translation"])
        self.assertEqual(Path(exp.test["stem"]+".fr").readlines()[2], "comment vas-tu ?\n")
        self.assertEqual(Path(exp.test["stem"]+".en").readlines()[2], "how you been ?\n")
        self.assertEqual(Path(exp.test["translation"]).readlines()[2], "how vas-tu ? \n")
        self.assertAlmostEquals(bleu, 61.39, 2)  
        
    
    def parallel(self):
  
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, _, _, test = datadivision.divideData(acorpus, 10, 0, 10, randomPick=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = SlurmExperiment("paralleltest", "fr", "en", maxJobs=2)
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        self.assertTrue(exp.tm)
        bleu = exp.evaluateBLEU(test.getStem()+"2")
        self.assertTrue(exp.test)
        self.assertTrue(exp.test["translation"])
        self.assertEqual(Path(exp.test["stem"]+".fr").readlines()[2], "comment vas-tu ?\n")
        self.assertEqual(Path(exp.test["stem"]+".en").readlines()[2], "how you been ?\n")
        self.assertEqual(Path(exp.test["translation"]).readlines()[2], "how vas-tu ? \n")
        self.assertAlmostEquals(bleu, 61.39, 2)  
    
    
    def copy(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, _, _, test = datadivision.divideData(acorpus, 10, 0, 10, randomPick=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        exp = exp.copy("newexp")
        bleu = exp.evaluateBLEU(test.getStem()+"2")
        self.assertTrue(exp.test)
        self.assertTrue(exp.test["translation"])
        self.assertAlmostEquals(bleu, 61.39, 2)    
        
 
    def config(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, _, _, _ = datadivision.divideData(acorpus, 10, 0, 10, randomPick=False)
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        config = MosesConfig(exp.iniFile)  
        self.assertSetEqual(config.getPaths(), 
                            set([exp.lm,
                                 exp.tm+"/phrase-table.reduced.gz",
                                 exp.tm+"/reordering-table.wbe-msd-bidirectional-fe.gz"]))  
        
        exp2 = Experiment("test")
        self.assertEqual(exp2.sourceLang, "fr")   
        self.assertEqual(exp2.targetLang, "en")   
        self.assertEqual(exp2.lm, exp.lm)  
        self.assertEqual(exp2.ngram_order, exp.ngram_order)  
        self.assertEqual(exp2.tm, exp.tm)
        self.assertEqual(exp2.tm, exp.expPath+"/translationmodel/model") 
        self.assertEqual(exp2.iniFile, exp.expPath+"/translationmodel/model/moses.ini") 
        self.assertEqual(exp.test, None)
 
 
 
    def analyse(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, _, _, test = datadivision.divideData(acorpus, 10, 0, 10, randomPick=False)
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        exp = exp.copy("newexp")
    
        sys.stdout = open(self.tmpdir + "/out.txt", 'w')         
        exp.evaluateBLEU(test.getStem())
        exp.analyseErrors()
        sys.stdout.flush()
        output = Path(self.tmpdir + "/out.txt").read()
        self.assertIn("Previous line (reference):\tI\'m sorry to hear that.\n" 
                      + "Source line:\t\t\tAh, n\'en parlons plus.\n" 
                      + "Current line (reference):\tAah, screw her.\n"
                      + "Current line (actual):\t\tAh, would heat parlons plus.", output)
        
    
    def duplicates(self):
        dupls = datadivision.extractDuplicates(BasicCorpus(self.duplicatesFile))
        self.assertEqual(len(dupls), 8)
        self.assertIn(691, dupls)
        
        
    def test_mosesparallel(self):
        acorpus = AlignedCorpus(self.inFile.getStem(), "fr", "en")
        train, _, _, _ = datadivision.divideData(acorpus, 10, 0, 10,randomPick=False)
        experiment.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -f " + exp.iniFile,
                                    stdin="qui êtes-vous ?\n")
        self.assertEqual(output, "qui are you ?") 
        Path(self.tmpdir + "/transtest.fr").writelines(["qui êtes-vous ?\n", "tant pis .\n"])
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -f " + exp.iniFile,
                                    stdin=(self.tmpdir + "/transtest.fr"))
        self.assertEqual(output, "qui are you ? \ntant mind .")
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -jobs 2 -f " + exp.iniFile,
                                    stdin=(self.tmpdir + "/transtest.fr"))
        self.assertEqual(output, "qui are you ? \ntant mind .")
        
        
        
        # binarise model!
        # Json!

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    unittest.main()
