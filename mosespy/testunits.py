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
import mosespy.constants as constants
import mosespy.slurm as slurm
import mosespy.system as system
import mosespy.analyser as analyser
from mosespy.system import Path, ShellExecutor
from mosespy.corpus import AlignedCorpus, CorpusProcessor
from mosespy.experiment import Experiment, MosesConfig
from mosespy.slurm import SlurmExperiment
import mosespy.datadivision as datadivision

slurm.correctSlurmEnv()

class Pipeline(unittest.TestCase):

    def setUp(self):
        self.tmpdir = Path(__file__).getUp().getUp() + "/tmp" + str(uuid.uuid4())[0:8]
        os.makedirs(self.tmpdir)
        self.inFile = (Path(__file__).getUp()+"/data/subtitles.fr").copy(self.tmpdir)
        self.outFile = (Path(__file__).getUp()+"/data/subtitles.en").copy(self.tmpdir)
        self.duplicatesFile = (Path(__file__).getUp()+"/data/withduplicates.txt").copy(self.tmpdir)
        
    def test_preprocessing(self):
        
        processor = CorpusProcessor(self.tmpdir, ShellExecutor())
        trueFile = processor.processFile(self.inFile)
        trueLines = trueFile.readlines()
        self.assertIn("bien occupé .\n", trueLines)
        self.assertIn("comment va Janet ?\n", trueLines)
        self.assertIn("non , j&apos; ai complètement compris .\n", trueLines)
        self.assertTrue(processor.truecaser.isModelTrained("fr"))
        self.assertFalse(processor.truecaser.isModelTrained("en"))
        self.assertIn(self.inFile.basename().addFlag("true"), os.listdir(self.tmpdir))
        
        revertFile = processor.revertFile(trueFile)
        revertLines = revertFile.readlines()
        self.assertIn("non, j'ai complètement compris.\n", revertLines)
        
        corpus2 = processor.processCorpus(AlignedCorpus(self.inFile.getStem(), "fr", "en"))
        self.assertIsInstance(corpus2, AlignedCorpus)
        self.assertEqual(corpus2.getStem(), self.tmpdir + "/" + self.inFile.basename().getStem().addFlag("clean"))
        self.assertIn("non , j&apos; ai complètement compris .\n", corpus2.getSourceFile().readlines())
        self.assertEqual(corpus2.sourceLang, "fr")
        self.assertEqual(corpus2.targetLang, "en")
        self.assertIn(self.inFile.basename().addFlag("clean"), os.listdir(self.tmpdir))
        self.assertIn(self.outFile.basename().addFlag("clean"), os.listdir(self.tmpdir))
        self.assertEquals(len(os.listdir(self.tmpdir)), 8)


    def test_division(self):
        _, _, _, test = datadivision.divideData(self.inFile.getStem(), "fr", "en", 
                                                10, 0, 10)
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
        
        newFile = datadivision.filterOutLines(self.outFile, test.getTargetFile())
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
        
    
    def test_split(self):
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

        
    def test_langmodel(self):
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        result1 = exp.queryLanguageModel("Where is it ?")
        self.assertAlmostEqual(result1["logprob"], -6.29391, 3)
        result2 = exp.queryLanguageModel("unrelated phrase I find here")
        self.assertAlmostEqual(result2["logprob"], -8.35604, 3)
        self.assertEqual(result2["OOVs"], 3)
        self.assertTrue(exp.lm)
        
    
    def test_translationmodel(self):
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test2", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(self.inFile.getStem(), pruning=False)
        self.assertTrue(exp.tm)
        exp.executor.run("gunzip " + exp.tm+"/phrase-table.gz")
        lines = Path(exp.tm + "/phrase-table").read()
        self.assertIn("veux te donner ||| want to give ||| ", lines)
        exp.executor.run("gzip " + exp.tm + "/phrase-table")
        config = MosesConfig(exp.tm + "/moses.ini")
        self.assertEqual(config.getPhraseTable(), exp.tm + "/phrase-table.gz")
        initSize = config.getPhraseTable().getSize()
        exp.trainTranslationModel(self.inFile.getStem(), pruning=True)
        config = MosesConfig(exp.tm + "/moses.ini")
        self.assertEqual(config.getPhraseTable(), exp.tm + "/phrase-table.reduced.gz")
        newSize = config.getPhraseTable().getSize()
        self.assertLessEqual(newSize/1000, initSize/1000)
        
    
    def test_tuning(self):
        train, tune, _, _ = datadivision.divideData(self.inFile.getStem(), "fr", "en", 
                                                    10, 0, 10, randomPick=False)
        tuneSourceLines = tune.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        tuneTargetLines = tune.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(tune.getStem() + "2.fr").writelines(tuneSourceLines)
        Path(tune.getStem() + "2.en").writelines(tuneTargetLines)
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem(), pruning=False)
        exp.tuneTranslationModel(tune.getStem() + "2")
        self.assertIn("tunedmodel", exp.iniFile)
        self.assertTrue(Path(exp.iniFile.getUp()+"/run2.out").exists())
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

    def test_paths(self):
        p = Path(self.tmpdir + "/blabla.en")
        self.assertFalse(p.exists())
        self.assertEqual(p.getAbsolute().getUp().getUp(), Path(__file__).getUp().getUp())
        self.assertEquals(p.addFlag("prop"), self.tmpdir + "/blabla.prop.en")
        self.assertEquals(p.addFlag("prop").getFlags(), "prop")
        self.assertEquals(p.getLang(), "en")
        self.assertEquals(p.getStem(), self.tmpdir + "/blabla")
        self.assertEquals(p.addFlag("prop").removeFlags(), self.tmpdir + "/blabla.en")
        self.assertEquals(p.basename(), "blabla.en")
        p.writelines(["line1\n", "line2"])
        self.assertTrue(p.exists())
        self.assertListEqual(p.readlines(), ["line1\n", "line2"])
        self.assertEquals(p.addFlag("prop").addFlag("blob"), self.tmpdir + "/blabla.prop.blob.en")
        self.assertEquals(p.addFlag("prop").changeFlag("blob"), self.tmpdir + "/blabla.blob.en")
        self.assertEquals(p.getSize(), 11)
        self.assertEquals(p.getDescription(), p + " (0K)")
    
    
    def test_translate(self):
        train, _, _, test = datadivision.divideData(self.inFile.getStem(), "fr", "en", 
                                                    10, 0, 10, randomPick=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        bleu = exp.evaluateBLEU(test.getStem()+"2")[1]
        self.assertTrue(exp.results)
        self.assertTrue(exp.results.getTranslationFile())
        print "translated corpus: " + exp.results.getSourceFile()
        print "translated corpus2: " + exp.results.getTargetFile()
        self.assertEqual(Path(exp.results.getSourceFile()).readlines()[2], "comment vas-tu ?\n")
        self.assertEqual(Path(exp.results.getTargetFile()).readlines()[2], "how you been?\n")
        self.assertEqual(Path(exp.results.getTranslationFile()).readlines()[2], "how vas-tu?\n")
        self.assertAlmostEquals(bleu, 61.39, delta=2)  
        exp.translateFile(exp.results.getSourceFile(), self.tmpdir + "/translation.en", revertOutput=False)
        self.assertEqual((self.tmpdir + "/translation.en").readlines()[2], "how vas-tu ? \n")
        exp.translateFile(exp.results.getSourceFile(), self.tmpdir + "/translation.en", revertOutput=True)
        self.assertEqual((self.tmpdir + "/translation.en").readlines()[2], "how vas-tu?\n")
    
    def test_parallel(self):  
        train, _, _, test = datadivision.divideData(self.inFile.getStem(), "fr", "en",
                                                     10, 0, 10, randomPick=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        constants.expDir = self.tmpdir + "/"
        exp = SlurmExperiment("paralleltest", "fr", "en", maxJobs=2)
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        self.assertTrue(exp.tm)
        bleu = exp.evaluateBLEU(test.getStem()+"2")[1]
        self.assertTrue(exp.results)
        self.assertTrue(exp.results.getTranslationFile())
        self.assertEqual(exp.results.getSourceFile().readlines()[2], "comment vas-tu ?\n")
        self.assertEqual(exp.results.getTargetFile().readlines()[2], "how you been?\n")
        self.assertEqual(exp.results.getTranslationFile().readlines()[2], "how vas-tu?\n")
        self.assertAlmostEquals(bleu, 61.39, delta=2)  
    
    
    def test_copy(self):
        train, _, _, test = datadivision.divideData(self.inFile.getStem(), "fr", "en", 
                                                    10, 0, 10, randomPick=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        exp = exp.copy("newexp")
        bleu = exp.evaluateBLEU(test.getStem()+"2")[1]
        self.assertTrue(exp.results)
        self.assertTrue(exp.results.getTranslationFile())
        self.assertAlmostEquals(bleu, 61.39, delta=2)      
        
 
    def test_config(self):
        train, _, _, _ = datadivision.divideData(self.inFile.getStem(), "fr", "en", 
                                                 10, 0, 10, randomPick=False)
        constants.expDir = self.tmpdir + "/"
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
        self.assertEqual(exp.results, None)
 
 
    def test_analyse(self):
        train, _, _, test = datadivision.divideData(self.inFile.getStem(), "fr", "en", 
                                                    10, 0, 10, randomPick=False)
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        exp = exp.copy("newexp")
    
        sys.stdout = open(self.tmpdir + "/out.txt", 'w')         
        exp.evaluateBLEU(test.getStem())
        analyser.analyseResults(exp.results)
        sys.stdout.flush()
        output = Path(self.tmpdir + "/out.txt").read()
        self.assertIn("Previous line (reference):\tI\'m sorry to hear that.\n" 
                      + "Source line:\t\t\tAh, n\'en parlons plus.\n" 
                      + "Current line (reference):\tAah, screw her.\n"
                      + "Current line (actual):\t\tAh, would heat parlons plus.", output)
        
    
    def test_duplicates(self):
        dupls = datadivision.extractDuplicates(self.duplicatesFile)
        self.assertEqual(len(dupls), 8)
        self.assertIn(691, dupls)
        
        
    def test_mosesparallel(self):
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(self.inFile.getStem())
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -f " + exp.iniFile,
                                    stdin="qui êtes-vous ?\n")
        self.assertEqual(output, "Who are you ?") 
        Path(self.tmpdir + "/transtest.fr").writelines(["qui êtes-vous ?\n", "tant pis .\n"])
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -f " + exp.iniFile,
                                    stdin=(self.tmpdir + "/transtest.fr"))
        self.assertEqual(output, "Who are you ? \ntant mind .")
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -jobs 2 -f " + exp.iniFile,
                                    stdin=(self.tmpdir + "/transtest.fr"))
        self.assertEqual(output, "Who are you ? \ntant mind .")
        
        
    def test_binarise(self):
        constants.expDir = self.tmpdir + "/"
        exp = Experiment("test", "fr", "en")
        exp.trainLanguageModel(self.outFile, preprocess=True)
        exp.trainTranslationModel(self.inFile.getStem())
        output = system.run_output(Path(__file__).getUp() + "/moses_parallel.py -f " + exp.iniFile,
                                    stdin="qui êtes-vous ?\n")
        self.assertEqual(output, "Who are you ?")
        output = system.run_output(Path(__file__).getUp().getUp() + "/moses/bin/moses -f " + exp.iniFile,
                                    stdin="qui êtes-vous ?\n")
        self.assertEqual(output, "Who are you ?")
        self.assertEquals(exp.translate("qui êtes-vous?"), "Who are you?")
        self.assertEquals(exp.translate("qui êtes-vous ?", preprocess=False), "Who are you?")
        exp.binariseModel()
        self.assertIn("PhraseDictionaryBinary", exp.iniFile.read())
        self.assertIn("/binmodel", exp.iniFile.read())
        self.assertEquals(exp.translate("qui êtes-vous?"), "Who are you?")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    unittest.main()
