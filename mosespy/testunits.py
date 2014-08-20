# -*- coding: utf-8 -*- 

import unittest, uuid, os, shutil, slurm
import experiment
from system import Path, CommandExecutor
from processing import CorpusProcessor
from corpus import BasicCorpus, AlignedCorpus
from config import MosesConfig

inFile = Path(__file__).getUp().getUp()+"/data/tests/subtitles.fr"
outFile = Path(__file__).getUp().getUp()+"/data/tests/subtitles.en"

class Pipeline(unittest.TestCase):

    def setUp(self):
        self.tmpdir = "./tmp" + str(uuid.uuid4())[0:6]
        os.makedirs(self.tmpdir)
        print "using directory " + str(self.tmpdir)
        
    def test_preprocessing(self):
        
        processor = CorpusProcessor(self.tmpdir, CommandExecutor())
        trueFile = processor.processFile(inFile)
        trueLines = trueFile.readlines()
        self.assertIn("bien occupé .\n", trueLines)
        self.assertIn("comment va Janet ?\n", trueLines)
        self.assertIn("non , j&apos; ai complètement compris .\n", trueLines)
        self.assertTrue(processor.truecaser.isModelTrained("fr"))
        self.assertFalse(processor.truecaser.isModelTrained("en"))
        self.assertIn(inFile.basename().addProperty("true"), os.listdir(self.tmpdir))
        
        revertFile = processor.revertFile(trueFile)
        revertLines = revertFile.readlines()
        self.assertIn("non, j'ai complètement compris.\n", revertLines)
        
        corpus2 = processor.processCorpus(AlignedCorpus(inFile.getStem(), "fr", "en"))
        self.assertIsInstance(corpus2, AlignedCorpus)
        self.assertEqual(corpus2.getStem(), self.tmpdir + "/" + inFile.basename().getStem().addProperty("clean"))
        self.assertIn("non , j&apos; ai complètement compris .\n", corpus2.getSourceFile().readlines())
        self.assertEqual(corpus2.sourceLang, "fr")
        self.assertEqual(corpus2.targetLang, "en")
        self.assertIn(inFile.basename().addProperty("clean"), os.listdir(self.tmpdir))
        self.assertIn(outFile.basename().addProperty("clean"), os.listdir(self.tmpdir))
        self.assertEquals(len(os.listdir(self.tmpdir)), 5)


    def test_division(self):
        acorpus = AlignedCorpus(inFile.getStem(), "fr", "en")
        train, tune, test = acorpus.divideData(self.tmpdir, 10, 10)
        self.assertTrue(os.path.exists(test.getStem()+".indices"))
        testlines = (test.getStem()+".en").readlines()
        indlines = (test.getStem()+".indices").readlines()
        self.assertEquals(indlines[0].strip(), inFile.getStem())
        targetlines = outFile.readlines()
        for i in range(0, len(testlines)):
            testLine = testlines[i]
            originIndex = int(indlines[i+1].strip())
            correspondingLine = targetlines[originIndex]
            self.assertEquals(testLine, correspondingLine)
        
        occurrences = test.getTargetCorpus().getOccurrences()
        histories = test.getTargetCorpus().getHistories()
        self.assertGreater(len(occurrences), 8)

        for i in range(0, len(testlines)):
            testLine = testlines[i]
            occ = occurrences[testLine]
            self.assertGreaterEqual(len(occ), 1)
            self.assertIn(i, occ)
            oindices = [k for k, x in enumerate(targetlines) if x == testLine]
            self.assertEquals(testLine, targetlines[oindices[0]])
            self.assertTrue(any([histories[i]==targetlines[max(0,q-2):q] for q in oindices]))
        
        newFile = test.getTargetFile().addProperty("2") 
        BasicCorpus(outFile).filterOutLines(test.getTargetFile(), newFile)
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
        acorpus = AlignedCorpus(inFile.getStem(), "fr", "en")
        splitStems = acorpus.splitData(self.tmpdir, 2)
        self.assertTrue(Path(self.tmpdir + "/0.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/1.fr").exists())
        self.assertTrue(Path(self.tmpdir + "/0.en").exists())
        self.assertTrue(Path(self.tmpdir + "/1.en").exists())
        self.assertEquals(len(Path(self.tmpdir + "/0.fr").readlines()), 50)
        self.assertEquals(len(Path(self.tmpdir + "/1.fr").readlines()), 50)
        self.assertEquals(len(Path(self.tmpdir + "/0.en").readlines()), 50)
        self.assertEquals(len(Path(self.tmpdir + "/1.en").readlines()), 50)

        self.assertSetEqual(set(splitStems), set([Path(self.tmpdir + "/0"), Path(self.tmpdir + "/1")]))
        self.assertEquals(Path(self.tmpdir + "/0.fr").readlines()[0], inFile.readlines()[0])
        self.assertEquals(Path(self.tmpdir + "/0.en").readlines()[0], outFile.readlines()[0])
        self.assertEquals(Path(self.tmpdir + "/1.fr").readlines()[0], inFile.readlines()[50])
        self.assertEquals(Path(self.tmpdir + "/1.en").readlines()[0], outFile.readlines()[50])
    
        splitStems = acorpus.splitData(self.tmpdir, 3)
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
        experiment.expDir = self.tmpdir + "/"
        exp = experiment.Experiment("test", "fr", "en")
        exp.trainLanguageModel(outFile, preprocess=True)
        result1 = exp.queryLanguageModel("Where is it ?")
        self.assertAlmostEqual(result1["logprob"], -6.29391, 3)
        result2 = exp.queryLanguageModel("unrelated phrase I find here")
        self.assertAlmostEqual(result2["logprob"], -8.35604, 3)
        self.assertEqual(result2["OOVs"], 3)
        self.assertTrue(exp.settings.has_key("lm"))
        
    
    def test_translationmodel(self):
        experiment.expDir = self.tmpdir + "/"
        exp = experiment.Experiment("test", "fr", "en")
        exp.trainLanguageModel(outFile, preprocess=True)
        exp.trainTranslationModel(inFile.getStem())
        self.assertTrue(exp.settings.has_key("tm"))
        exp.executor.run("gunzip " + exp.settings["tm"]+"/model/phrase-table.gz")
        lines = Path(exp.settings["tm"]+"/model/phrase-table").readlines()
        self.assertIn("veux te donner ||| want to give ||| 1 0.0451128 1 1 "
                      + "||| 0-0 1-1 2-2 ||| 1 1 1 ||| ||| \n", lines)
        exp.executor.run("gzip " + exp.settings["tm"]+"/model/phrase-table")
        config = MosesConfig(exp.settings["tm"]+"/model/moses.ini")
        self.assertEqual(config.getPhraseTable(), exp.settings["tm"]+"/model/phrase-table.gz")
        initSize = config.getPhraseTable().getSize()
        exp._prunePhraseTable()
        self.assertEqual(config.getPhraseTable(), exp.settings["tm"]+"/model/phrase-table.reduced.gz")        
        newSize = config.getPhraseTable().getSize()
        self.assertLess(newSize, initSize)
        
    
    def test_tuning(self):
        acorpus = AlignedCorpus(inFile.getStem(), "fr", "en")
        train, tune, test = acorpus.divideData(self.tmpdir, 10, 10, random=False)
        tuneSourceLines = tune.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        tuneTargetLines = tune.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(tune.getStem() + "2.fr").writelines(tuneSourceLines)
        Path(tune.getStem() + "2.en").writelines(tuneTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = experiment.Experiment("test", "fr", "en")
        exp.trainLanguageModel(outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        exp.tuneTranslationModel(tune.getStem() + "2")
        self.assertTrue(exp.settings.has_key("ttm"))
        self.assertTrue(Path(exp.settings["ttm"]+"/run3.out").exists())
        self.assertTrue(Path(exp.settings["ttm"]+"/moses.ini").exists())
        
        exp.reduceSize()
        self.assertSetEqual(set(os.listdir(exp.settings["path"])), 
                            set(['langmodel.blm.en', 'settings.json', 'translationmodel', 
                                  'truecasingmodel.en', 'truecasingmodel.fr', 'tunedmodel']))
        self.assertSetEqual(set(os.listdir(exp.settings["tm"])), set(["model"]))
        self.assertSetEqual(set(os.listdir(exp.settings["tm"]+"/model")), set(["moses.ini"]))
        self.assertSetEqual(set(os.listdir(exp.settings["ttm"])), set(["moses.ini"]))

    def test_paths(self):
        p = Path(self.tmpdir + "/blabla.en")
        self.assertFalse(p.exists())
        self.assertEqual(p.getAbsolute().getUp().getUp(), Path(__file__).getUp())
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
    
    
    def test_translate(self):
        acorpus = AlignedCorpus(inFile.getStem(), "fr", "en")
        train, tune, test = acorpus.divideData(self.tmpdir, 10, 10, random=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = experiment.Experiment("test", "fr", "en")
        exp.trainLanguageModel(outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        bleu = exp.evaluateBLEU(test.getStem()+"2")
        self.assertTrue(exp.settings.has_key("test"))
        self.assertTrue(exp.settings["test"]["translation"])
        self.assertEqual(Path(exp.settings["test"]["stem"]+".fr").readlines()[2], "comment vas-tu ?\n")
        self.assertEqual(Path(exp.settings["test"]["stem"]+".en").readlines()[2], "how you been ?\n")
        self.assertEqual(Path(exp.settings["test"]["translation"]).readlines()[2], "how vas-tu ? \n")
        self.assertAlmostEquals(bleu, 61.39, 2)  
        
    
    def test_parallel(self):
  
        acorpus = AlignedCorpus(inFile.getStem(), "fr", "en")
        train, tune, test = acorpus.divideData(self.tmpdir, 10, 10, random=False)
        testSourceLines = test.getSourceFile().readlines() + train.getSourceFile().readlines()[0:10]
        testTargetLines = test.getTargetFile().readlines() + train.getTargetFile().readlines()[0:10]        
        Path(test.getStem() + "2.fr").writelines(testSourceLines)
        Path(test.getStem() + "2.en").writelines(testTargetLines)
        experiment.expDir = self.tmpdir + "/"
        exp = slurm.SlurmExperiment("paralleltest", "fr", "en", maxJobs=2)
        exp.trainLanguageModel(outFile, preprocess=True)
        exp.trainTranslationModel(train.getStem())
        self.assertTrue(exp.settings.has_key("tm"))
        bleu = exp.evaluateBLEU(test.getStem()+"2")
        self.assertTrue(exp.settings.has_key("test"))
        self.assertTrue(exp.settings["test"]["translation"])
        self.assertEqual(Path(exp.settings["test"]["stem"]+".fr").readlines()[2], "comment vas-tu ?\n")
        self.assertEqual(Path(exp.settings["test"]["stem"]+".en").readlines()[2], "how you been ?\n")
        self.assertEqual(Path(exp.settings["test"]["translation"]).readlines()[2], "how vas-tu ? \n")
        self.assertAlmostEquals(bleu, 61.39, 2)  
        
        
    def tearDown(self):
        print ""
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    unittest.main()