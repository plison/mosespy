# -*- coding: utf-8 -*- 

import unittest, uuid, os, shutil
import experiment
from paths import Path
from preprocessing import Preprocessor
from process import CommandExecutor
from corpus import AlignedCorpus

inFile = Path(__file__).getUp().getUp()+"/data/tests/subtitles.fr"
outFile = Path(__file__).getUp().getUp()+"/data/tests/subtitles.en"

class Pipeline(unittest.TestCase):

    def setUp(self):
        self.tmpdir = "./tmp" + str(uuid.uuid4())[0:6]
        os.makedirs(self.tmpdir)
        
    def test_preprocessing(self):
        
        processor = Preprocessor(self.tmpdir, CommandExecutor())
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
        print os.listdir(self.tmpdir)
        self.assertEquals(len(os.listdir(self.tmpdir)), 5)


    def test_langmodel(self):
        experiment.expDir = self.tmpdir + "/"
        exp = experiment.Experiment("test", "fr", "en")
        exp.trainLanguageModel(outFile, preprocess=True, keepArpa=True)
        
    def tearDown(self):
        print ""
    #    shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    unittest.main()