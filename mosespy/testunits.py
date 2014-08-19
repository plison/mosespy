# -*- coding: utf-8 -*- 

import unittest, uuid, os, shutil
import experiment, corpus
from paths import Path
from preprocessing import Preprocessor
from process import CommandExecutor
from corpus import BasicCorpus, AlignedCorpus

inFile = Path(__file__).getUp().getUp()+"/data/tests/subtitles.fr"
outFile = Path(__file__).getUp().getUp()+"/data/tests/subtitles.en"

class Pipeline(unittest.TestCase):

    def setUp(self):
        self.tmpdir = "./tmp" + str(uuid.uuid4())[0:6]
        os.makedirs(self.tmpdir)
        print "using directory " + str(self.tmpdir)
        
    def preprocessing(self):
        
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
            
        newFile = BasicCorpus(outFile).filterOutLines(test.getTargetFile(), self.tmpdir)
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
        
        
    def langmodel(self):
        experiment.expDir = self.tmpdir + "/"
        exp = experiment.Experiment("test", "fr", "en")
        exp.trainLanguageModel(outFile, preprocess=True, keepArpa=True)
        
    def tearDown(self):
        print ""
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    unittest.main()