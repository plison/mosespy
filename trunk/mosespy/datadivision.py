
import sys, math, random
import mosespy.slurm as slurm
from mosespy.corpus import AlignedCorpus, BasicCorpus
from mosespy.system import Path



def divideData(corpus, nbTuning=1000, nbDevelop=3000, 
               nbTesting=3000, randomPick=True, duplicatesWindow=4):
    
    if not isinstance(corpus, AlignedCorpus):
        raise RuntimeError("corpus must be of type AlignedCorpus")
    
    if nbTuning + nbDevelop + nbTesting > corpus.getSourceFile().countNbLines():
        raise RuntimeError("cannot divide such small amount of data")
    
    outputPath = corpus.getSourceFile().getUp()
    
    if randomPick:
        nbLines = corpus.getSourceFile().countNbLines()
        toExclude = extractDuplicates(corpus.getSourceCorpus(), outputPath, duplicatesWindow)
        
        tuningIndices =_drawRandom(nbTuning, nbLines, exclusion=toExclude)
        toExclude = toExclude.union(tuningIndices)
        developIndices =_drawRandom(nbDevelop, nbLines, exclusion=toExclude)
        toExclude = toExclude.union(developIndices)
        testingIndices = _drawRandom(nbTesting,nbLines, exclusion=toExclude)
    else:
        nbLines = corpus.getSourceFile().countNbLines()
        tuningIndices = range(0,nbLines)[-nbTuning-nbTesting-nbDevelop:-nbDevelop-nbTesting]
        developIndices = range(0,nbLines)[-nbDevelop-nbTesting:-nbTesting]
        testingIndices = range(0,nbLines)[-nbTesting:]

    sourceLines = corpus.getSourceFile().readlines()
    targetLines = corpus.getTargetFile().readlines()

    trainSourceLines = []
    tuneSourceLines = []
    devSourceLines = []       
    testSourceLines = []       
    print "Dividing source data..."
    for i in range(0, len(sourceLines)):
        sourceLine = sourceLines[i]
        if i in tuningIndices:
            tuneSourceLines.append(sourceLine)
        elif i in developIndices:
            devSourceLines.append(sourceLine)
        elif i in testingIndices:
            testSourceLines.append(sourceLine)
        else:
            trainSourceLines.append(sourceLine)
    
    trainTargetLines = []
    tuneTargetLines = []
    devTargetLines = []
    testTargetLines = []       
    print "Dividing target data..."
    for i in range(0, len(targetLines)):
        targetLine = targetLines[i]
        if i in tuningIndices:
            tuneTargetLines.append(targetLine)
        elif i in developIndices:
            devTargetLines.append(targetLine)
        elif i in testingIndices:
            testTargetLines.append(targetLine)
        else:
            trainTargetLines.append(targetLine)
    
    trainStem = outputPath + "/" + (corpus.stem + ".train").basename()
    (trainStem + "." + corpus.sourceLang).writelines(trainSourceLines) 
    (trainStem + "." + corpus.targetLang).writelines(trainTargetLines)
    trainCorpus = AlignedCorpus(trainStem, corpus.sourceLang, corpus.targetLang)
    
    
    tuneStem = outputPath + "/" + (corpus.stem + ".tune").basename()
    (tuneStem + "." + corpus.sourceLang).writelines(tuneSourceLines) 
    (tuneStem + "." + corpus.targetLang).writelines(tuneTargetLines)
    (tuneStem + ".indices").writelines([corpus.stem+"\n"] + 
                                       [str(i)+"\n" for i in sorted(list(tuningIndices))])
    tuneCorpus = AlignedCorpus(tuneStem, corpus.sourceLang, corpus.targetLang)

    devStem = outputPath + "/" + (corpus.stem + ".dev").basename()
    (devStem + "." + corpus.sourceLang).writelines(devSourceLines) 
    (devStem + "." + corpus.targetLang).writelines(devTargetLines)
    (devStem + ".indices").writelines([corpus.stem+"\n"] + 
                                       [str(i)+"\n" for i in sorted(list(developIndices))])
    devCorpus = AlignedCorpus(devStem, corpus.sourceLang, corpus.targetLang)

    testStem = outputPath + "/" + (corpus.stem + ".test").basename()
    (testStem + "." + corpus.sourceLang).writelines(testSourceLines) 
    (testStem + "." + corpus.targetLang).writelines(testTargetLines)
    (testStem + ".indices").writelines([corpus.stem+"\n"] + 
                                       [str(i)+"\n" for i in sorted(list(testingIndices))])
    testCorpus = AlignedCorpus(testStem, corpus.sourceLang, corpus.targetLang)

    return trainCorpus, tuneCorpus, devCorpus, testCorpus
    
    
def extractDuplicates(corpus, outputPath, window=4, nbSplits=20):
    
    if not isinstance(corpus, BasicCorpus):
        raise RuntimeError("corpus must be of type BasicCorpus")
    
    print "Start search for duplicates (%i splits)"%(nbSplits)
    sourceLines = corpus.getCorpusFile().readlines()
    nbLines = len(sourceLines)
    indices = range(0, nbLines)
    indices.sort(key=lambda x : sourceLines[x])
    
    step = len(indices)/nbSplits    
    indicesFiles = [Path(outputPath + "/ind"+str(i)) for i in range(0, nbSplits)]
    for i in range(0, nbSplits):
        indicesFiles[i].write(" ".join([str(j) for j in indices[i*step:i*step + step]]))
    
    args = [(corpus.getCorpusFile(),indicesFile, window) for indicesFile in indicesFiles]
    
    outputs = slurm.SlurmExecutor().run_parallel_function(_printDuplicates, args,
                                                      stdouts=[True]*nbSplits)
    duplicates = set()
    for output in outputs:
        duplicates = duplicates.union([int(d) for d in output.split()])
    print ("Duplicates found: " + str(len(duplicates)) 
           + " (" + str(len(duplicates)*100.0/nbLines) + " % of total)") 

    return duplicates



def  _printDuplicates(sourceFile, indicesFile, window):
    sys.stderr.write("Starting local extraction of source duplicates...\n") 
    indicesFile = Path(indicesFile)
    indices = [int(val) for val in indicesFile.read().split()] 
    sourceLines = Path(sourceFile).readlines()
    duplicates = set() 
    for i in range(0, len(indices)):
        curIndex = indices[i]
        curWindow = sourceLines[curIndex:curIndex+window]
        for j in range(i+1, len(indices)-window):
            nextIndex = indices[j]
            nextWindow = sourceLines[nextIndex:nextIndex+window]
            if curWindow[0] != nextWindow[0]:
                break
            elif curWindow == nextWindow:
                duplicates.add(curIndex)
                duplicates.add(nextIndex)
                break
        if len(indices) > 100 and not (i % (len(indices)/100)):
            sys.stderr.write("Extraction of duplicates: " + 
                             str(math.ceil(i*10000/len(indices)) / 100) + " %\n")
    
    indicesFile.remove()
    print " ".join([str(d) for d in duplicates])

   

      
def _drawRandom(nbToDraw, maxValue, exclusion=None):
    
    numbers = set()     
    while len(numbers) < nbToDraw:
        choice = random.randrange(0, maxValue)
        if not exclusion or choice not in exclusion:
            numbers.add(choice)
    
    return numbers   

 
def filterOutLines(fullCorpus, toRemoveCorpus):

    inputLines = fullCorpus.getCorpusFile().readlines()
    
    occurrences = toRemoveCorpus.getOccurrences()
    print occurrences
    histories = toRemoveCorpus.getHistories()  

    outputFile = fullCorpus.getCorpusFile().addProperty("filtered") 
    with open(outputFile, 'w', 1000000) as newLmFileD:                 
        skippedLines = []
        for i in range(2, len(inputLines)):
            l = inputLines[i]
            toSkip = False
            if l in occurrences:
                for index in occurrences[l]:
                    print "indexes are : " + str(index)
                    print "comparing " + str(histories[index]) + " with " + str(inputLines[i-2:i])
                    if histories[index] == inputLines[i-2:i]:
                        skippedLines.append(l)
                        toSkip = True
            if not toSkip:
                newLmFileD.write(l)                                

    print "Number of skipped lines: " + str(len(skippedLines))
    return outputFile

