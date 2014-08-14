
from utils import Path

def getAlignment(source, target, translation):
    source = Path(source)
    target = Path(target)
    translation = Path(translation)
    if source.countNbLines() != target.countNbLines():
        raise RuntimeError("Number of lines in source and reference translations are different")
    elif target.countNbLines() != translation.countNbLines():
        raise RuntimeError("Number of lines in actual and reference translations are different")
    
    alignments = []
    sourceLines = source.readlines()
    targetLines = target.readlines()
    print "description of source: " + source.getDescription() + " and nb lines " + str(source.countNbLines())
    translationLines = translation.readlines()
    for i in range(0, len(sourceLines)):
        align = {"source": sourceLines[i].strip(), "target": targetLines[i].strip(),
                 "translation": translationLines[i].strip(), "index": i}
        alignments.append(align)
    
    print "Number of aligned lines: " + str(len(alignments))
    return alignments


def addHistory(alignments, fullCorpusSource, fullCorpusTarget):
 
    alignmentsBySource = {}
    for i in range(0, len(alignments)):
        alignment = alignments[i]
        if not alignmentsBySource.has_key(alignment["source"]):
            alignmentsBySource[alignment["source"]] = []
        alignmentsBySource[alignment["source"]].append(alignment)
    
    with open(fullCorpusSource, 'r') as fullCorpusSourceD:
        fullSourceLines = fullCorpusSourceD.readlines()
    
    with open(fullCorpusTarget, 'r') as fullCorpusTargetD:
        fullTargetLines = fullCorpusTargetD.readlines()
        
    if len(fullSourceLines) != len(fullTargetLines):
        raise RuntimeError("full corpus is not aligned")
    
    for i in range(0, len(fullSourceLines)):
        sourceLine = fullSourceLines[i]
        if alignmentsBySource.has_key(sourceLine):
            targetLine = fullTargetLines[i]
            for alignment in alignmentsBySource[sourceLine]:
                if targetLine == alignment["target"]:
                    previousLine = fullTargetLines[i-1]
                    alignment["previous"] = previousLine
      
   
def analyseShortAnswers(source, target, translation, fullCorpusSource, fullCorpusTarget):

    alignments = getAlignment(source, target, translation)
    addHistory(alignments, fullCorpusSource, fullCorpusTarget)
    
    print "Analysis of short words"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if len(align["target"].split()) <= 3 and WER >= 0.5:
            if align.has_key("previous"):
                print "Previous line (reference):\t" + align["previous"]
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"
   


def analyseQuestions(source, target, translation):
    
    alignments = getAlignment(source, target, translation)
    
    print "Analysis of questions"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if "?" in align["target"] and WER >= 0.25:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"


def analyseBigErrors(source, target, translation):
    
    alignments = getAlignment(source, target, translation)
    
    print "Analysis of large translation errors"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if WER >= 0.6:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"

    
def extractNgrams(tokens, size):
    ngrams = []
    if len(tokens) < size:
        return ngrams   
    for i in range(size-1, len(tokens)):
        ngrams.append(" ".join(tokens[i-size+1:i+1]))
    print ngrams
    return ngrams
    

def getBLEUScore(reference, actual, ngrams=4):
    if len(reference) != len(actual):
        raise RuntimeError("reference and actual translation lines have different lengths")
    for i in range(0, len(reference)):
        reftokens = reference[i].split()
        actualtokens = actual[i].split()
        bp = min(1, (len(reftokens)+0.0)/len(actualtokens))
        product = bp
        for j in range(1, ngrams+1):
            refNgrams = set(extractNgrams(reftokens, j))
            if len(refNgrams) == 0:
                break
            actNgrams = set(extractNgrams(actualtokens, j))
            correctNgrams = refNgrams.intersection(actNgrams)
            precision = (len(correctNgrams)+0.0)/len(refNgrams)
            product *= precision
    return product


def getWER(reference, actual):
    refTokens = reference.split()
    actualTokens = actual.split()
    if len(refTokens) == 0:
        return len(actualTokens)
    if len(refTokens) < len(actualTokens):
        return getWER(actual, reference)
 
    # len(refTokens) >= len(actualTokens)
    if len(actualTokens) == 0:
        return len(refTokens)
 
    previous_row = range(len(actualTokens) + 1)
    for i, c1 in enumerate(refTokens):
        current_row = [i + 1]
        for j, c2 in enumerate(actualTokens):
            insertions = previous_row[j + 1] + 1 
            deletions = current_row[j] + 1       
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
 
    return (previous_row[-1]+0.0)/len(refTokens)
