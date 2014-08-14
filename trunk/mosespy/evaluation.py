
import utils

def analyseShortAnswers(trueTarget, translation):
    if utils.countNbLines(trueTarget) != utils.countNbLines(translation):
        raise RuntimeError("Number of lines in actual and reference translations are different")

    with open(trueTarget, 'r') as trueT:
        trueLines = trueT.readlines()
    with open(translation, 'r') as actualT:
        actualLines = actualT.readlines()
    
    print "Analysis of short words"
    print "----------------------"
    for i in range(0, len(trueLines)):
        trueLine = trueLines[i].strip()
        actualLine = actualLines[i].strip()
        WER = getWER(trueLine, actualLine)
        if len(trueLine.split()) <= 3 and WER >= 0.5:
            print "Previous line:\t\t\t" + trueLines[i-1].strip()
            print "Current line (reference):\t" + trueLine
            print "Current line (actual):\t\t" + actualLine
            print "----------------------"
   


def analyseQuestions(trueTarget, translation):
    if utils.countNbLines(trueTarget) != utils.countNbLines(translation):
        raise RuntimeError("Number of lines in actual and reference translations are different")

    with open(trueTarget, 'r') as trueT:
        trueLines = trueT.readlines()
    with open(translation, 'r') as actualT:
        actualLines = actualT.readlines()
    
    print "Analysis of questions"
    print "----------------------"
    for i in range(0, len(trueLines)):
        trueLine = trueLines[i].strip()
        actualLine = actualLines[i].strip()
        WER = getWER(trueLine, actualLine)
        if "?" in trueLine and WER >= 0.25:
            print "Current line (reference):\t" + trueLine
            print "Current line (actual):\t\t" + actualLine
            print "----------------------"


def analyseBigErrors(trueTarget, translation):
    if utils.countNbLines(trueTarget) != utils.countNbLines(translation):
        raise RuntimeError("Number of lines in actual and reference translations are different")

    with open(trueTarget, 'r') as trueT:
        trueLines = trueT.readlines()
    with open(translation, 'r') as actualT:
        actualLines = actualT.readlines()
    
    print "Analysis of large translation errors"
    print "----------------------"
    for i in range(0, len(trueLines)):
        trueLine = trueLines[i].strip()
        actualLine = actualLines[i].strip()
        WER = getWER(trueLine, actualLine)
        if WER >= 0.6:
            print "Current line (reference):\t" + trueLine
            print "Current line (actual):\t\t" + actualLine
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
