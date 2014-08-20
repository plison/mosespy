# -*- coding: utf-8 -*-


__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date: $"    


import string
      
def printSummary(alignments):
    
    analyseAllErrors(alignments)
    #analyser.analyseShortAnswers(alignments)
    #analyseQuestions(alignments)
    #analyseBigErrors(alignments)
       


def compare(line1, line2):
    line1 = line1.lower().translate(string.maketrans("",""), string.punctuation).strip()
    line2 = line2.lower().translate(string.maketrans("",""), string.punctuation).strip()
    return line1 == line2
    
    
def analyseAllErrors(alignments):
    print "Analysis of all errors"
    print "----------------------"
    for align in alignments:
        if not compare(align["target"], align["translation"]):
            if align.has_key("previoustarget"):
                print "Previous line (reference):\t" + align["previoustarget"]
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"

     
def analyseShortAnswers(alignments):

    print "Analysis of short words"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if len(align["target"].split()) <= 3 and WER >= 0.5:
            if align.has_key("previoustarget"):
                print "Previous line (reference):\t" + align["previoustarget"]
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"
   


def analyseQuestions(alignments):
        
    print "Analysis of questions"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if "?" in align["target"] and WER >= 0.25:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"


def analyseBigErrors(alignments):
    
    
    print "Analysis of large translation errors"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if WER >= 0.7:
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
  

        