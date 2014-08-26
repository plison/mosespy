# -*- coding: utf-8 -*-

# =================================================================                                                                   
# Copyright (C) 2014-2017 Pierre Lison (plison@ifi.uio.no)
                                                                            
# Permission is hereby granted, free of charge, to any person 
# obtaining a copy of this software and associated documentation 
# files (the "Software"), to deal in the Software without restriction, 
# including without limitation the rights to use, copy, modify, merge, 
# publish, distribute, sublicense, and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, 
# subject to the following conditions:

# The above copyright notice and this permission notice shall be 
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# =================================================================  


"""Module employed to analyse translation results.

"""

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date::                      $"


import string
from collections import Counter  
from mosespy.corpus import TranslatedCorpus
     
def analyseResults(results):
    if not isinstance(results, TranslatedCorpus):
        raise RuntimeError("results must be of type TranslatedCorpus")
    alignments = results.getAlignments(addHistory=True)
 #   analyseShortAnswers(alignments)
 #   analyseQuestions(alignments)   
    analyseErrorsWithSubstring(alignments, "toch")
        
def analyseAllErrors(alignments):
    print "Analysis of all errors"
    print "----------------------"
    for align in alignments:
        if not _compare(align["target"], align["translation"]):
            if align.has_key("previoustarget"):
                print "Previous line (reference):\t" + align["previoustarget"]
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"

     
def analyseShortAnswers(alignments):

    print "Analysis of short words"
    print "----------------------"
    errorDict = Counter()
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if len(align["target"].split()) <= 3 and WER >= 0.5:
            tupleTrans = _translationTuple(align)
            errorDict[tupleTrans] += 1
    for transError in errorDict.most_common(20):
        print "Source line:\t\t\t" + transError[0][0]
        print "Target line (reference):\t" + transError[0][1]
        print "Target line (actual):\t\t" + transError[0][2]
        print "Number of occurrences: " + str(transError[1])
        print "----------------------"
   
 


def analyseErrorsWithSubstring(alignments, substring):
       
    print "Analysis of questions"
    print "----------------------"
    for align in alignments:
        WER = getWER(align["target"], align["translation"])
        if substring in align["target"].lower() and WER >= 0.3:
            print "Source line:\t\t\t" + align["source"]
            if align.has_key("previoustarget"):
                print "Previous line (reference):\t" + align["previoustarget"]
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
    """Extract the n-grams of a particular size in the list of tokens.
    
    """
    ngrams = []
    if len(tokens) < size:
        return ngrams   
    for i in range(size-1, len(tokens)):
        ngrams.append(" ".join(tokens[i-size+1:i+1]))
    return ngrams
    

def getBLEUScore(reference, actual, ngrams=4):
    """Returns the BLEU score between the reference and actual translations,
    with an n-gram of a particular maximum size.
    
    """
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
    """Returns the Word Error Rate between the reference and actual
    translations.
    
    """
    refTokens = reference.lower().translate(string.maketrans("",""), 
                                            string.punctuation).split()
    actualTokens = actual.lower().translate(string.maketrans("",""), 
                                            string.punctuation).split()
    if len(refTokens) == 0:
        return len(actualTokens)
    if len(refTokens) < len(actualTokens):
        return getWER(actual, reference)
 
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
  

def _translationTuple(align):
    source = align["source"].lower().strip()
    target = align["target"].lower().strip()
    trans = align["translation"].lower().strip()
    return (source, target, trans)
                                                   

def _compare(line1, line2):
    line1 = line1.lower().translate(string.maketrans("",""), string.punctuation).strip()
    line2 = line2.lower().translate(string.maketrans("",""), string.punctuation).strip()
    return line1 == line2
    
        