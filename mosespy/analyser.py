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
from mosespy.corpus import TranslatedCorpus



class ErrorAnalyser():
    
    def __init__(self, *conditions):
        if len(conditions) > 0:
            self.conditions = conditions
        else:
            self.conditions = [Condition()]
        
    def analyseResults(self, results):
        """Analyse the translation results (encoded as a translated corpus)
        under the set of conditions for the analyser.
        
        """
        if not isinstance(results, TranslatedCorpus):
            raise RuntimeError("results must be of type TranslatedCorpus")
        alignments = results.getAlignments(addHistory=True)
        for cond in self.conditions:
            print "Analysis of errors under the condition: %s"%(str(cond))
            print "----------------------"
            incr = 1
            for align in alignments:
                if cond.isSatisfiedBy(align):
                    print "%i. Source line:\t\t %s"%(incr, align.source)
                    if align.targethistory:
                        print "    [Previous line (reference):\t" + align.targethistory + "]"
                    print "    Current line (reference):\t" + align.target
                    print "    Current line (actual):\t" + align.translation
                    print "----------------------"
                    incr += 1
      

class Condition():
    """Condition on an alignment pair, made of:
        - substrings that must be found in the source, target and/or 
        translation sentences
        - lower and upper bounds on the Word Error Rate between target 
        and translation
        - lower and upper bounds on the sentence length (on target)
        
    These conditions are interpreted conjunctively, i.e. all parts must
    be true in order for the condition to be satisfied.  Disjunctive
    conditions must use the DisjunctiveCondition class.
    
    """
    def __init__(self, **kwargs):
        """Creates an empty condition.
        
        """
        self.inSource = []
        self.inTarget = []
        self.inTranslation = []
        self.wer = (0, 1)
        self.length = (0, 100)

        for key in ['inSource', 'inTarget', 'inTranslation']:
            if key in kwargs and isinstance(kwargs[key], basestring):
                getattr(self, key).append(kwargs[key])
            elif key in kwargs and isinstance(kwargs[key], list):
                getattr(self, key).extend(kwargs[key])       
  
        if "wer" in kwargs:
            self.wer = kwargs["wer"]
        if "length" in kwargs:
            self.length = kwargs["length"]
        
     
    def isSatisfiedBy(self, pair):
        """Returns true if the alignment pair satisfied all elements
        of the condition, and false otherwise.
        
        """
        for inS in self.inSource:
            if inS not in pair.source:
                return False
        for inT in self.inTarget:
            if inT not in pair.target:
                return False
        for inT in self.inTranslation:
            if pair.translation and inT not in pair.translation:
                return False
            
        targetSplits = pair.target.split()
        if (len(targetSplits) < self.length[0] or 
            len(targetSplits) > self.length[1]):
            return False
        
        if pair.translation:
            WER = getWER(pair.target, pair.translation)
            if (WER < self.wer[0] or WER > self.wer[1]):
                return False
        
        return True
    
    def __str__(self):
        """Returns a string representation of the condition.
        
        """
        subconds = []
        for inS in self.inSource:
            subconds.append("'%s' in source"%(inS))
        for inT in self.inTarget:
            subconds.append("'%s' in target"%(inT))
        
        if self.length != (0, 100):
            subconds.append("sentence length in [%i,%i]"
                            %(self.length[0], self.length[1])) 
             
        if self.wer != (0, 1):
            subconds.append("WER in [%.2f,%.2f]"
                            %(self.wer[0], self.wer[1]))
            
        return " and ".join(subconds) if subconds else "True"
    
    

class OrCondition():
    """Representation of a disjunctive condition, i.e. the  condition
    will be true if at least one subcondition is true.
    
    """
    def __init__(self, *subconditions):
        """Creates a new condition made of the disjunctive combination
        of the subconditions.
        
        """
        self.subconditions = subconditions
        
    def isSatisfiedBy(self, pair):
        """Returns true if at least one subcondition is satisfied by the
        alignment pair, and false otherwise.
        
        """
        for subcondition in self.subconditions:
            if subcondition.isSatisfiedBy(pair):
                return True
        return False
    
    def __str__(self):
        """Returns a string representation of the condition.
        
        """
        return " or ".join([str(subcond) for subcond in self.subconditions])
        
         

def extractNgrams(tokens, size):
    """Extract the n-grams of a particular size in the list of tokens.
    
    """
    ngrams = []
    if len(tokens) < size:
        return ngrams   
    for i in range(size-1, len(tokens)):
        ngrams.append(" ".join(tokens[i-size+1:i+1]))
    return ngrams
    

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


       
        