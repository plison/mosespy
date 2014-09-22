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
import urwid
from mosespy.corpus import ReferenceCorpus


def startAnalysis(results, initCondition=None):
    """Analyse the translation results (encoded as a translated corpus)
    under a particular set of conditions.
    
    """
    if not initCondition:
        initCondition=Condition()
    if not isinstance(results, ReferenceCorpus):
        raise RuntimeError("results must be of type ReferenceCorpus")
    AnalysisUI(initCondition, results)
  

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
            if not any([inT in t for t in pair.target]):
                return False
        for inT in self.inTranslation:
            if pair.translation and inT not in pair.translation:
                return False
        
        if not (self.length[0] <= len(pair.translation.split())<= self.length[1]):
            return False
        
        if pair.translation and not any([self.wer[0] <= getWER(t, pair.translation) <= 
                                         self.wer[1] for t in pair.target if t.strip()]):
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
    
 
        
class ConditionButton(urwid.Button):
    
    def __init__(self, text, condBox):
        self.condBox = condBox
        urwid.Button.__init__(self, text)
        
 
class ConditionBox(urwid.ListBox):
    
    def __init__(self, condition, topUI):
        elList = []
        elList.append(urwid.Text("Analysis of errors under\nthe following criteria:"))
        elList.append(urwid.AttrMap(urwid.Divider(), "bright"))
    
        lengthCols = [(28, urwid.IntEdit("Sentence length:  from ", 
                                         str(condition.length[0]))), 
                      (9, urwid.IntEdit(" to ",str(condition.length[1])))]
        elList.append(urwid.Columns(lengthCols))
        werCols = [(28, urwid.Edit("Word Error Rate:  from ",
                                   str(condition.wer[0]))), 
                   (9, urwid.Edit(" to ",str(condition.wer[1])))]
        elList.append(urwid.Columns(werCols))   
        elList.append(urwid.Edit("Source substring: ", ";".join(condition.inSource)))
        elList.append(urwid.Edit("Target substring: ",";".join(condition.inTarget)))
        elList.append(urwid.Edit("Translation substring: ",";".join(condition.inTranslation)))
        
        elList.append(urwid.Divider())
        elList.append(urwid.Columns([(20,urwid.AttrMap(ConditionButton("Search errors", self), None, focus_map='reversed'))]))
        walker = urwid.SimpleFocusListWalker(elList)

        urwid.connect_signal(elList[8][0].original_widget, 'click', 
                             lambda x : x.condBox.updateCondition())
        
        self.topUI = topUI
        urwid.ListBox.__init__(self, walker)
        
    
    def updateCondition(self):
        cond = Condition()
        cond.length = (int(self.body[2][0].edit_text),
                       int(self.body[2][1].edit_text))
        cond.wer = (float(self.body[3][0].edit_text),
                    float(self.body[3][1].edit_text))
        cond.inSource = [t for t in self.body[4].edit_text.split(";") if t.strip()]
        cond.inTarget = [t for t in self.body[5].edit_text.split(";") if t.strip()]
        cond.inTranslation = [t for t in self.body[6].edit_text.split(";") if t.strip()]
        self.topUI.updateErrorBox(cond)
        
        
        
           

class ErrorBox(urwid.ListBox):
    
    def __init__(self, aligns):
        elList = []
        self.aligns = aligns
        for i in range(0, len(aligns)):
            widget = self.getAlignWidget(i)
            elList.append(widget)
        walker = urwid.SimpleFocusListWalker(elList)
        urwid.ListBox.__init__(self, walker)
        self.lineInFocus = 0
        
    
    def getAlignWidget(self, number, addHistory=False):
        tab = " " * (len(str(number+1))+2)
        pair = self.aligns[number]
        refsText = [(tab+"Reference:    "+ t) for t in pair.target if t.strip()]
        text= ("%i. Source:       %s"%((number+1), pair.source) + "\n" 
               + "\n".join(refsText) + "\n")
        if addHistory:
            text += tab + "Previous:     " + pair.targethistory + "\n"    
        WER = min([getWER(t, pair.translation) for t in pair.target])
        text += tab + "Translation:  " + pair.translation + " (WER=%i%%)\n"%(WER*100)
        widget = urwid.Button(text)
        urwid.connect_signal(widget, 'click', self.selection, number)
        return widget
    
    
    def selection(self, _, choice):
        addHistory = self.lineInFocus!=choice
        self.body[choice] = self.getAlignWidget(choice, addHistory)
        self.lineInFocus = choice if addHistory else 0

   

class AnalysisUI():

    def __init__(self, condition, results):
        self.aligns = results.getAlignments(addHistory=True)        
        self.focus = None
        top = urwid.Columns([(40, ConditionBox(condition, self)), 
                             (80,ErrorBox([]))], 2, 1)  
        self.mainLoop = urwid.MainLoop(top)
        self.updateErrorBox(condition)
        self.mainLoop.run()
          
    
    def updateErrorBox(self, newCondition):
        errors = []
        for a in self.aligns:
            if newCondition.isSatisfiedBy(a):
                errors.append(a)
        cols = urwid.Columns([(40, ConditionBox(newCondition, self)), 
                              (80, ErrorBox(errors))], 2, 1) 
        self.mainLoop.widget = cols 
        
                

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


       
        