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
    alignments = results.getAlignments(addHistory=True)
    AnalysisUI(initCondition, alignments).run()
  

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
        
        if not any([self.length[0] <= len(t.split()) <= self.length[1] for t in pair.target]):
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
    
    
 
class ConditionBox(urwid.ListBox):
    
    def __init__(self, condition):
        elList = []
        elList.append(urwid.Text("Analysis of errors under the following criteria:"))
        elList.append(urwid.Text(str(condition)))
        walker = urwid.SimpleFocusListWalker(elList)
        urwid.ListBox.__init__(self, walker)
        

class ErrorBox(urwid.ListBox):
    
    def __init__(self, aligns):
        elList = []
        for i in range(0, len(aligns)):
            a = aligns[i]
            but = urwid.Button("%i. Source:       %s"%((i+1), a.source))
            urwid.connect_signal(but, 'click', self.selection, i)
            elList.append(but)
            tab = " " * (len(str(i))+2)
            for t in a.target:
                if t.strip():
                    elList.append(urwid.Text(tab + "  Reference:    "+ t))
            WER = min([getWER(t, a.translation) for t in a.target])
            elList.append(urwid.Text(tab + "  Translation:  " + a.translation
                                      + " (WER=%i%%)"%(WER*100)))
            elList.append(urwid.Divider())
        walker = urwid.SimpleFocusListWalker(elList)
        urwid.ListBox.__init__(self, walker)
        self.aligns = aligns
        self.lineInFocus = 0
    
    
    def selection(self, _, choice):
        tab = " " * (len(str(choice))+2)
        align = self.aligns[choice]
        indexList = choice*4
        if self.lineInFocus == indexList:
            del self.body[indexList + 2]
            self.lineInFocus = 0
        elif align.targethistory:
            previousText = urwid.Text(tab + "  Previous:     "+ align.targethistory)         
            self.body.insert(indexList + 2, previousText)
            self.body.set_focus(indexList)
            self.lineInFocus = indexList
    

    def _keypress_down(self, size):
        (maxcol, maxrow) = size

        middle, top, bottom = self.calculate_visible(
            (maxcol,maxrow), True)
        if middle is None: return True

        focus_row_offset,focus_widget,focus_pos,focus_rows,cursor=middle
        trim_bottom, fill_below = bottom

        row_offset = focus_row_offset + focus_rows
        rows = focus_rows

        # look for selectable widget below
        pos = focus_pos
        widget = None
        for widget, pos, rows in fill_below:
            if rows and widget.selectable():
                # this one will do
                self.change_focus((maxcol,maxrow), pos,
                    row_offset, 'above')
                return
            row_offset += rows

        # at this point we must scroll
        row_offset -= 1
        self._invalidate()

        while row_offset < maxrow:
            self.body.insert(0, urwid.Text("HERE %i"%(row_offset)))
            # need to scroll in another candidate widget
            widget, pos = self.body.get_next(pos)
            if widget is None:
                # cannot scroll any further
                return True # keypress not handled
            rows = widget.rows((maxcol,))
            if rows and widget.selectable():
                # this one will do
                self.change_focus((maxcol,maxrow), pos,
                    row_offset, 'above')
                return
            row_offset += rows

        if not focus_widget.selectable() or focus_row_offset+focus_rows-1 <= 0:
            # just take bottom one if current is not selectable
            # or if focus has moved out of view
            if widget is None:
                self.shift_focus((maxcol,maxrow),
                    row_offset-rows)
                return
            # FIXME: catch this bug in testcase
            #self.change_focus((maxcol,maxrow), pos,
            #    row_offset+rows, 'above')
            self.change_focus((maxcol,maxrow), pos,
                row_offset-rows, 'above')
            return

        # check if cursor will stop scroll from taking effect
        if cursor is not None:
            x,y = cursor
            if y+focus_row_offset-1 < 0:
                # cursor position is a problem,
                # choose another focus
                if widget is None:
                    # try harder to get next widget
                    widget, pos = self.body.get_next(pos)
                    if widget is None:
                        return # can't do anything
                else:
                    row_offset -= rows

                if row_offset >= maxrow:
                    # must scroll further than 1 line
                    row_offset = maxrow-1

                self.change_focus((maxcol,maxrow),pos,
                    row_offset, 'above', )
                return

        # if all else fails, keep the current focus.
        self.shift_focus((maxcol,maxrow), focus_row_offset-1)

   

   

class AnalysisUI(urwid.MainLoop):

    def __init__(self, condition, aligns):
        self.aligns = []
        for a in aligns:
            if condition.isSatisfiedBy(a):
                self.aligns.append(a)
        self.focus = None
        columns = [(30, ConditionBox(condition)), (100,ErrorBox(aligns))]
        top = urwid.Columns(columns, 2, 1)
        urwid.MainLoop.__init__(self, top)
        
                

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


       
        