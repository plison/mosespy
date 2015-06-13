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


"""Module for generating a webpage to analyse the translation outputs
in an interactive manner. The web page displays a table with the source
and translation sentences, with tooltips to easily compare the generated
translations with the references.  The user can filter the translation
outputs according to various criteria (sentence length, word error rate,
occurrence of particular substrings, etc.).

"""
__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'


import re
from mosespy.corpus import ReferenceCorpus
from mosespy.system import Path
import xml.etree.cElementTree as ET


def generateHTML(refCorpus, corpusProcessor):
    """Generates the webpage for the given aligned corpus containing 
    source, (reference) targets and actual translation. The method 
    generates the webpage and returns its local path.
    
    """ 

    if not isinstance(refCorpus, ReferenceCorpus) or not refCorpus.translation:
        raise RuntimeError(str(refCorpus) + " is not a reference corpus with translations")

    readCorpus = corpusProcessor.revertReferenceCorpus(refCorpus)
    tokenisedAligns = refCorpus.getAlignments()
    untokenisedAligns = readCorpus.getAlignments()
    bleu, _ = corpusProcessor.getBleuScore(refCorpus)

    doc = "<html>\n"
    
    doc += "<head>\n"
    doc += head
    javascript = open(Path(__file__).getUp() + "/data/analyser.js", 'r').read()
    doc += "<script>" + javascript + "</script>\n"
    doc += """<script id="transdata" type="text/xmldata">"""
    doc += _generateXML(tokenisedAligns, untokenisedAligns)
    doc += "</script>\n" 
    doc += "</head>\n"
    
    doc += """<body>\n<form action="">"""
    doc += """<h2>Evaluation results (<span id="nblines">%i</span> translations):</h2>\n"""%len(tokenisedAligns)
    doc += """<table id="outputsinfo">\n"""
    pathTag = lambda p : """<i><a href="%s">%s</a></i>"""%(p,p)
    doc += """<tr><td><b>Source file: </b></td><td>%s</td></tr>\n"""%(pathTag(refCorpus.sourceCorpus))
    doc += """<tr><td><b>Reference file(s): </b></td><td>%s</td></tr>\n"""%("<br>".join([pathTag(r) for r in refCorpus.refCorpora]))
    doc += """<tr><td><b>Translation file: </b></td><td>%s</td></tr>\n"""%(pathTag(refCorpus.translation))
    doc += """<tr><td><b>Total BLEU score: </b></td><td>%.2f</td></tr>\n"""%(bleu)
    doc += "</table><br>\n"
    doc += """<h4 style="display: inline-block;">Filtered translation outputs"""
    doc += """(<span id="nboutputs">0</span>/%i):</h4>"""%len(tokenisedAligns)
    doc += """<span style="margin-left:250px;">Maximum table size:&nbsp;&nbsp;</span>"""
    doc += """<select name="tablesize" onchange="this.form.submit()">
              <option selected="true">100</option> <option>500</option>
              <option>1000</option> <option>no limit</option> </select>"""
    doc += """<table class="outputs">
        <tr class=\"header\">
            <td width=\"2%%\"></td>
            <td width=\"49%%\">&nbsp;&nbsp;<b>Source (%s)</b></td>
            <td width=\"49%%\">&nbsp;&nbsp;<b>Target (%s)</b></td>
        </tr>
    """%(refCorpus.sourceLang, refCorpus.targetLang)
    
    doc += "</table><br>\n"
    doc += filterbox
    doc += "</body>\n"
    doc += "</html>"
    
    htmlFile = refCorpus.getStem() + ".html"
    o = open(htmlFile, 'w')
    o.write(doc)
    o.close()
    return htmlFile

        
def _generateXML(tokenisedAligns, untokenisedAligns):
    """Generate the XML representation of the reference corpus.  The corpus must be
    a reference corpus with associated translations. The generated XML can then
    be included as data to display the translation outputs in a web page.
    
    """
       
    root = ET.Element("translations")

    countTokens = lambda s : str(len([t for t in s.split(" ")]))
    wordTokens = lambda s : [t for t in s.split(" ") if re.compile(r'[\w_]+', re.UNICODE).search(t)]
    countWords = lambda s : str(len(wordTokens(s)))
    contiguousTokens = lambda t,u : ",".join([str(k) for k in _getContiguousTokens(t,u)])
    
    for i in range(0, len(tokenisedAligns)):         
        
        tokAlign = tokenisedAligns[i]
        untokAlign = untokenisedAligns[i]
 
        pairEl = ET.SubElement(root, "pair")
       
        tokAlign.source = _clean(tokAlign.source)
        sourceEl = ET.SubElement(pairEl, "source")
        sourceEl.text = tokAlign.source.decode("UTF-8")   
        sourceEl.set("nbtokens", countTokens(tokAlign.source))
        sourceEl.set("nbwords", countWords(tokAlign.source))
        sourceEl.set("contiguous", contiguousTokens(tokAlign.source, untokAlign.source))
      
        tokAlign.translation = _clean(tokAlign.translation)
        translation_words = wordTokens(tokAlign.translation)
         
        closest = None; closest_punct = None
        for j in range(0, len(tokAlign.target)):
            reference = _clean(tokAlign.target[j])
            refEl = ET.SubElement(pairEl, "reference")
            refEl.text = reference.decode("UTF-8")
            refEl.set("nbtokens", countTokens(reference))
            refEl.set("nbwords", countWords(reference))
            refEl.set("contiguous", contiguousTokens(reference, untokAlign.target[j]))
            grid = EditGrid(" ".join(wordTokens(reference)), " ".join(translation_words))
            refEl.set("wer", "%.2d"%grid.wer)
            if not closest or closest.edit.wer > grid.wer:
                editsRef = _completeEdits(grid.refTokens, reference)
                editsTrans = _completeEdits(grid.actualTokens, tokAlign.translation)
                closest = {"el":refEl, "edits_ref":editsRef, "edits_trans":editsTrans}
            grid = EditGrid(reference, tokAlign.translation)
            refEl.set("wer_punct", "%.2d"%grid.wer)
            if not closest_punct or closest_punct.edit.wer > grid.wer:
                editsRef = [t.edit for t in grid.refTokens]
                editsTrans = [t.edit for t in grid.actualTokens]
                closest_punct = {"el":refEl, "edits_ref":editsRef, "edits_trans":editsTrans}
        
        closest["el"].set("edits", "".join(closest["edits_ref"]))
        closest_punct["el"].set("edits_punct", "".join(closest_punct["edits_ref"]))

        transEl = ET.SubElement(pairEl, "translation")
        transEl.text = tokAlign.translation.decode("UTF-8")
        transEl.set("nbtokens", countTokens(tokAlign.translation))
        transEl.set("nbwords", countWords(tokAlign.translation))
        transEl.set("contiguous", contiguousTokens(tokAlign.translation, untokAlign.translation))
        transEl.set("edits", "".join(closest["edits_trans"]))
        transEl.set("edits_punct", "".join(closest_punct["edits_trans"]))
        
                    
    return ET.tostring(root)
 
        

def _getContiguousTokens(tokenised, untokenised):
    """Returns the list of tokens that are contiguous to the previous
    token in the untokenised version.  These tokens are typically punctuation
    marks.
    
    """
    contiguousToks = []
    tokens = tokenised.split(" ")
    tokens2 = untokenised.split(" ")
    k = 0
    for j in range(0, len(tokens)):
        tokens2[k] = tokens2[k].replace(tokens[j], '', 1)           
        if tokens2[k] == '':
            k += 1
        else:
            contiguousToks.append((j+1))
    if len(tokens) != (len(untokenised.split(" ")) + len(contiguousToks)):
        print "Warning, error trying to find contiguous tokens"
        print "Tokenised string: " + tokenised
    return contiguousToks


def _completeEdits(filteredTokens, fullSentence):
    """Given a set of tokens (along with their edit path), generates
    a complete list of edit paths, adding the edit 'p' in case the
    token is not in the filtered tokens.
    
    """
    allTokens = fullSentence.split(" ")
    edits = []
    k = 0
    for t in allTokens:
        if k < len(filteredTokens) and t == filteredTokens[k].token:
            edits.append(filteredTokens[k].edit)
            k += 1
        else:
            edits.append('p')
    result = "".join(edits)
    return result

 

def _clean(sentence):
    """Performs some basic cleanup of the sentence.
    
    """
    cleaned = sentence.replace("&quot;", "\"").replace("&apos;", "'")
    cleaned = cleaned.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return cleaned
           
    

head = """
<title>Translation results</title>
<link rel="stylesheet" type="text/css" href="http://folk.uio.no/plison/css/mosespy.css">
<script type="text/javascript"
    src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>
<script type="text/javascript"
    src="https://cdnjs.cloudflare.com/ajax/libs/qtip2/2.2.1/basic/jquery.qtip.min.js"></script>
"""
        
        
filterbox = """
    <script>
     function togglefilter() {
        $('.filter').toggle();
        if ($('#toggler').text() == '<<<') {
            $('.outputs').css('padding-right', $('.outputs').attr('alt'));
            $('#toggler').html('&#62;&#62;&#62;');
        } else {
            $('.outputs').attr('alt', $('.outputs').css('padding-right'));
            $('.outputs').css('padding-right', '30');
            $('#toggler').html('&#60;&#60;&#60;');
        }
        return false;
    }
    </script>
    <div class="filter">
        <h3>Filter results:</h3>

            <ol style="list-style-type: square; padding: 0; margin: 10px">
                <li><p class="condition">Sentence length (nb. of tokens):</p> <br>
                    Min: <input type="text" name="minsource" size="4" /> &nbsp;
                    Max: <input type="text" name="maxsource" size="4" />&nbsp; [Source]<br> 
                    Min: <input type="text" name="minref" size="4" /> &nbsp;
                    Max: <input type="text" name="maxref" size="4" />&nbsp; [Reference]<br> 
                    Min: <input type="text" name="mintrans" size="4" /> &nbsp;
                    Max: <input type="text" name="maxtrans" size="4" />&nbsp; [Translation] 
                    <br></li>

                <li><p class="condition">Word Error Rate (in &#37;):</p> <br>
                    Min: <input type="text" name="minwer" size="4" /> &nbsp;
                    Max: <input  type="text" name="maxwer" size="4" /><br><br></li>
                    
                <li style="list-style-type: none;">
                <input type="checkbox"  name="punct" value="yes">
                <span  style="display: inline-block; vertical-align: top; width: 220px;">Take
                        punctuation into account for the length and WER</span> <br>
                </li>
                <li><p class="condition">Source substring(s):</p> <br>
                <textarea rows="1" cols="35" name="sourcesub"></textarea><br></li>

                <li><p class="condition">Reference substring(s):</p> <br>
                <textarea rows="1" cols="35" name="refsub"></textarea><br></li>

                <li><p class="condition">Translation substring(s):</p> <br>
                <textarea rows="1" cols="35" name="transsub"></textarea><br> <i>(use ; as separator)</i><br><br></li>
                
                 <li style="list-style-type: none;">
                <input type="checkbox"  name="tokenised" value="yes">
                <span  style="display: inline-block; vertical-align: top; width: 220px;">Display the
                sentences in tokenised format</span> <br>
                </li>
            </ol>
        <span style="position: absolute; right: 20px; bottom: 20px"><button>
                Update</button></span>
   </div>
    <div style="position: fixed; right: 70; top: 40;">
        <a id='toggler' href='#' onclick="togglefilter()">&#62;&#62;&#62;</a>
    </div>
    """

   
class EditGrid:
    """Grid for the edit distance between two strings (one is typically a
    reference translation, while the other one is a generated translation).
   
    """
    
    def __init__(self, reference, actual):

        self.refTokens = [EditToken(t) for t in reference.split()]
        self.actualTokens = [EditToken(t) for t in actual.split()]
        
        self.distgrid, self.editgrid = self.__createGrids()
        self.wer = (float(self.distgrid[-1][-1])/len(self.refTokens)*100)
        
        i,j = len(self.refTokens), len(self.actualTokens)
        while i!=0 or j!=0:
            decision = self.editgrid[i][j]
            if decision != 'i':
                self.refTokens[i-1].edit = decision
                i = i-1
            if decision != 'd':
                self.actualTokens[j-1].edit = decision
                j = j-1


    def __createGrids(self):
        """Creates a distance grid specifying the edit distance between
        any pairs of prefix of the two strings.
        
        """
        nbColumns = len(self.actualTokens) + 1
        nbRows = len(self.refTokens) + 1
        distgrid = [[0 for _ in range(nbColumns)] for _ in range(nbRows)] 
        editgrid = [['' for _ in range(nbColumns)] for _ in range(nbRows)] 
        
        for i in range(1,nbColumns):
            distgrid[0][i] = distgrid[0][i-1] + 1        
            editgrid[0][i] = 'i'
           
        for i in range(1,nbRows):
            refToken = self.refTokens[i-1]
            distgrid[i][0] = distgrid[i-1][0] + 1
            editgrid[i][0] = 'd'
               
            for j in range(1, nbColumns):
                actualToken = self.actualTokens[j-1]
                equalToken = refToken==actualToken       
                
                insert_dist = distgrid[i-1][j] + 1
                delete_dist = distgrid[i][j-1] + 1
                substitute_dist = distgrid[i-1][j-1] + (not equalToken)
                distance = min(insert_dist, delete_dist, substitute_dist) 
                distgrid[i][j] = distance  
                if distance == substitute_dist:
                    editgrid[i][j] = 'g' if equalToken else 's'
                else:
                    editgrid[i][j] = 'i' if distance == delete_dist else 'd'
        
        return distgrid,editgrid
               
    def __str__(self):
        """Returns the edit distance table.
        
        """
        result = "Edit distance:\n"
        result = result + " \t \t" + "\t".join(str(t) for t in self.actualTokens) + "\n"
        result = result +   " \t" + "\t".join(str(j) for j in self.distgrid[0]) + "\n"
        for i in range(0, len(self.refTokens)):
            result = result + str(self.refTokens[i]) + "\t"
            result = result + "\t".join(str(j) for j in self.distgrid[i+1]) + "\n"              
        return result
        

 

class EditToken:
    """Token used to calculate the edit distance.
    
    """
    def __init__(self, token):
        self.token = token
        self.lowered = token.lower()
        self.edit = None
    
    def __str__(self):
        return self.token
    
    def __repr__(self):
        return self.token
    
    
    def __eq__(self, other):
        if isinstance(other,EditToken):
            return other.lowered == self.lowered
        else:
            return False
