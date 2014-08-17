
import nlp, string
     
     
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
        WER = nlp.getWER(align["target"], align["translation"])
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
        WER = nlp.getWER(align["target"], align["translation"])
        if "?" in align["target"] and WER >= 0.25:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"


def analyseBigErrors(alignments):
    
    
    print "Analysis of large translation errors"
    print "----------------------"
    for align in alignments:
        WER = nlp.getWER(align["target"], align["translation"])
        if WER >= 0.7:
            print "Source line:\t\t\t" + align["source"]
            print "Current line (reference):\t" + align["target"]
            print "Current line (actual):\t\t" + align["translation"]
            print "----------------------"

        