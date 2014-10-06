
import sys, codecs, unicodedata


def extractWords(ngramFile):
    ngramContent = codecs.open(ngramFile, encoding="utf-8")
    lines = ngramContent.readlines()
    ngramContent.close()

    words = {}
    for l in lines:
        if not l[0].isalpha():
            continue
        split = l.split()
        word = split[0].strip()
        frequency = int(split[1].strip())
        if frequency < 500:
            continue
        if not word.isalpha():
            continue
        wlow = word.lower().encode("utf-8")
        words[wlow] = frequency
    return words


def remove_accents(word):
    normalised = unicodedata.normalize('NFKD',word.decode("utf-8"))
    return normalised.encode("ascii", "replace")
        
 

def pruneWrongAccents(words):
    
    words2 = {}
    for w in words:
        woaccent = remove_accents(w)
        if not words2.has_key(woaccent) or words[w] > words[words2[woaccent]]:
            words2[woaccent] = w
    
    for w in list(words.keys()):
        woaccent = remove_accents(w)
        if w != words2[woaccent]:
            otherword = words2[woaccent]
            if words[w] < (words[otherword]*100) and words[w] < 2000:         
                print ("Skipping %s %i (comparing to %s %i)"
                   %(w, words[w], words2[woaccent], words[words2[woaccent]]))
                del words[w]



if __name__ == '__main__':
                
    if len(sys.argv) != 3:
        print "Wrong number of arguments"
    
    words = extractWords(sys.argv[1])
    if "fr" in sys.argv[1]:
        pruneWrongAccents(words)
    
    with open(sys.argv[2], 'w') as outFile:
        for w in words:
            outFile.write(w + "\t" + str(words[w]) + "\n")
    

