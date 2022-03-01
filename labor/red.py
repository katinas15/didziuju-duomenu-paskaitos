import sys
 
sys.stdin = open("smapout.txt","r")
sys.stdout = open("redout.txt","w")

word2count = {}

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    if word in word2count:
        word2count[word] = word2count[word]+count
    else:
        word2count[word] = count
for word in word2count.keys():
    print ('%s\t%s'% ( word, word2count[word] ))