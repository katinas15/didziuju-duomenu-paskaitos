#!/usr/bin/env python
import sys

sys.stdin = open("text.txt","r")
sys.stdout = open("mapout.txt","w")


for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words: 
        print('%s\t%s' % (word, 1))