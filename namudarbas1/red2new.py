#!/usr/bin/env python

import sys

sys.stdin = open("smapout.txt","r")
sys.stdout = open("red2outnew.txt","w")

current_key = None
current_set = {}
for line in sys.stdin:
    line = line.strip()
    marsrutas_data, zona = line.split('\t')

    if current_key == marsrutas_data:
        current_set.add(zona)

    else:
        if current_key != None:
            if len(current_set) > 1:
                print('%s\t%s' % (1, current_key))
        current_set = {zona}
        current_key = marsrutas_data


if current_key != None:
    if len(current_set) > 1:
        print('%s\t%s' % (1, current_key))
