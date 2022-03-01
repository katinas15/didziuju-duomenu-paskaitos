#!/usr/bin/env python
import sys

sys.stdin = open("duom_cut.txt","r")
sys.stdout = open("mapout2.txt","w")


for line in sys.stdin:
  line = line.strip()[2:-2]
  elements = line.split('}}{{')
  for el in elements: 
    # print(1, el)
    parameters = line.split('}{')
    marsrutas = None
    geografine_zona = None
    sustojimo_data = None
    for param in parameters:
      k, v = param.split('=')
      k = k.replace('}','').replace('{','')
      v = v.replace('}','').replace('{','')
      if k == 'marsrutas':
        marsrutas = int(v)
      elif k == 'geografine zona':
        geografine_zona = v
      elif k == 'sustojimo data':
        sustojimo_data = v

    if marsrutas != None and geografine_zona != None and sustojimo_data != None and marsrutas != '' and geografine_zona != '' and sustojimo_data != '' and marsrutas != ' ' and geografine_zona != ' ' and sustojimo_data != ' ':
      # print(f"{marsrutas}_{sustojimo_data}\t{geografine_zona}")
      key = str(marsrutas) + '_' + str(sustojimo_data)
      print('%s\t%s' % (key, geografine_zona))

      # panaudoti set gauti skirtingas zonas marsrute
      # po map turi buti marsrutas_data, 158_2021-01-01
      # if len set daugiau uz 1, tada emit(marsrutas)
    