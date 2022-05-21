# spark-submit main.py
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

lines = sc.textFile("namudarbas1/duom_full.txt")
def FM(line):
  line = line.strip()
  line = line[2:-2]
  susstring = line.split('}}{{')
  return susstring
A=lines.flatMap(FM)
print(A.take(3))

def MapF(stopas):
  parstrings = stopas.split('}{')
  diena = None
  siuntos = None
  geografine_zona = None
  klientu_skaicius = None
  for parstring in parstrings: 
    (vardas, reiksme) = parstring.split('=')
    if(reiksme != '' and vardas == 'sustojimo savaites diena'):
      diena = reiksme
    if(reiksme != '' and vardas == 'siuntu skaicius'):
      siuntos = reiksme
    if(reiksme != '' and vardas == 'geografine zona'):
      geografine_zona = reiksme
    if(reiksme != '' and vardas == 'sustojimo klientu skaicius'):
      klientu_skaicius = reiksme
  try:
    diena = int(diena)
    siuntos = int(siuntos)
    klientu_skaicius = int(reiksme)
  except:
    diena = None
    siuntos = None
    geografine_zona = None
    klientu_skaicius = None

  key = str(diena) + '_' + str(geografine_zona)
  return(key, [siuntos, klientu_skaicius])
B = A.map(MapF)
# print(B.take(5))
test = B.take(5)

C = B.filter(lambda pair : pair[1][0] != None and pair[1][1] != None and pair[0].find('None') == -1)
# print(C.take(5))

def red (a,b):
  return (a[0] + b[0], a[1] + b[1])
D = C.reduceByKey(red)
E = D.sortByKey()
# ats = D.collect()
E.saveAsTextFile('hdfs:///user/maria_dev/nd2ats')