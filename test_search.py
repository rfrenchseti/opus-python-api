import pandas as pd

from opusapi import *

O = OPUSAPI(verbose=True)

sq1 = StringQuery('volumeid', 'COISS_2001', 'matches')
print(sq1)

sq2 = StringQuery('datasetid', 'EDR')
print(sq2)

q = Query(sq1, sq2)
print(q)

c1 = O.get_count(sq1)
print(c1)
c2 = O.get_count(sq2)
print(c2)

c = O.get_count(q)
print(c)

mq1 = MultQuery('planet', ['jupiter', 'earth'])
print(mq1)

c = O.get_count(mq1)
print(c)

rq1 = RangeQuery('observationduration', minimum=10)
print(rq1)

c = O.get_count(rq1)
print(c)

rq2 = RangeQuery('observationduration', maximum=10)
print(rq2)

c = O.get_count(rq2)
print(c)

rq3 = RangeQuery('RINGGEOringradius', minimum=1.1, maximum=1.4, qtype='all', unit='saturnradii')
print(rq3)

c = O.get_count(rq3)
print(c)
