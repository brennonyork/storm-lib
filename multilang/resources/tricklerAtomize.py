import storm
import sys
import os
import subprocess as sp

def strmap(func, ls):
    for i in range(0,len(ls)): ls[i] = func(ls[i])

def extend(ls, num):
    for i in range(0, num): ls.append('')

def toTuple(f):
    dataList = {"dns":10,
                "client":12,
                "service":11,
                "fingerprint":9}
    launch_tuple = ['tripdump', f]

    for line in sp.Popen(launch_tuple, stdout=sp.PIPE, bufsize=1).stdout:
        line = line.split('|')
        strmap(lambda x:x.strip(), line)
        line[0] = line[0][:-1].lower() # Remove trailing colon

        if(len(line) > 1):
            fieldVals = dataList.get(line[0], None)
            if(type(fieldVals) is int):
                if(fieldVals > len(line)):
                    extend(line, (fieldVals - len(line)))
                    yield line
                elif(fieldVals == len(line)):
                    yield line

class tricklerBolt(storm.Bolt):
    def process(self, tuple):
        for ls in toTuple(tuple.values[0]):
            storm.emit(ls, stream=str(ls[0]))

        os.remove(tuple.values[0])

tricklerBolt().run()
