#!/usr/bin/env python3

from cases import Cases

all = Cases()
all.print(0)
files = all.reconstruct(leadtime=[0,1,4,5,6])


#[print(x.names) for x in all_cases.cases.values()]
#for k,v in all_cases.cases.items():
   #print(v.runs.data)
#print(initial_files)
