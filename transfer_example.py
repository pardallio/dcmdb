#!/usr/bin/env python3

#
# Example on how to transfer data to lumi
#
# Ulf Andrae, SMHI, 2023
#

from cases import Cases,hub
import os
import sys

scratch = os.environ['SCRATCH']
lumi_basepath='/project/project_462000140/de_33050_common_data/cases/'
#file_template = 'fc(.*)fp'
selection = { 'finland_2017' : ['cy43_deode_ref_fin'] }
remote = { 'host' : 'lumi_transfer' ,'outpath' : None }

# Load the data 
example = Cases(selection=selection)
for case in example.names:
   for exp in example.cases.names:
       x=example.cases.runs.data
       file_template=example.cases.runs.file_templates[0]
       for date in x[file_template].keys():
           outpath_template = os.path.join(lumi_basepath,case,exp,'%Y/%m/%d/%H/')
           files = example.reconstruct(dtg=date)
           remote['outpath'] = hub(outpath_template,date)
           example.transfer(files,scratch,remote)
           sys.exit()


