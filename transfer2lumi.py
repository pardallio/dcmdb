#!/usr/bin/env python3

#
# Example on how to transfer data from atos to lumi.
# Data will be temporary located under hpc-login:$SCRATCH/case/run/... 
# 
# So far the transfer implementation is by date-hour only
#
# Ulf Andrae, SMHI, 2023
#

from cases import Cases, hub
import os


# Specify the case and the run
case = 'gavle_2021' 
run = 'deode_cy46ref'

# Specify the remote host, i.e. lumi
# remote = "lumi_transfer"
remote = "my_lumi_user@lumi.csc.fi"

# Load the data 
example = Cases(selection = { case : [run] })
for case in example.names:
   for exp in example.cases.names:
       x=example.cases.runs.data
       file_template=example.cases.runs.file_templates[0]
       for date in x[file_template].keys():

           print("Fetch:",date)
           # Parse the paths and expand the dates
           outpath_template = example.meta[case][run][remote]["path_template"]
           scratch_template = os.path.join(os.environ["SCRATCH"],case,exp,'%Y/%m/%d/%H/')
           remote_outpath = hub(outpath_template,date)
           scratch_outpath = hub(scratch_template,date)

           # Get a list of files
           files = example.reconstruct(dtg=date)

           # Do the actual copy from ecf to scratch and rsync to lumi, 
           # and clean the intermediate files
           example.transfer(files,scratch_outpath, { 'host' : remote ,'outpath' : remote_outpath })
